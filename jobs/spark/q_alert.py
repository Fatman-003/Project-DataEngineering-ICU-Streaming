"""Q_alert: RAW chartevents -> 1 s MAP medians + context -> HYPOTENSION_SUSTAINED -> icu.alt.spark.alerts.

Parses RAW independently of Q_wide (plan §3.1); rejections are dropped here
because Q_wide already writes them to the Spark DLQ.
"""
import json

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.streaming.state import GroupStateTimeout

from alert_logic import AlertLogic
from business_contract import encode
from parse_step import parsed_chartevents, to_records
from records import OUTPUT_SCHEMA, kafka_record
from spark_config import TOPICS, start
from wide_logic import MemoryMap, MemoryValue, expire_seen

STATE_SCHEMA = "seen string, pending string, context string, episode string"
DEDUP_TTL_MS = 24 * 3600 * 1000


def evaluate(key, frames, state):
    watermark = state.getCurrentWatermarkMs()
    rows = [] if state.hasTimedOut else [row for frame in frames for row in to_records(frame)]
    if state.exists:
        seen, pending, context, episode = state.get
        logic = AlertLogic(MemoryMap(json.loads(seen)),
                           MemoryMap({int(k): v for k, v in json.loads(pending).items()}),
                           MemoryMap(json.loads(context)), MemoryValue(episode))
    else:
        logic = AlertLogic(MemoryMap(), MemoryMap(), MemoryMap(), MemoryValue())
    alerts = logic.process(rows, watermark)
    expire_seen(logic.seen, watermark, DEDUP_TTL_MS)
    state.update((encode(logic.seen.items), encode(logic.pending.items), encode(logic.context.items),
                  logic.episode.get()))
    timer = logic.next_timer()
    if timer is not None:
        state.setTimeoutTimestamp(timer)
    yield pd.DataFrame([kafka_record(TOPICS["alerts"], alert) for alert in alerts],
                       columns=["topic", "key", "value", "headers"])


def build(spark, config):
    alerts = (parsed_chartevents(spark, config).where(F.col("status") == "VALID")
              .groupBy("stay_key").applyInPandasWithState(
                  evaluate, OUTPUT_SCHEMA, STATE_SCHEMA, "append", GroupStateTimeout.EventTimeTimeout))
    writer = (alerts.writeStream.format("kafka").option("kafka.bootstrap.servers", config.bootstrap_servers)
              .option("kafka.acks", "all").option("kafka.linger.ms", "0"))
    return start(writer, config, "alert")
