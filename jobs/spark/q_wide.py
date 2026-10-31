"""Q_wide: RAW chartevents -> icu.alt.spark.telemetry + icu.alt.spark.deadletter.

One Kafka sink routes each row by its `topic` column (DECISIONS D6), so the
stateful operator runs once per batch and output stays at-least-once.
"""
import json

import pandas as pd
from pyspark.sql.streaming.state import GroupStateTimeout

from business_contract import encode
from parse_step import INVALID_KEY, parsed_chartevents, to_records
from records import OUTPUT_SCHEMA, kafka_record
from spark_config import TOPICS, start
from wide_logic import MemoryMap, MemoryValue, WideLogic

STATE_SCHEMA = "seen string, pending string, maximum long"
DEDUP_TTL_MS = 24 * 3600 * 1000
OUTPUT_TOPICS = {"TELEMETRY": TOPICS["telemetry"], "DLQ": TOPICS["deadletter"]}


def output_frame(outputs):
    rows = [kafka_record(OUTPUT_TOPICS[kind], payload) for kind, payload in outputs]
    return pd.DataFrame(rows, columns=["topic", "key", "value", "headers"])


def assemble(key, frames, state):
    watermark = state.getCurrentWatermarkMs()
    rows = [] if state.hasTimedOut else [row for frame in frames for row in to_records(frame)]
    if key[0] == INVALID_KEY:
        # Rejections carry no per-stay state; Flink emits them before any keyed operator.
        yield output_frame([("DLQ", row["dlq"]) for row in rows])
        return
    if state.exists:
        seen, pending, maximum = state.get
        logic = WideLogic(MemoryMap(json.loads(seen)), MemoryMap({int(k): v for k, v in json.loads(pending).items()}),
                          MemoryValue(maximum))
    else:
        logic = WideLogic(MemoryMap(), MemoryMap(), MemoryValue())
    outputs = logic.process(rows, watermark)
    logic.expire_seen(watermark, DEDUP_TTL_MS)
    state.update((encode(logic.seen.items), encode(logic.pending.items), logic.maximum.get()))
    timer = logic.next_timer()
    if timer is not None:
        # Fires once the Spark watermark exceeds T, i.e. Flink's `wm >= T` (DECISIONS D8).
        state.setTimeoutTimestamp(timer)
    yield output_frame(outputs)


def build(spark, config):
    wide = (parsed_chartevents(spark, config).groupBy("stay_key").applyInPandasWithState(
        assemble, OUTPUT_SCHEMA, STATE_SCHEMA, "append", GroupStateTimeout.EventTimeTimeout))
    writer = (wide.writeStream.format("kafka").option("kafka.bootstrap.servers", config.bootstrap_servers)
              .option("kafka.acks", "all").option("kafka.linger.ms", "0")
              .option("kafka.max.request.size", str(8 * 1024 * 1024)))
    return start(writer, config, "wide")
