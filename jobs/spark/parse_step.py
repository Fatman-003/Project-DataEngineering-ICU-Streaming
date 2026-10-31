"""mapInPandas step shared by Q_wide and Q_alert: Kafka row -> validated record or DLQ envelope.

Rejected rows get a NULL event_ts and the key "__invalid__": a NULL never moves
the watermark and is never dropped as late (spike S7), matching Flink, where
rejections leave before timestamps are assigned. A Kafka-timestamp fallback
(as the plan suggested) would push the watermark to wall-clock time.
"""
import pandas as pd

from business_contract import encode
from event_time import event_timestamp_ms
from records import source_record
from runtime_pipeline import parse_and_validate

INVALID_KEY = "__invalid__"
PARSED_SCHEMA = "stay_key string, event_ts timestamp, offset long, status string, payload string"
KAFKA_COLUMNS = ("topic", "partition", "offset", "key", "value", "headers")


def parse_rows(frame, stream="chartevents"):
    rows = []
    for item in frame.itertuples(index=False):
        source = source_record(item.topic, item.partition, item.offset, item.key, item.value, item.headers)
        record, failed = parse_and_validate(source, stream)
        if failed is not None:
            rows.append((INVALID_KEY, None, item.offset, "REJECTED", encode(failed)))
        else:
            event = record["event"]
            rows.append((str(event["stay_id"]), pd.Timestamp(event_timestamp_ms(event), unit="ms"), item.offset,
                         "VALID", encode({"source": source, "event": event})))
    return pd.DataFrame(rows, columns=["stay_key", "event_ts", "offset", "status", "payload"])


def parse_chartevents(batches):
    for frame in batches:
        yield parse_rows(frame)


def parsed_chartevents(spark, config):
    from contracts import TOPICS
    from spark_config import WATERMARK_DELAY, kafka_source
    return (kafka_source(spark, config, TOPICS["chartevents"]).select(*KAFKA_COLUMNS)
            .mapInPandas(parse_chartevents, PARSED_SCHEMA)
            .withWatermark("event_ts", WATERMARK_DELAY))


def to_records(frame):
    """Parsed pandas rows -> the dict shape WideLogic/AlertLogic expect."""
    import json
    rows = []
    for item in frame.itertuples(index=False):
        payload = json.loads(item.payload)
        if item.status == "REJECTED":
            rows.append({"status": "REJECTED", "offset": int(item.offset), "dlq": payload})
        else:
            rows.append({"status": "VALID", "offset": int(item.offset), **payload})
    return rows
