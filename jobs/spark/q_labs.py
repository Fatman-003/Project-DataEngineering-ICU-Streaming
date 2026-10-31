"""Q_labs: validate RAW labevents and count outcomes (Flink only prints valid labs)."""
import json
import os

import pandas as pd

from spark_config import kafka_source, start
from contracts import TOPICS
from records import source_record
from runtime_pipeline import parse_and_validate

SCHEMA = "status string, error_type string"


def validate_labs(batches):
    for frame in batches:
        rows = []
        for item in frame.itertuples(index=False):
            source = source_record(item.topic, item.partition, item.offset, item.key, item.value, item.headers)
            _, failed = parse_and_validate(source, "labevents")
            rows.append(("VALID", None) if failed is None else
                        ("REJECTED", failed["error_message"].split(":", 1)[0]))
        yield pd.DataFrame(rows, columns=["status", "error_type"])


def build(spark, config):
    counts_path = os.path.join(config.metrics_root, "labs_counts.jsonl")

    def record_counts(frame, batch_id):
        counts = {f"{row['status']}:{row['error_type'] or ''}".rstrip(":"): row["count"]
                  for row in frame.groupBy("status", "error_type").count().collect()}
        if counts:
            with open(counts_path, "a", encoding="utf-8") as out:
                out.write(json.dumps({"batchId": batch_id, "counts": counts}) + "\n")

    validated = (kafka_source(spark, config, TOPICS["labevents"])
                 .select("topic", "partition", "offset", "key", "value", "headers")
                 .mapInPandas(validate_labs, SCHEMA))
    return start(validated.writeStream.foreachBatch(record_counts), config, "labs")
