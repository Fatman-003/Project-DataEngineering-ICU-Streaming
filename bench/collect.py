"""Read RAW and engine output topics from `earliest` into parquet files of one run directory.

    python bench/collect.py --run bench/results/<name> [--engines flink,spark] [--docker "sudo docker"]

Writes raw.parquet, outputs.parquet (engine, kind, topic, offset, append_ts, key,
payload, headers) and metadata.json. With --docker, also copies the Spark
progress JSONL files from the icu-spark container into spark_metrics/.
"""
import argparse
import json
import shlex
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

RAW_TOPICS = {"chartevents": "icu.v2.chartevents", "labevents": "icu.v2.labevents"}
ENGINE_TOPICS = {
    "flink": {"telemetry": "icu.telemetry", "aggregates": "icu.vitals.5m", "alerts": "icu.alerts",
              "deadletter": "icu.deadletter"},
    "spark": {"telemetry": "icu.alt.spark.telemetry", "aggregates": "icu.alt.spark.vitals.5m",
              "alerts": "icu.alt.spark.alerts", "deadletter": "icu.alt.spark.deadletter"},
}
TIMESTAMP_TYPES = {0: "NOT_AVAILABLE", 1: "CREATE_TIME", 2: "LOG_APPEND_TIME"}


def read_topic(topic, bootstrap):
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "icu-bench-collect",
                         "enable.auto.commit": False, "enable.partition.eof": False})
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        rows = []
        for partition in sorted(partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            if low == high:
                continue
            consumer.assign([TopicPartition(topic, partition, low)])
            deadline = time.monotonic() + 120
            last = low - 1
            while last < high - 1:
                if time.monotonic() > deadline:
                    raise RuntimeError(f"Timed out reading {topic}[{partition}] at offset {last}/{high}")
                for message in consumer.consume(num_messages=10000, timeout=1):
                    if message.error():
                        raise RuntimeError(message.error())
                    kind, stamp = message.timestamp()
                    rows.append({"topic": topic, "partition": partition, "offset": message.offset(),
                                 "append_ts": stamp, "timestamp_type": TIMESTAMP_TYPES.get(kind, str(kind)),
                                 "key": message.key(), "payload": message.value(),
                                 "headers": json.dumps({k: None if v is None else v.decode("utf-8", "replace")
                                                        for k, v in (message.headers() or [])})})
                    last = message.offset()
        return rows
    finally:
        consumer.close()


def collect(run_dir, bootstrap="127.0.0.1:9092", engines=("flink", "spark"), docker=None):
    import pandas as pd
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)
    raw = [row for topic in RAW_TOPICS.values() for row in read_topic(topic, bootstrap)]
    outputs = [dict(row, engine=engine, kind=kind) for engine in engines
               for kind, topic in ENGINE_TOPICS[engine].items() for row in read_topic(topic, bootstrap)]
    pd.DataFrame(raw).to_parquet(run_dir / "raw.parquet", index=False)
    pd.DataFrame(outputs).to_parquet(run_dir / "outputs.parquet", index=False)
    metadata = {"collected_at": datetime.now(timezone.utc).isoformat(), "bootstrap": bootstrap,
                "engines": list(engines), "raw_records": len(raw), "output_records": len(outputs),
                "timestamp_types": sorted({row["timestamp_type"] for row in raw + outputs})}
    if docker and "spark" in engines:
        target = run_dir / "spark_metrics"
        target.mkdir(exist_ok=True)
        subprocess.run([*shlex.split(docker), "cp", "icu-spark:/opt/icu/metrics/.", str(target)], check=True)
        metadata["spark_metrics"] = sorted(p.name for p in target.iterdir())
    (run_dir / "metadata.json").write_text(json.dumps(metadata, indent=2))
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", required=True)
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    parser.add_argument("--engines", default="flink,spark")
    parser.add_argument("--docker", help='Docker command for copying Spark metrics, e.g. "sudo docker"')
    args = parser.parse_args()
    print(json.dumps(collect(args.run, args.bootstrap, tuple(args.engines.split(",")), args.docker), indent=2))


if __name__ == "__main__":
    main()
