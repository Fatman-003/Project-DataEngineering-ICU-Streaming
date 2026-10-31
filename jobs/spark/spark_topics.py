"""Create the four icu.alt.spark.* output topics (1 partition, RF 1).

scripts/demo/kafka_topics.py only manages the six core topics, so its helpers
cannot be reused for these names; the topic settings below mirror it.
"""
import os
import sys
import time

from spark_config import TOPICS
from runtime_config import DLQ_MAX_MESSAGE_BYTES


def ensure_topics(bootstrap, timeout=60):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.monotonic() + timeout
    while True:
        try:
            existing = admin.list_topics(timeout=10).topics
            break
        except Exception:
            if time.monotonic() >= deadline:
                raise
            time.sleep(1)
    requested = [NewTopic(topic, num_partitions=1, replication_factor=1,
                          config={"max.message.bytes": str(DLQ_MAX_MESSAGE_BYTES)} if name == "deadletter" else {})
                 for name, topic in TOPICS.items() if topic not in existing]
    for future in admin.create_topics(requested).values() if requested else ():
        future.result(15)
    while True:
        current = admin.list_topics(timeout=10).topics
        missing = [topic for topic in TOPICS.values()
                   if topic not in current or current[topic].error or set(current[topic].partitions) != {0}]
        if not missing:
            return list(TOPICS.values())
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Spark output topics not ready: {missing}")
        time.sleep(0.25)


if __name__ == "__main__":
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    print("Spark output topics ready:\n" + "\n".join(ensure_topics(bootstrap)))
