"""Idempotent bootstrap of the three RAW topics only."""
import argparse
import json
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic
from config import TOPICS, settings


def ensure_topics(bootstrap):
    admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 10000})
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1) for name in TOPICS.values()],
        operation_timeout=10, request_timeout=15)
    result = {}
    for name, future in futures.items():
        try:
            future.result()
            result[name] = "CREATED"
        except KafkaException as error:
            if error.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
            result[name] = "EXISTS"
    metadata = admin.list_topics(timeout=10)
    for name in TOPICS.values():
        topic = metadata.topics[name]
        if topic.error:
            raise KafkaException(topic.error)
        if len(topic.partitions) != 1 or any(
                len(partition.replicas) != 1 for partition in topic.partitions.values()):
            raise ValueError(f"{name}: expected one partition and replication factor one")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    try:
        print(json.dumps(ensure_topics(settings()["bootstrap"]), indent=2))
        return 0
    except Exception as error:
        print(f"Topic bootstrap failed: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
