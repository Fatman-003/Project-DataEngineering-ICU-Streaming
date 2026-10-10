"""Idempotent RAW bootstrap, with an optional TICKET-006 DLQ on the same broker."""
import argparse
import json
import sys
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ConfigEntry, AlterConfigOpType, ResourceType
from config import PROJECT, TOPICS, settings

sys.path.insert(0, str(PROJECT / "jobs/flink"))
from runtime_config import DLQ_TOPIC, DLQ_MAX_MESSAGE_BYTES


def ensure_topics(bootstrap, include_dlq=False):
    topics = list(TOPICS.values()) + ([DLQ_TOPIC] if include_dlq else [])
    admin = AdminClient({"bootstrap.servers": bootstrap, "socket.timeout.ms": 10000})
    futures = admin.create_topics(
        [NewTopic(name, num_partitions=1, replication_factor=1) for name in topics],
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
    for name in topics:
        topic = metadata.topics[name]
        if topic.error:
            raise KafkaException(topic.error)
        if len(topic.partitions) != 1 or any(
                len(partition.replicas) != 1 for partition in topic.partitions.values()):
            raise ValueError(f"{name}: expected one partition and replication factor one")
    if include_dlq:
        resource = ConfigResource(ResourceType.TOPIC, DLQ_TOPIC)
        resource.add_incremental_config(ConfigEntry(
            "max.message.bytes", str(DLQ_MAX_MESSAGE_BYTES), incremental_operation=AlterConfigOpType.SET))
        for future in admin.incremental_alter_configs([resource], request_timeout=15).values():
            future.result()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-dlq", action="store_true", help="Also create icu.v1.deadletter")
    args = parser.parse_args()
    try:
        print(json.dumps(ensure_topics(settings()["bootstrap"], include_dlq=args.include_dlq), indent=2))
        return 0
    except Exception as error:
        print(f"Topic bootstrap failed: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
