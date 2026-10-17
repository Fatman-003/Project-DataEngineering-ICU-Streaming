"""Bootstrap seven topics on the existing broker; V1 deletion is gated by live smoke."""
import sys
import time
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jobs/flink"))
from contracts import CORE_TOPICS, DLQ_TOPIC

V1_TOPICS = (
    "icu.v1.vitals.numeric", "icu.v1.clinical.events", "icu.v1.vitals.waveform", "icu.v1.deadletter",
    "icu.v1.primary.vitals.processed", "icu.v1.primary.clinical.processed",
    "icu.v1.primary.vitals.aggregates", "icu.v1.primary.alerts",
)


DEMO_GROUPS = ("icu-ui-gateway-v2", "icu-flink-v2", "icu-timescale-writer-v2")


def ensure_topics(bootstrap="localhost:9092", *, admin=None, monotonic=time.monotonic,
                  sleep=time.sleep, timeout=30):
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = admin or AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    requested = [NewTopic(topic, num_partitions=1, replication_factor=1,
                          config={"max.message.bytes": str(8 * 1024 * 1024)} if topic == DLQ_TOPIC else {})
                 for topic in CORE_TOPICS if topic not in existing]
    for future in admin.create_topics(requested).values() if requested else ():
        future.result(15)
    deadline = monotonic() + timeout
    while True:
        current = admin.list_topics(timeout=10).topics
        extras = {name for name in current if name.startswith("icu.v2.")} - set(CORE_TOPICS)
        if extras:
            raise RuntimeError(f"Unexpected V2 topics: {sorted(extras)}")
        not_ready = [topic for topic in CORE_TOPICS if topic not in current
                     or current[topic].error or set(current[topic].partitions) != {0}
                     or len(current[topic].partitions[0].replicas) != 1]
        if not not_ready:
            break
        if monotonic() >= deadline:
            raise RuntimeError(f"Missing or incorrectly configured topics after metadata refresh: {not_ready}")
        sleep(0.25)
    return list(CORE_TOPICS)


def topic_offsets(topics, bootstrap="localhost:9092"):
    """Read-only partition bounds; does not join or commit any consumer group."""
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "icu-demo-reset-reader",
                         "enable.auto.commit": False})
    try:
        metadata = consumer.list_topics(timeout=10).topics
        return {topic: {str(partition): list(consumer.get_watermark_offsets(
                    TopicPartition(topic, partition), timeout=10))
                       for partition in metadata[topic].partitions}
                for topic in topics}
    finally:
        consumer.close()


def reset_demo_topics(bootstrap="localhost:9092", *, admin=None, bounds_reader=None,
                      monotonic=time.monotonic, sleep=time.sleep, timeout=90):
    """Only call after the V2 job, gateway and owned producer have stopped.

    Recreate the seven allowlisted topics and delete only their three inactive
    consumer groups. Neither broker storage nor unrelated topics are mutated.
    """
    from confluent_kafka import KafkaException, KafkaError
    from confluent_kafka.admin import AdminClient
    admin = admin or AdminClient({"bootstrap.servers": bootstrap})
    bounds_reader = bounds_reader or (lambda topics: topic_offsets(topics, bootstrap))
    before = admin.list_topics(timeout=10).topics
    extras = {name for name in before if name.startswith("icu.v2.")} - set(CORE_TOPICS)
    if extras:
        raise RuntimeError(f"Unexpected V2 topics; refusing a partial reset: {sorted(extras)}")
    unrelated = {name: tuple(sorted((p, tuple(v.replicas)) for p, v in data.partitions.items()))
                 for name, data in before.items() if name not in CORE_TOPICS}
    unrelated_bounds = bounds_reader([name for name in unrelated if not name.startswith("__")])
    targets = [name for name in CORE_TOPICS if name in before]
    if targets:
        for future in admin.delete_topics(targets, operation_timeout=15).values():
            future.result(20)
    deadline = monotonic() + timeout
    while set(admin.list_topics(timeout=10).topics) & set(CORE_TOPICS):
        if monotonic() >= deadline:
            raise RuntimeError("Timed out waiting for the seven V2 topics to disappear")
        sleep(0.25)
    # Deleting an inactive group removes any offsets still referencing deleted topic IDs.
    for group in DEMO_GROUPS:
        while True:
            try:
                admin.delete_consumer_groups([group])[group].result(10)
                break
            except KafkaException as error:
                code = error.args[0].code()
                if code == KafkaError.GROUP_ID_NOT_FOUND:
                    break
                if code != KafkaError.NON_EMPTY_GROUP or monotonic() >= deadline:
                    raise
                sleep(0.5)
    ensure_topics(bootstrap, admin=admin, monotonic=monotonic, sleep=sleep)
    # Newly created leaders can take a short time to become queryable.
    while True:
        try:
            offsets = bounds_reader(CORE_TOPICS)
            if any(partitions != {"0": [0, 0]} for partitions in offsets.values()) or len(offsets) != 7:
                raise RuntimeError("V2 reset did not leave seven empty topics; another producer may still be active")
            break
        except KafkaException:
            if monotonic() >= deadline:
                raise
            sleep(0.25)
    after = admin.list_topics(timeout=10).topics
    for name, fingerprint in unrelated.items():
        current = after.get(name)
        if current is None or fingerprint != tuple(sorted((p, tuple(v.replicas)) for p, v in current.partitions.items())):
            raise RuntimeError(f"Unrelated topic metadata changed: {name}")
    after_bounds = bounds_reader(list(unrelated_bounds))
    # A concurrent unrelated producer may advance its own log; never truncate it.
    for name, partitions in unrelated_bounds.items():
        for partition, (low, high) in partitions.items():
            new_low, new_high = after_bounds[name][partition]
            if new_low < low or new_high < high:
                raise RuntimeError(f"Unrelated topic log moved backwards: {name}")
    return {"topics": offsets, "groups_reset": list(DEMO_GROUPS),
            "unrelated_before": unrelated_bounds, "unrelated_after": after_bounds}


def remove_verified_v1(bootstrap="localhost:9092"):
    """Called only after all live smoke assertions pass, never during startup."""
    from confluent_kafka.admin import AdminClient
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    targets = [topic for topic in V1_TOPICS if topic in existing]
    if targets:
        for future in admin.delete_topics(targets, operation_timeout=15).values():
            future.result(20)
    return targets


if __name__ == "__main__":
    print("V2 topics ready:\n" + "\n".join(ensure_topics()))
