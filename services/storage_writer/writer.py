"""At-least-once Kafka writer: durable DB commit precedes each offset commit."""
import json
import logging
import os
import signal
import time
from pathlib import Path

import psycopg
from confluent_kafka import Consumer
from config import GROUP_ID, TOPIC_TABLE, db_params
from db import mapped_record, persist_one

LOG = logging.getLogger("icu.storage_writer")
READY = Path("/tmp/icu-writer-ready")


def stop_unhealthy(message, reason):
    READY.unlink(missing_ok=True)
    LOG.critical("Poison business record %s[%s]@%s: %s; offset not committed; writer unhealthy",
                 message.topic(), message.partition(), message.offset(), reason)
    # Keep the container visibly unhealthy until an operator fixes the contract.
    # Restarting automatically would just retry the same poison message forever.
    while True:
        time.sleep(3600)


def store_and_commit(consumer, connection, message, event):
    result = persist_one(connection, message.topic(), event, message.key())
    consumer.commit(message=message, asynchronous=False)
    return result


def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    def shutdown(signum, frame):
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, shutdown)
    READY.unlink(missing_ok=True)
    consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "icu-kafka:9094"),
                         "group.id": GROUP_ID, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest", "fetch.wait.max.ms": 20})
    connection = None

    def assigned(client, partitions):
        assigned_topics = {partition.topic for partition in partitions}
        if assigned_topics != set(TOPIC_TABLE):
            raise ValueError(f"Unexpected Storage Writer assignment: {assigned_topics}")
        READY.write_text(str(os.getpid()), encoding="ascii")
        LOG.info("Assigned project business partitions: %s", sorted(assigned_topics))

    try:
        consumer.subscribe(list(TOPIC_TABLE), on_assign=assigned)
        while True:
            message = consumer.poll(0.2)
            if message is None:
                continue
            if message.error():
                LOG.warning("Kafka consumer: %s", message.error())
                continue
            try:
                event = json.loads(message.value().decode("utf-8"))
                table, fields = mapped_record(message.topic(), event, message.key())
                identity = fields["aggregate_id" if table == "vitals_15m" else
                                  "alert_id" if table == "alerts" else "event_id"]
            except (ValueError, TypeError, KeyError, UnicodeError, AttributeError) as error:
                stop_unhealthy(message, error)
            backoff = 0.25
            while True:
                try:
                    if connection is None or connection.closed:
                        connection = psycopg.connect(**db_params(), autocommit=True)
                    store_and_commit(consumer, connection, message, event)
                    break
                except (psycopg.errors.IntegrityError, ValueError) as error:
                    stop_unhealthy(message, error)
                except (psycopg.Error, OSError) as error:
                    LOG.warning("Retry DB %s[%s]@%s %s: %s; no offset commit",
                                message.topic(), message.partition(), message.offset(), identity, error)
                    if connection is not None:
                        connection.close()
                    connection = None
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 5)
            # No per-row log in steady state; lag and DB rows are the evidence.
    finally:
        READY.unlink(missing_ok=True)
        if connection is not None:
            connection.close()
        consumer.close()


if __name__ == "__main__":
    run()
