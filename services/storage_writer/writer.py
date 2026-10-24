"""Commit DB before Kafka offsets; durable DLQ for invalid/exhausted records."""
import logging
import os
import signal
import time
from pathlib import Path
import psycopg
from confluent_kafka import Consumer, Producer
from config import GROUP_ID, TOPIC_TABLE, db_params
from db import mapped_record, persist_one
from business_contract import (ContractError, validate, deadletter, source_record, headers, encode, DLQ_TOPIC)
from contracts import parse_event

LOG = logging.getLogger("icu.storage_writer")
READY = Path("/tmp/icu-writer-ready")
MAX_DB_ATTEMPTS = 8


def store_and_commit(consumer, connection, message, event):
    result = persist_one(connection, message.topic(), event, message.key())
    consumer.commit(message=message, asynchronous=False)
    return result


def reject_and_commit(consumer, producer, message, code, stage, retryable=False):
    envelope = deadletter(source_record(message), code, code + ": business record could not be processed",
                          stage, GROUP_ID, retryable)
    if code == "DB_WRITE_ERROR":
        envelope["attempt"] = MAX_DB_ATTEMPTS
    failures = []
    def delivered(error, record):
        if error:
            failures.append(str(error))
    producer.produce(DLQ_TOPIC, key=message.key() or envelope["dlq_id"].encode(),
                     value=encode(envelope).encode(), headers=headers(envelope), on_delivery=delivered)
    if producer.flush(20) or failures:
        raise RuntimeError("DLQ delivery failed; original offset not committed")
    consumer.commit(message=message, asynchronous=False)


def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    def shutdown(signum, frame):
        raise SystemExit(0)
    signal.signal(signal.SIGTERM, shutdown)
    READY.unlink(missing_ok=True)
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "icu-kafka:9094")
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": GROUP_ID, "enable.auto.commit": False,
                         "auto.offset.reset": "earliest", "fetch.wait.max.ms": 20})
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all"})
    connection = None
    def assigned(client, partitions):
        if {partition.topic for partition in partitions} != set(TOPIC_TABLE):
            raise ValueError("Unexpected Storage Writer assignment")
        READY.write_text(str(os.getpid()), encoding="ascii")
    try:
        consumer.subscribe(list(TOPIC_TABLE), on_assign=assigned)
        while True:
            message = consumer.poll(0.2)
            if message is None or message.error():
                continue
            try:
                event = parse_event(message.value().decode("utf-8"))
                validate(event, message.topic(), message.key(), message.headers(), require_headers=True)
                mapped_record(message.topic(), event, message.key())
            except UnicodeError:
                reject_and_commit(consumer, producer, message, "INVALID_UTF8", "DESERIALIZATION")
                continue
            except ContractError as error:
                reject_and_commit(consumer, producer, message, error.code,
                                  "STORAGE" if error.code == "REFERENCE_NOT_FOUND" else "VALIDATION",
                                  retryable=error.code == "REFERENCE_NOT_FOUND")
                continue
            except (ValueError, TypeError, AttributeError, RecursionError):
                reject_and_commit(consumer, producer, message, "JSON_PARSE_ERROR", "DESERIALIZATION")
                continue
            for attempt in range(MAX_DB_ATTEMPTS):
                try:
                    if connection is None or connection.closed:
                        connection = psycopg.connect(**db_params(), autocommit=True)
                    store_and_commit(consumer, connection, message, event)
                    break
                except ContractError as error:
                    reject_and_commit(consumer, producer, message, error.code, "PROCESSING")
                    break
                except psycopg.IntegrityError:
                    reject_and_commit(consumer, producer, message, "DB_CONSTRAINT_ERROR", "STORAGE")
                    break
                except (psycopg.Error, OSError):
                    if connection is not None:
                        connection.close()
                    connection = None
                    if attempt + 1 == MAX_DB_ATTEMPTS:
                        reject_and_commit(consumer, producer, message, "DB_WRITE_ERROR", "STORAGE", retryable=True)
                        break
                    LOG.warning("Temporary DB error; retry %s/%s; offset not committed", attempt + 1, MAX_DB_ATTEMPTS)
                    time.sleep(min(0.25 * 2**attempt, 5))
    finally:
        READY.unlink(missing_ok=True)
        if connection is not None:
            connection.close()
        consumer.close()


if __name__ == "__main__":
    run()
