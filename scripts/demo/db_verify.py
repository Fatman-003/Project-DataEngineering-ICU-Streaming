"""Bounded live Kafka-to-Timescale checks for each FastVerify run."""
import json
import math
import time
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "services/storage_writer"))
sys.path.insert(0, str(PROJECT / "jobs/flink"))

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient
from psycopg import sql

from config import GROUP_ID, TOPIC_TABLE, CORE_TABLES, TIME_COLUMNS, IDENTITY_COLUMNS
from db import parse_time, persist_one
from db_admin import connect, reference_counts, runtime_counts, source_counts, verify_structure
from fast_verify import read_pipeline
from registry import load_patients

from db import TABLE_COLUMNS
from business_contract import metadata, headers, encode, stable_uuid
COMPARE = TABLE_COLUMNS


def storage_lag(admin=None, offset_reader=None):
    admin = admin or AdminClient({"bootstrap.servers": "localhost:9092"})
    group = admin.describe_consumer_groups([GROUP_ID])[GROUP_ID].result(10)
    if len(group.members) != 1:
        raise AssertionError(f"Storage consumer group has {len(group.members)} active members, expected one")
    assigned = {(p.topic, p.partition) for p in group.members[0].assignment.topic_partitions}
    expected = {(topic, 0) for topic in TOPIC_TABLE}
    if assigned != expected:
        raise AssertionError(f"Storage assignment mismatch: {sorted(assigned)}")
    committed = admin.list_consumer_group_offsets([ConsumerGroupTopicPartitions(GROUP_ID)])[GROUP_ID].result(10)
    positions = {(p.topic, p.partition): p.offset for p in committed.topic_partitions}
    if not set(positions) <= expected:
        raise AssertionError(f"Storage committed offset set mismatch: {sorted(positions)}")
    if offset_reader is None:
        consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "icu-storage-lag-reader",
                             "enable.auto.commit": False})
        try:
            highs = {pair: consumer.get_watermark_offsets(TopicPartition(*pair), timeout=5)[1]
                     for pair in expected}
        finally:
            consumer.close()
    else:
        highs = offset_reader(expected)
    # A subscribed topic with no records has no committed position yet.
    # auto.offset.reset=earliest makes its effective starting position zero.
    lag = {f"{topic}[{partition}]": high - max(0, positions.get((topic, partition), 0))
           for (topic, partition), high in highs.items()}
    if any(value < 0 for value in lag.values()):
        raise AssertionError(f"Storage offsets beyond topic ends: {lag}")
    return lag


def wait_storage_lag(timeout=90, monotonic=time.monotonic, sleep=time.sleep):
    deadline = monotonic() + timeout
    while True:
        lag = storage_lag()
        if all(value == 0 for value in lag.values()):
            return lag
        if monotonic() >= deadline:
            raise AssertionError(f"Storage consumer lag did not reach zero: {lag}")
        sleep(0.2)


def matching_row(connection, table, event):
    time_column, identity = TIME_COLUMNS[table], IDENTITY_COLUMNS[table]
    columns = COMPARE[table]
    query = sql.SQL("SELECT {} FROM {} WHERE {} = %s AND {} = %s").format(
        sql.SQL(", ").join(map(sql.Identifier, columns)), sql.Identifier(table),
        sql.Identifier(time_column), sql.Identifier(identity))
    rows = connection.execute(query, (parse_time(event[time_column]), event[identity])).fetchall()
    if len(rows) != 1:
        raise AssertionError(f"Expected one DB row for {table}.{identity}={event[identity]}, got {len(rows)}")
    for column, stored in zip(columns, rows[0]):
        expected = event[column]
        if column in ("sample_time", "window_end", "alert_time"):
            expected = parse_time(expected)
        if isinstance(expected, (int, float)) and type(expected) is not bool:
            if not math.isclose(stored, expected, rel_tol=1e-10, abs_tol=1e-10):
                raise AssertionError(f"Kafka/DB {table}.{column} mismatch: {stored} != {expected}")
        elif str(stored) != str(expected):
            raise AssertionError(f"Kafka/DB {table}.{column} mismatch: {stored} != {expected}")
    return {"table": table, "id": event[identity], "matched_columns": list(columns)}


def verify_database():
    lag = wait_storage_lag()
    records = read_pipeline()
    with connect() as connection:
        structure = verify_structure(connection, allow_legacy=True)
        if structure["extension_version"] != "2.30.2":
            raise AssertionError(f"Timescale version mismatch: {structure['extension_version']}")
        version = connection.execute("SHOW server_version").fetchone()[0]
        if not version.startswith("17."):
            raise AssertionError(f"Expected PostgreSQL 17, got {version}")
        refs = reference_counts(connection)
        if {key: refs[key] for key in ("patients", "admissions", "icustays")} != {
                "patients": 4, "admissions": 4, "icustays": 4}:
            raise AssertionError(f"Reference identity count mismatch: {refs}")
        if refs["d_items"] != 6 or refs["d_labitems"] != 1:
            raise AssertionError(f"Active dictionary seed mismatch: {refs}")
        patients = {row[0] for row in connection.execute("SELECT subject_id FROM patients")}
        if patients != {p["subject_id"] for p in load_patients()}:
            raise AssertionError("Database patients differ from frozen P01-P04 registry")
        expected_items = {220045, 220052, 220050, 220051, 220277, 220210}
        if {row[0] for row in connection.execute("SELECT itemid FROM d_items")} != expected_items:
            raise AssertionError("Vital dictionary IDs differ from current simulator")
        if {row[0] for row in connection.execute("SELECT itemid FROM d_labitems")} != {50813}:
            raise AssertionError("Lab dictionary IDs differ from current simulator")
        counts = runtime_counts(connection)
        expected_counts = {table: len({(e[TIME_COLUMNS[table]], e[IDENTITY_COLUMNS[table]]) for e in records[topic]
                                       if e["event_type"] != "SEPSIS_ALERT_STATUS_CHANGED"})
                           for topic, table in TOPIC_TABLE.items()}
        expected_counts["waveform_segments"] = 0
        if counts != expected_counts or any(counts[t] <= 0 for t in TOPIC_TABLE.values()):
            raise AssertionError(f"Business counts differ: DB={counts}, Kafka={expected_counts}")
        sources = source_counts(connection)
        if any(sources.values()):
            raise AssertionError("Source placeholders received streaming writes")
        alert_counts = {p["demo_id"]: connection.execute(
            "SELECT count(*) FROM sepsis_alert_events WHERE stay_id=%s", (p["stay_id"],)).fetchone()[0]
            for p in load_patients()}
        if alert_counts != {"P01":0,"P02":1,"P03":0,"P04":0}:
            raise AssertionError(f"Unexpected alerts: {alert_counts}")
        matches = []
        for topic, table in TOPIC_TABLE.items():
            event = records[topic][0]
            matches.append(matching_row(connection, table, event))
            # Replay the actual persisted Kafka business payload. The physical
            # time + ID key must make the second durable insert a no-op.
            kafka_key = str(event.get("stay_id", event.get("mapped_stay_id"))).encode()
            persist_one(connection, topic, event, kafka_key)
            matching_row(connection, table, event)
        if runtime_counts(connection) != counts:
            raise AssertionError("Duplicate business retry changed DB row counts")
    if any(storage_lag().values()):
        raise AssertionError("Storage lag rose during idempotency verification")
    return {"result": "PASS", "postgres_version": version, "timescale_version": structure["extension_version"],
            "core_tables": len(structure["tables"]), "hypertables": len(structure["hypertables"]),
            "reference_counts": refs, "runtime_counts": counts, "source_counts": sources, "alert_counts": alert_counts,
            "consumer_group": GROUP_ID, "consumer_lag": lag,
            "kafka_database_matches": matches, "duplicate_retries": len(matches)}


def verify_status_change():
    from confluent_kafka import Producer
    from contracts import BUSINESS_TOPICS, DLQ_TOPIC
    records = read_pipeline()
    created = next(e for e in records[BUSINESS_TOPICS["alerts"]] if e["event_type"] == "SEPSIS_ALERT_CREATED")
    update = dict(metadata("SEPSIS_ALERT_STATUS_CHANGED", [created["alert_id"], "ACKNOWLEDGED"], "icu-api"),
                  **{k: created[k] for k in ("alert_time", "alert_id", "stay_id")}, status="ACKNOWLEDGED")
    producer = Producer({"bootstrap.servers":"localhost:9092", "acks":"all", "enable.idempotence":True})
    errors = []
    def delivered(error, message):
        if error: errors.append(str(error))
    def publish(event):
        producer.produce(BUSINESS_TOPICS["alerts"], key=str(event["stay_id"]).encode(), value=encode(event).encode(),
                         headers=headers(event), on_delivery=delivered)
        if producer.flush(20) or errors: raise AssertionError("Status publish failed")
    with connect() as connection:
        before = runtime_counts(connection)
    publish(update); publish(update)
    wait_storage_lag()
    with connect() as connection:
        row = connection.execute("SELECT status FROM sepsis_alert_events WHERE alert_time=%s AND alert_id=%s AND stay_id=%s",
                                 (parse_time(update["alert_time"]), update["alert_id"], update["stay_id"])).fetchone()
        assert row == ("ACKNOWLEDGED",), row
        assert runtime_counts(connection) == before
    invalid = dict(update, **metadata("SEPSIS_ALERT_STATUS_CHANGED", "missing-alert", "icu-api"),
                   alert_id=stable_uuid("no-such-alert"))
    publish(invalid)
    wait_storage_lag()
    deadline = time.monotonic() + 15
    while True:
        rejects = read_pipeline()[DLQ_TOPIC]
        if any(e["error_code"] == "ALERT_NOT_FOUND" and e["consumer_group"] == GROUP_ID for e in rejects):
            break
        if time.monotonic() >= deadline: raise AssertionError("Missing ALERT_NOT_FOUND DLQ")
        time.sleep(0.1)
    with connect() as connection:
        assert runtime_counts(connection) == before
    return {"result":"PASS", "status":"ACKNOWLEDGED", "duplicate_update":"idempotent", "missing_alert":"ALERT_NOT_FOUND / no insert"}
