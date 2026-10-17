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
from db_admin import connect, reference_counts, runtime_counts, verify_structure
from fast_verify import read_pipeline
from registry import load_patients

COMPARE = {
    "chartevents": ("event_id", "stay_id", "signal_name", "event_time", "valuenum"),
    "labevents": ("event_id", "mapped_stay_id", "lab_name", "event_time", "valuenum"),
    "vitals_15m": ("aggregate_id", "stay_id", "signal_name", "window_start", "window_end",
                   "mean", "min", "max"),
    "alerts": ("alert_id", "stay_id", "alert_type", "trigger_time", "threshold_value"),
}


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


def wait_storage_lag(timeout=20, monotonic=time.monotonic, sleep=time.sleep):
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
        if column in ("event_time", "window_start", "window_end", "trigger_time"):
            expected = parse_time(expected)
        if isinstance(expected, (int, float)) and type(expected) is not bool:
            if not math.isclose(stored, expected, rel_tol=1e-10, abs_tol=1e-10):
                raise AssertionError(f"Kafka/DB {table}.{column} mismatch: {stored} != {expected}")
        elif stored != expected:
            raise AssertionError(f"Kafka/DB {table}.{column} mismatch: {stored} != {expected}")
    return {"table": table, "id": event[identity], "matched_columns": list(columns)}


def verify_database():
    lag = wait_storage_lag()
    records = read_pipeline()
    with connect() as connection:
        structure = verify_structure(connection)
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
        expected_counts = {table: len(records[topic]) for topic, table in TOPIC_TABLE.items()}
        if counts != expected_counts or any(value <= 0 for value in counts.values()):
            raise AssertionError(f"Business row counts do not match current Kafka run: DB={counts}, Kafka={expected_counts}")
        p02 = next(p for p in load_patients() if p["demo_id"] == "P02")
        p03 = next(p for p in load_patients() if p["demo_id"] == "P03")
        alert_counts = {p["demo_id"]: connection.execute(
            "SELECT count(*) FROM alerts WHERE stay_id = %s", (p["stay_id"],)).fetchone()[0]
            for p in (p02, p03)}
        if alert_counts != {"P02": 1, "P03": 0}:
            raise AssertionError(f"Database alert count mismatch: {alert_counts}")
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
            "reference_counts": refs, "runtime_counts": counts, "alert_counts": alert_counts,
            "consumer_group": GROUP_ID, "consumer_lag": lag,
            "kafka_database_matches": matches, "duplicate_retries": len(matches)}
