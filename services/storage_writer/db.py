"""Natural-key idempotency, alert identity checks, commit before Kafka offsets."""
import json
from pathlib import Path
import sys
from psycopg import sql
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jobs/flink"))
from business_contract import validate, ContractError, timestamp
from config import TOPIC_TABLE, TIME_COLUMNS, IDENTITY_COLUMNS

REGISTRY = {str(p["stay_id"]): p for p in json.loads(
    (Path(__file__).resolve().parents[2] / "config/demo_patients.yaml").read_text(encoding="utf-8"))["patients"]}
TABLE_COLUMNS = {
    "patient_telemetry_stream": ("sample_time", "stay_id", "heart_rate", "map", "sbp", "spo2"),
    "vitals_sliding_window_5m": ("window_end", "stay_id", "avg_heart_rate", "avg_map", "avg_sbp", "avg_spo2"),
    "sepsis_alert_events": ("alert_time", "alert_id", "stay_id", "alert_type", "severity",
                            "trigger_value", "trigger_message", "status")}
TIMESTAMPS = frozenset(TIME_COLUMNS.values())
QUERIES = {}
parse_time = timestamp


def mapped_record(topic, event, kafka_key):
    validate(event, topic, kafka_key)
    if topic not in TOPIC_TABLE or str(event["stay_id"]) not in REGISTRY:
        raise ContractError("REFERENCE_NOT_FOUND", "stay_id not in fixed patient registry")
    table = TOPIC_TABLE[topic]
    columns = TABLE_COLUMNS[table]
    if event["event_type"] == "SEPSIS_ALERT_STATUS_CHANGED":
        columns = ("alert_time", "alert_id", "stay_id", "status")
    fields = {column: timestamp(event[column]) if column in TIMESTAMPS else event.get(column) for column in columns}
    return table, fields


def insert_sql(table):
    if table not in TABLE_COLUMNS:
        raise ValueError(table)
    if table not in QUERIES:
        columns = TABLE_COLUMNS[table]
        QUERIES[table] = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}, {}) DO NOTHING").format(
            sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, columns)),
            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
            sql.Identifier(TIME_COLUMNS[table]), sql.Identifier(IDENTITY_COLUMNS[table]))
    return QUERIES[table]


def persist_one(connection, topic, event, kafka_key):
    table, fields = mapped_record(topic, event, kafka_key)
    with connection.transaction():
        if event["event_type"] == "SEPSIS_ALERT_STATUS_CHANGED":
            cursor = connection.execute(
                "UPDATE sepsis_alert_events SET status=%s WHERE alert_time=%s AND alert_id=%s AND stay_id=%s",
                (fields["status"], fields["alert_time"], fields["alert_id"], fields["stay_id"]))
            if cursor.rowcount != 1:
                raise ContractError("ALERT_NOT_FOUND", "Status change references no matching alert")
        else:
            connection.execute(insert_sql(table), list(fields.values()))
    return table, fields[IDENTITY_COLUMNS[table]]
