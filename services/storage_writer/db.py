"""Strict fixed-topic mapping and one durable transaction per business message."""
import json
import math
from pathlib import Path
from datetime import datetime

from psycopg import sql
from config import TOPIC_TABLE, TIME_COLUMNS, IDENTITY_COLUMNS

REGISTRY = {str(p["stay_id"]): p for p in json.loads(
    (Path(__file__).resolve().parents[2] / "config/demo_patients.yaml").read_text(encoding="utf-8"))["patients"]}

TABLE_COLUMNS = {
    "chartevents": ("event_time", "event_id", "schema_version", "subject_id", "hadm_id", "stay_id",
                    "bed_id", "itemid", "signal_name", "value", "valuenum", "valueuom",
                    "quality_status", "ordering_status", "processed_at"),
    "labevents": ("event_time", "event_id", "schema_version", "subject_id", "hadm_id",
                  "mapped_stay_id", "bed_id", "itemid", "lab_name", "value", "valuenum",
                  "valueuom", "quality_status", "ordering_status", "processed_at"),
    "vitals_15m": ("window_end", "aggregate_id", "schema_version", "window_start", "window_minutes",
                   "subject_id", "hadm_id", "stay_id", "bed_id", "signal_name", "count", "mean",
                   "min", "max", "variance", "emitted_at"),
    "alerts": ("trigger_time", "alert_id", "schema_version", "subject_id", "hadm_id", "stay_id",
               "bed_id", "alert_type", "severity", "signal_name", "trigger_value", "threshold_value",
               "episode_start", "status", "alert_emitted_at"),
}
TIMESTAMPS = frozenset(("event_time", "processed_at", "window_start", "window_end", "emitted_at",
                        "trigger_time", "episode_start", "alert_emitted_at"))
REQUIRED_NON_NULL = {
    "chartevents": frozenset(TABLE_COLUMNS["chartevents"]) - {"value"},
    "labevents": frozenset(TABLE_COLUMNS["labevents"]) - {"value", "valuenum", "valueuom", "ordering_status"},
    "vitals_15m": frozenset(TABLE_COLUMNS["vitals_15m"]) - {"variance", "emitted_at"},
    "alerts": frozenset(TABLE_COLUMNS["alerts"]) - {"alert_emitted_at"},
}
QUERIES = {}


def parse_time(value):
    if not isinstance(value, str):
        raise ValueError("Business timestamp must be a string")
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if result.tzinfo is None:
        raise ValueError("Business event timestamps must carry a timezone")
    return result


def mapped_record(topic, event, kafka_key):
    if topic not in TOPIC_TABLE or not isinstance(event, dict) or event.get("schema_version") != "2.0":
        raise ValueError("Invalid business topic or schema")
    table = TOPIC_TABLE[topic]
    stay_field = "mapped_stay_id" if table == "labevents" else "stay_id"
    stay = event.get(stay_field)
    patient = REGISTRY.get(str(stay))
    if (patient is None or type(stay) is not int or kafka_key != str(stay).encode()
            or any(event.get(field) != patient[field] for field in ("subject_id", "hadm_id", "bed_id"))):
        raise ValueError("Business Kafka key and fixed patient identity must agree")
    if table in ("chartevents", "labevents") and event.get("quality_status") != "VALID":
        raise ValueError("Only clean validated events can be persisted")
    if table == "vitals_15m" and event.get("window_minutes") != 15:
        raise ValueError("Only Flink 15-minute aggregates can be persisted")
    if table == "alerts" and (event.get("alert_type") != "SUSTAINED_HYPOTENSION"
                              or event.get("signal_name") != "MAP"):
        raise ValueError("Unsupported clinical alert")
    if any(event.get(column) is None for column in REQUIRED_NON_NULL[table]):
        raise ValueError(f"Missing required {table} field")
    for column in ("subject_id", "hadm_id", stay_field):
        if type(event[column]) is not int:
            raise ValueError(f"{column} must be an integer")
    if table in ("chartevents", "labevents"):
        if type(event["itemid"]) is not int or type(event["valuenum"]) not in (int, float):
            raise ValueError("Clean measurement has invalid numeric type")
        if event.get("ordering_status") not in ("IN_ORDER", "OUT_OF_ORDER"):
            raise ValueError("Invalid clean ordering status")
    if table == "vitals_15m" and (type(event["count"]) is not int or event["count"] <= 0):
        raise ValueError("Invalid aggregate count")
    if table == "alerts" and (type(event["trigger_value"]) not in (int, float)
                              or type(event["threshold_value"]) not in (int, float)):
        raise ValueError("Invalid alert trigger value")
    fields = {}
    for column in TABLE_COLUMNS[table]:
        value = event.get(column)
        if column in TIMESTAMPS and value is not None:
            value = parse_time(value)
        elif isinstance(value, float) and not math.isfinite(value):
            raise ValueError(f"Nonfinite value in {column}")
        fields[column] = value
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
        connection.execute(insert_sql(table), list(fields.values()))
    # Transaction context committed before return; caller may now commit offset.
    return table, fields[IDENTITY_COLUMNS[table]]
