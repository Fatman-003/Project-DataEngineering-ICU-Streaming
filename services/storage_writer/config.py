"""Only the four accepted business streams enter TimescaleDB."""
import os

GROUP_ID = "icu-timescale-writer-v2"
TOPIC_TABLE = {
    "icu.v2.chartevents.cleaned": "chartevents",
    "icu.v2.labevents.cleaned": "labevents",
    "icu.v2.vitals.15m": "vitals_15m",
    "icu.v2.alerts": "alerts",
}
REFERENCE_TABLES = ("patients", "admissions", "icustays", "d_items", "d_labitems")
RUNTIME_TABLES = tuple(TOPIC_TABLE.values())
CORE_TABLES = (*REFERENCE_TABLES, *RUNTIME_TABLES)
TIME_COLUMNS = {"chartevents": "event_time", "labevents": "event_time",
                "vitals_15m": "window_end", "alerts": "trigger_time"}
IDENTITY_COLUMNS = {"chartevents": "event_id", "labevents": "event_id",
                    "vitals_15m": "aggregate_id", "alerts": "alert_id"}


def db_params(host=None, values=None):
    values = os.environ if values is None else values
    return {"host": host or values.get("TIMESCALE_HOST", "icu-timescaledb"),
            "port": int(values.get("TIMESCALE_PORT", "5432")),
            "dbname": values.get("POSTGRES_DB", "icu"),
            "user": values.get("POSTGRES_USER", "icu_app"),
            "password": values["POSTGRES_PASSWORD"],
            "connect_timeout": 3}
