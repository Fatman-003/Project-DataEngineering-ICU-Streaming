"""Three streaming destinations; source placeholders and waveform have no writer."""
import os
GROUP_ID = "icu-timescale-writer-v2"
TOPIC_TABLE = {"icu.telemetry": "patient_telemetry_stream",
               "icu.vitals.5m": "vitals_sliding_window_5m",
               "icu.alerts": "sepsis_alert_events"}
REFERENCE_TABLES = ("patients", "admissions", "icustays", "d_items", "d_labitems")
SOURCE_TABLES = ("chartevents", "labevents")
RUNTIME_TABLES = (*TOPIC_TABLE.values(), "waveform_segments")
CORE_TABLES = (*REFERENCE_TABLES, *SOURCE_TABLES, *RUNTIME_TABLES)
TIME_COLUMNS = {"patient_telemetry_stream": "sample_time", "vitals_sliding_window_5m": "window_end",
                "sepsis_alert_events": "alert_time", "waveform_segments": "segment_start"}
IDENTITY_COLUMNS = {"patient_telemetry_stream": "stay_id", "vitals_sliding_window_5m": "stay_id",
                    "sepsis_alert_events": "alert_id", "waveform_segments": "segment_id"}


def db_params(host=None, values=None):
    values = os.environ if values is None else values
    return {"host": host or values.get("TIMESCALE_HOST", "icu-timescaledb"),
            "port": int(values.get("TIMESCALE_PORT", "5432")),
            "dbname": values.get("POSTGRES_DB", "icu"),
            "user": values.get("POSTGRES_USER", "icu_app"),
            "password": values["POSTGRES_PASSWORD"], "connect_timeout": 3}
