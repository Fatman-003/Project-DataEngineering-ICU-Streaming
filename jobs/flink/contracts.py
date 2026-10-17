"""Seven V2 topics and strict JSON parsing."""
import json

TOPICS = {"chartevents": "icu.v2.chartevents", "labevents": "icu.v2.labevents"}
BUSINESS_TOPICS = {
    "chartevents": "icu.v2.chartevents.cleaned",
    "labevents": "icu.v2.labevents.cleaned",
    "aggregates": "icu.v2.vitals.15m",
    "alerts": "icu.v2.alerts",
}
DLQ_TOPIC = "icu.v2.deadletter"
CORE_TOPICS = (*TOPICS.values(), *BUSINESS_TOPICS.values(), DLQ_TOPIC)
CHART_FIELDS = frozenset((
    "schema_version", "event_id", "subject_id", "hadm_id", "stay_id", "bed_id",
    "itemid", "signal_name", "event_time", "valuenum", "valueuom"))
LAB_FIELDS = (CHART_FIELDS - {"stay_id", "signal_name"}) | {"mapped_stay_id", "lab_name"}
SIGNALS = {
    "HR": (220045, "bpm", 0, 350), "MAP": (220052, "mmHg", 0, 300),
    "SBP": (220050, "mmHg", 0, 350), "DBP": (220051, "mmHg", 0, 250),
    "SpO2": (220277, "%", 0, 100), "RR": (220210, "insp/min", 0, 100),
}
LABS = {"Lactate": (50813, "mmol/L", 0, 50)}


def selected_topics(stream="all"):
    return dict(TOPICS) if stream == "all" else {stream: TOPICS[stream]}


def _invalid_constant(value):
    raise ValueError(f"Non-JSON constant: {value}")


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"Duplicate JSON field: {key}")
        result[key] = value
    return result


def parse_event(raw):
    event = json.loads(raw, parse_constant=_invalid_constant, object_pairs_hook=_unique_pairs)
    if not isinstance(event, dict):
        raise ValueError("Expected a JSON object")
    return event


def stay_key(event):
    return str(event.get("stay_id", event.get("mapped_stay_id")))