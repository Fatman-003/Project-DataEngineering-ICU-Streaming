"""RAW schema, real registry identity, dictionary and numeric validity checks."""
import math
from datetime import datetime, timezone
from contracts import CHART_FIELDS, LAB_FIELDS, SIGNALS, LABS, RAW_SCHEMA_VERSION
from registry import load_patients
PATIENTS = load_patients()


class ValidationError(ValueError):
    def __init__(self, error_type, reason):
        super().__init__(reason)
        self.error_type, self.reason = error_type, reason


def fail(reason, kind="SCHEMA_ERROR"):
    raise ValidationError(kind, reason)


def parse_timestamp(value, field="event_time"):
    if not isinstance(value, str) or "T" not in value:
        fail(f"{field} must be an ISO timestamp with timezone")
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            fail(f"{field} needs an explicit timezone")
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError) as error:
        fail(f"Invalid {field}: {error}")


def validate_event(event, stream):
    expected = CHART_FIELDS if stream == "chartevents" else LAB_FIELDS if stream == "labevents" else None
    if expected is None or not isinstance(event, dict):
        fail("Unknown stream or non-object payload")
    if set(event) != expected:
        fail(f"Contract fields differ: missing={sorted(expected - set(event))}, extra={sorted(set(event) - expected)}")
    if event["schema_version"] != RAW_SCHEMA_VERSION:
        fail("schema_version must equal " + RAW_SCHEMA_VERSION)
    for field in ("event_id", "valueuom"):
        if not isinstance(event[field], str) or not event[field].strip():
            fail(f"{field} must be nonempty text")
        try:
            event[field].encode("utf-8", errors="strict")
        except UnicodeError:
            fail(f"{field} contains invalid Unicode")
    stay_field = "stay_id" if stream == "chartevents" else "mapped_stay_id"
    for field in ("subject_id", "hadm_id", stay_field, "bed_id", "itemid"):
        if type(event[field]) is not int or event[field] <= 0:
            fail(f"{field} must be a positive integer")
    if not any(all(event[field] == patient[field] for field in ("subject_id", "hadm_id", "bed_id"))
               and event[stay_field] == patient["stay_id"] for patient in PATIENTS):
        fail("Patient/admission/stay/bed must match one of P01-P04")
    parse_timestamp(event["event_time"])
    name_field = "signal_name" if stream == "chartevents" else "lab_name"
    dictionary = SIGNALS if stream == "chartevents" else LABS
    name = event[name_field]
    if not isinstance(name, str) or name not in dictionary:
        fail(f"Unsupported {name_field}")
    itemid, unit, lower, upper = dictionary[name]
    if event["itemid"] != itemid or event["valueuom"] != unit:
        fail(f"{name} itemid/unit do not match the dictionary")
    value = event["valuenum"]
    try:
        valid = type(value) in (int, float) and math.isfinite(value) and lower <= value <= upper
    except OverflowError:
        valid = False
    if not valid:
        fail(f"{name} valuenum must be finite in [{lower}, {upper}]", "VALUE_ERROR")
