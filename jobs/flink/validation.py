"""Pure TICKET-006 engineering checks; accepted payloads remain unchanged.

These bounds are data-validity checks, not clinical diagnostic thresholds.
Naive MIMIC timestamps use UTC only as a consistent epoch/comparison convention.
"""
import math
from datetime import datetime, timezone


NUMERIC_BOUNDS = {
    "HR": (0, None),
    "MAP": (0, None),
    "SBP": (0, None),
    "DBP": (0, None),
    "SPO2": (0, 100),
    "RR": (0, None),
}
CLINICAL_TYPES = frozenset({"LAB", "GCS", "URINE"})


class ValidationError(ValueError):
    def __init__(self, error_type, reason):
        super().__init__(reason)
        self.error_type = error_type
        self.reason = reason


def _required(event, field):
    if field not in event:
        raise ValidationError("SCHEMA_ERROR", f"Missing required field: {field}")
    return event[field]


def _text(event, field):
    value = _required(event, field)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("SCHEMA_ERROR", f"{field} must be a nonempty string")
    try:
        value.encode("utf-8", errors="strict")
    except UnicodeEncodeError as error:
        raise ValidationError("SCHEMA_ERROR", f"{field} contains an unpaired Unicode surrogate") from error
    return value


def parse_timestamp(value, field):
    """Parse an ISO timestamp for comparison without replacing the payload value."""
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("SCHEMA_ERROR", f"{field} must be an ISO timestamp string")
    # datetime.fromisoformat accepts date-only inputs; a runtime timestamp needs time.
    if "T" not in value and " " not in value:
        raise ValidationError("SCHEMA_ERROR", f"{field} must include a time")
    try:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError) as error:
        raise ValidationError("SCHEMA_ERROR", f"Invalid timestamp in {field}") from error


def _number(value, field):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("VALUE_ERROR", f"{field} must be numeric, excluding booleans")
    try:
        finite = math.isfinite(value)
    except (OverflowError, ValueError):
        finite = False
    if not finite:
        raise ValidationError("VALUE_ERROR", f"{field} must be finite")
    return value


def _validate_numeric(event):
    signal = _text(event, "signal_name")
    if signal not in NUMERIC_BOUNDS:
        raise ValidationError("SCHEMA_ERROR", f"Unknown canonical numeric signal: {signal}")
    value = _number(_required(event, "value"), "value")
    lower, upper = NUMERIC_BOUNDS[signal]
    if value < lower or (upper is not None and value > upper):
        interval = f"[{lower}, {upper}]" if upper is not None else f">= {lower}"
        raise ValidationError("VALUE_ERROR", f"{signal} value must be {interval}")


def _validate_clinical(event):
    event_type = _text(event, "event_type")
    if event_type not in CLINICAL_TYPES:
        raise ValidationError("SCHEMA_ERROR", f"Unknown clinical event_type: {event_type}")
    chart = parse_timestamp(_required(event, "chart_time"), "chart_time")
    available = _required(event, "available_time")
    if available is not None:
        available = parse_timestamp(available, "available_time")
        if available < chart:
            raise ValidationError("VALUE_ERROR", "available_time is earlier than chart_time")
    value = _required(event, "value")
    if value is not None:
        _number(value, "value")
    value_text = _required(event, "value_text")
    if value_text is not None and not isinstance(value_text, str):
        raise ValidationError("SCHEMA_ERROR", "value_text must be a string or null")


def _validate_waveform(event):
    _text(event, "waveform_record_id")
    _text(event, "signal_name")
    frequency = _number(_required(event, "sampling_rate_hz"), "sampling_rate_hz")
    if frequency <= 0:
        raise ValidationError("VALUE_ERROR", "sampling_rate_hz must be > 0")
    samples = _required(event, "samples")
    if not isinstance(samples, list) or not samples:
        raise ValidationError("SCHEMA_ERROR", "samples must be a nonempty array")
    for index, sample in enumerate(samples):
        if sample is not None:
            _number(sample, f"samples[{index}]")
    start = parse_timestamp(_required(event, "start_time"), "start_time")
    end = parse_timestamp(_required(event, "end_time"), "end_time")
    if end <= start:
        raise ValidationError("VALUE_ERROR", "end_time must be later than start_time")


def validate_event(event, stream):
    """Raise a typed rejection or return None; never mutate the original event."""
    if not isinstance(event, dict):
        raise ValidationError("SCHEMA_ERROR", "Event must be an object")
    if stream not in {"vitals", "clinical", "waveform"}:
        raise ValidationError("SCHEMA_ERROR", f"Unknown stream: {stream}")
    _text(event, "stay_id")
    if stream in {"vitals", "clinical"}:
        _text(event, "event_id")
    elif event.get("event_id") is not None:
        _text(event, "event_id")
    for field in ("subject_id", "hadm_id", "run_id", "patient_key", "replay_session_id"):
        if field in event:
            _text(event, field)
    if event.get("virtual_patient_id") is not None:
        _text(event, "virtual_patient_id")
    for field in ("event_time", "producer_time"):
        parse_timestamp(_required(event, field), field)
    {"vitals": _validate_numeric, "clinical": _validate_clinical,
     "waveform": _validate_waveform}[stream](event)
