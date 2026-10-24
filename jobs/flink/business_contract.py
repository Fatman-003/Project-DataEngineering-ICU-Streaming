"""Versioned UI contracts shared by Flink, Gateway and Storage Writer."""
import base64
import json
import math
import re
import uuid
from datetime import datetime, timezone

BUSINESS_TOPICS = {"telemetry": "icu.telemetry", "aggregates": "icu.vitals.5m", "alerts": "icu.alerts"}
DLQ_TOPIC = "icu.deadletter"
FIELDS = {"HR": "heart_rate", "MAP": "map", "SBP": "sbp", "SpO2": "spo2"}
RANGES = {"heart_rate": (0, 350), "map": (0, 300), "sbp": (0, 350), "spo2": (0, 100)}
ALERT_TYPES = ("HYPOTENSION_SUSTAINED", "QSOFA_HIGH_RISK", "SENSOR_DISCONNECTED")
STATUSES = ("ACTIVE", "ACKNOWLEDGED", "FALSE_ALARM")
NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "https://icu.local/contracts/1")
EVENT_TYPES = {"icu.telemetry": ("TELEMETRY_SAMPLE",), "icu.vitals.5m": ("VITALS_WINDOW_5M",),
               "icu.alerts": ("SEPSIS_ALERT_CREATED", "SEPSIS_ALERT_STATUS_CHANGED")}


class ContractError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code


def fail(code, message):
    raise ContractError(code, message)


def now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def encode(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"))


def stable_uuid(*parts):
    return str(uuid.uuid5(NAMESPACE, encode(parts)))


def metadata(event_type, identity, producer="icu-flink", produced_at=None):
    return dict(schema_version="1.0", message_id=stable_uuid(event_type, identity),
                event_type=event_type, produced_at=produced_at or now(), producer=producer)


def timestamp(value):
    if not isinstance(value, str) or not re.fullmatch(
            r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d+)?(?:Z|\+00:00)", value):
        fail("INVALID_TIMESTAMP", "Timestamp must be RFC3339 UTC")
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        fail("INVALID_TIMESTAMP", "Invalid calendar timestamp")


def canonical_uuid(value, field):
    try:
        parsed = uuid.UUID(value)
        if str(parsed) != value or parsed.variant != uuid.RFC_4122 or parsed.version not in (4, 5, 7):
            raise ValueError()
    except (ValueError, TypeError, AttributeError):
        fail("INVALID_FIELD_TYPE", field + " must be a canonical supported UUID")


def finite_number(value):
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def finite_json_float(value):
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("JSON number cannot be represented as finite float")
    return result


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON field")
        result[key] = value
    return result


def headers(event):
    result = {"content-type": "application/json; charset=utf-8",
              "schema-version": event["schema_version"],
              "message-id": event.get("message_id", event.get("dlq_id")),
              "event-type": event["event_type"], "producer": event["producer"]}
    if event.get("trace_id") is not None:
        result["trace-id"] = event["trace_id"]
    if event["event_type"] == "DLQ_EVENT":
        result.update({"dlq-source-topic": event["source_topic"], "dlq-error-code": event["error_code"]})
    return [(key, value.encode()) for key, value in result.items()]


def header_dict(items):
    return {key: value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
            for key, value in (items or [])}


def validate(event, topic, key=None, kafka_headers=None, require_headers=False):
    if not isinstance(event, dict):
        fail("INVALID_FIELD_TYPE", "Expected JSON object")
    version = event.get("schema_version")
    if not isinstance(version, str) or not re.fullmatch(r"1\.\d+", version):
        fail("SCHEMA_VERSION_UNSUPPORTED", "Unsupported schema major version")
    for field in ("message_id", "event_type", "produced_at", "producer", "stay_id"):
        if field not in event:
            fail("MISSING_REQUIRED_FIELD", "Missing " + field)
    canonical_uuid(event["message_id"], "message_id")
    timestamp(event["produced_at"])
    if not isinstance(event["producer"], str) or not event["producer"].strip():
        fail("INVALID_FIELD_TYPE", "producer must be nonempty text")
    if "trace_id" in event and (not isinstance(event["trace_id"], str)
                               or not re.fullmatch(r"[0-9a-f]{32}", event["trace_id"])):
        fail("INVALID_FIELD_TYPE", "trace_id must be 32 lowercase hex characters")
    stay = event["stay_id"]
    if type(stay) is not int or not 0 < stay < 2**63:
        fail("INVALID_FIELD_TYPE", "stay_id must be a positive int64")
    if key is not None and key != str(stay).encode():
        fail("INVALID_FIELD_TYPE", "Kafka key must match stay_id")
    if topic not in EVENT_TYPES or event["event_type"] not in EVENT_TYPES[topic]:
        fail("EVENT_TYPE_MISMATCH", "event_type does not match topic")
    if require_headers:
        actual = header_dict(kafka_headers)
        if any(actual.get(name) != value.decode() for name, value in headers(event)):
            fail("MISSING_REQUIRED_FIELD", "Missing or inconsistent Kafka headers")
    kind = event["event_type"]
    clock = "sample_time" if kind == "TELEMETRY_SAMPLE" else "window_end" if kind == "VITALS_WINDOW_5M" else "alert_time"
    if clock not in event:
        fail("MISSING_REQUIRED_FIELD", "Missing " + clock)
    parsed = timestamp(event[clock])
    if kind in ("TELEMETRY_SAMPLE", "VITALS_WINDOW_5M"):
        fields = list(RANGES) if kind == "TELEMETRY_SAMPLE" else ["avg_" + f for f in RANGES]
        if not any(event.get(f) is not None for f in fields):
            fail("MISSING_REQUIRED_FIELD", "At least one observation is required")
        for field in fields:
            value = event.get(field)
            if value is not None:
                if not finite_number(value):
                    fail("INVALID_FIELD_TYPE", field + " must be a finite number or null")
                bounds = RANGES[field.removeprefix("avg_")]
                if not bounds[0] <= value <= bounds[1]:
                    fail("VALUE_OUT_OF_RANGE", field + " outside supported range")
        if kind == "VITALS_WINDOW_5M" and (parsed.second or parsed.microsecond):
            fail("INVALID_TIMESTAMP", "window_end must align to a minute")
    else:
        canonical_uuid(event.get("alert_id"), "alert_id")
        allowed = ("ACTIVE", "FALSE_ALARM") if kind == "SEPSIS_ALERT_CREATED" else ("ACKNOWLEDGED", "FALSE_ALARM")
        if event.get("status") not in allowed:
            fail("INVALID_ENUM_VALUE", "Unsupported alert status")
        if kind == "SEPSIS_ALERT_CREATED":
            if event.get("alert_type") not in ALERT_TYPES or event.get("severity") not in ("WARNING", "CRITICAL"):
                fail("INVALID_ENUM_VALUE", "Unsupported alert type or severity")
            if "trigger_value" not in event or "trigger_message" not in event:
                fail("MISSING_REQUIRED_FIELD", "Missing alert trigger fields")
            value = event["trigger_value"]
            if value is not None and not finite_number(value):
                fail("INVALID_FIELD_TYPE", "trigger_value must be finite or null")
            if not isinstance(event["trigger_message"], str) or not 1 <= len(event["trigger_message"]) <= 1024:
                fail("VALUE_OUT_OF_RANGE", "trigger_message length must be 1..1024")
            if event["producer"] != "icu-alert-engine":
                fail("INVALID_ENUM_VALUE", "Alert creation producer must be icu-alert-engine")
        elif event["producer"] not in ("icu-api", "icu-alert-engine"):
            fail("INVALID_ENUM_VALUE", "Unsupported status-change producer")
    return event


def source_record(message):
    return dict(topic=message.topic(), partition=message.partition(), offset=message.offset(),
                key_base64=None if message.key() is None else base64.b64encode(message.key()).decode(),
                payload_base64=None if message.value() is None else base64.b64encode(message.value()).decode(),
                headers=header_dict(message.headers()))


def deadletter(source, code, reason, stage="PROCESSING", group="icu-flink-v2", retryable=False):
    raw = None if source.get("payload_base64") is None else base64.b64decode(source["payload_base64"])
    original = None
    if raw is not None:
        try:
            candidate = json.loads(raw.decode("utf-8"), object_pairs_hook=unique_object, parse_float=finite_json_float,
                                   parse_constant=lambda value: (_ for _ in ()).throw(ValueError()))
            if isinstance(candidate, dict):
                original = candidate
        except (ValueError, UnicodeError, RecursionError):
            pass
    identity = stable_uuid(source["topic"], source["partition"], source["offset"], group, code)
    result = dict(metadata("DLQ_EVENT", identity, group), dlq_id=identity, failed_at=now(),
                  source_topic=source["topic"], source_partition=source["partition"],
                  source_offset=source["offset"], kafka_key=None, consumer_group=group,
                  error_stage=stage, error_code=code, error_message=reason[:1024],
                  retryable=retryable, attempt=1, original_headers=source.get("headers", {}),
                  original_payload=original,
                  raw_payload_base64=None if original is not None else source.get("payload_base64"))
    if source.get("key_base64") is not None:
        result["kafka_key"] = base64.b64decode(source["key_base64"]).decode("utf-8", errors="replace")
    return result
