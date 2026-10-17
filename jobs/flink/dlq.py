"""Human-readable V2 rejections, with exact byte preservation for non-UTF8 inputs."""
import base64
import hashlib
import json
ERROR_TYPES = frozenset({"PARSE_ERROR", "SCHEMA_ERROR", "VALUE_ERROR", "DUPLICATE_EVENT", "LATE_EVENT"})


def encode(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"))


def payload_bytes(source):
    return None if source["payload_base64"] is None else base64.b64decode(source["payload_base64"], validate=True)


def rejection(source, stream, error_type, reason, event=None, failed_at=None):
    if error_type not in ERROR_TYPES:
        raise ValueError(error_type)
    raw = payload_bytes(source)
    identity = [source["topic"], source["partition"], source["offset"], error_type]
    result = {
        "schema_version": "2.0",
        "dlq_id": "dlq-" + hashlib.sha256(encode(identity).encode()).hexdigest(),
        "error_type": error_type, "error_reason": reason,
        "original_topic": source["topic"], "original_partition": source["partition"],
        "original_offset": source["offset"],
        "raw_payload": None if raw is None else raw.decode("utf-8", errors="replace"),
    }
    if raw is not None:
        try:
            raw.decode("utf-8", errors="strict")
        except UnicodeError:
            result["raw_payload_base64"] = source["payload_base64"]
    event = event or {}
    for field in ("event_id", "subject_id", "hadm_id", "bed_id", "event_time"):
        value = event.get(field)
        result[field] = value if type(value) in (str, int) else None
    stay = event.get("stay_id", event.get("mapped_stay_id"))
    result["stay_id"] = stay if type(stay) is int else None
    return result


def rejection_log(envelope):
    return encode({key: envelope.get(key) for key in (
        "dlq_id", "event_id", "stay_id", "error_type", "error_reason",
        "original_topic", "original_partition", "original_offset")})