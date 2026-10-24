"""Shared proposal DLQ envelope, including unchanged RAW sources."""
import base64
from business_contract import encode, deadletter

ERROR_TYPES = frozenset({"PARSE_ERROR", "SCHEMA_ERROR", "VALUE_ERROR", "DUPLICATE_EVENT", "LATE_EVENT",
                         "IDEMPOTENCY_CONFLICT"})


def payload_bytes(source):
    return None if source["payload_base64"] is None else base64.b64decode(source["payload_base64"], validate=True)


def rejection(source, stream, error_type, reason, event=None, failed_at=None):
    if error_type not in ERROR_TYPES:
        raise ValueError(error_type)
    code = {"PARSE_ERROR": "JSON_PARSE_ERROR", "SCHEMA_ERROR": "INVALID_FIELD_TYPE",
            "VALUE_ERROR": "VALUE_OUT_OF_RANGE", "DUPLICATE_EVENT": "PROCESSING_ERROR",
            "LATE_EVENT": "PROCESSING_ERROR", "IDEMPOTENCY_CONFLICT": "IDEMPOTENCY_CONFLICT"}[error_type]
    stage = "DESERIALIZATION" if error_type == "PARSE_ERROR" else (
        "PROCESSING" if error_type in ("DUPLICATE_EVENT", "LATE_EVENT", "IDEMPOTENCY_CONFLICT") else "VALIDATION")
    raw = payload_bytes(source)
    if error_type == "PARSE_ERROR" and raw is not None:
        try:
            raw.decode("utf-8", errors="strict")
        except UnicodeError:
            code = "INVALID_UTF8"
    if event is not None and event.get("schema_version") != "2.0":
        code = "SCHEMA_VERSION_UNSUPPORTED"
    return deadletter(source, code, error_type + ": " + reason, stage=stage)


def rejection_log(envelope):
    return encode({key: envelope.get(key) for key in ("dlq_id", "error_code", "error_message",
                                                     "source_topic", "source_partition", "source_offset")})
