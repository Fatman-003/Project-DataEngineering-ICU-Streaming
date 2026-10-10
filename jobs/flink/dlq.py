"""Lossless, versioned rejection envelopes and bounded audit logs."""
import base64
import hashlib
import json
from datetime import datetime, timezone

ERROR_TYPES = frozenset({"PARSE_ERROR", "SCHEMA_ERROR", "VALUE_ERROR",
                         "DUPLICATE_EVENT", "LATE_EVENT"})


def payload_bytes(source):
    encoded = source["payload_base64"]
    return None if encoded is None else base64.b64decode(encoded, validate=True)


def text_or_none(raw):
    if raw is None:
        return None
    try:
        return raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None


def rejection(source, stream, error_type, reason, event=None, failed_at=None):
    if error_type not in ERROR_TYPES:
        raise ValueError(f"Unknown rejection type {error_type}")
    raw = payload_bytes(source)
    key = source.get("key_base64")
    key_bytes = None if key is None else base64.b64decode(key, validate=True)
    # Tombstones are distinct from empty byte values even though both hash empty bytes.
    digest = hashlib.sha256(raw if raw is not None else b"").hexdigest()
    identity = json.dumps([source["topic"], source["partition"], source["offset"],
                           error_type, digest, raw is None], separators=(",", ":"))
    result = {
        "schema_version": "1.0.0", "dlq_id": "dlq-" + hashlib.sha256(identity.encode()).hexdigest(),
        "error_type": error_type, "error_reason": reason,
        "failed_at": failed_at or datetime.now(timezone.utc).isoformat(),
        "original_topic": source["topic"], "original_partition": source["partition"],
        "original_offset": source["offset"], "original_key": text_or_none(key_bytes),
        "original_key_base64": key, "stream_type": stream,
        "raw_payload": text_or_none(raw), "raw_payload_base64": source["payload_base64"],
        "raw_payload_is_null": raw is None, "raw_sha256": digest,
    }
    for field in ("event_id", "stay_id", "run_id", "dataset_type", "replay_session_id",
                  "event_time", "signal_name", "event_type"):
        value = (event or {}).get(field)
        if isinstance(value, str) and value.strip():
            try:
                value.encode("utf-8", errors="strict")
            except UnicodeEncodeError:
                value = None
        else:
            value = None
        result[field] = value
    return result


def encode(value):
    return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"))


def log_line(record, kind, **details):
    """Never log original payloads or full waveform arrays."""
    source, event = record["source"], record["event"]
    result = {field: event.get(field) for field in (
        "stay_id", "event_id", "signal_name", "event_type", "event_time", "source_event_time",
        "producer_time", "chart_time", "available_time", "availability_status",
        "validation_flags", "dataset_type", "replay_session_id", "value",
        "waveform_record_id", "sampling_rate_hz")}
    result.update(kind=kind, stream_type=record["stream_type"], topic=source["topic"],
                  partition=source["partition"], offset=source["offset"])
    samples = event.get("samples")
    if isinstance(samples, list):
        result.update(sample_count=len(samples), samples_preview=samples[:5])
    result.update(details)
    return encode(result)


def rejection_log(envelope):
    fields = ("stream_type", "event_id", "stay_id", "error_type", "error_reason",
              "original_topic", "original_partition", "original_offset", "raw_sha256",
              "dlq_id", "replay_session_id", "event_time", "signal_name", "event_type")
    return encode(dict(kind="DLQ_EVENT", **{field: envelope.get(field) for field in fields}))
