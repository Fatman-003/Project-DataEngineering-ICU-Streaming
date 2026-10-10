"""RAW JSON inspection only: no schema/value validation or business rules."""
import hashlib
import json

TOPICS = {
    "vitals": "icu.v1.vitals.numeric",
    "clinical": "icu.v1.clinical.events",
    "waveform": "icu.v1.vitals.waveform",
}


def selected_topics(stream):
    if stream == "all":
        return dict(TOPICS)
    if stream not in TOPICS:
        raise ValueError(f"Unknown stream: {stream}")
    return {stream: TOPICS[stream]}


def _invalid_constant(value):
    raise ValueError(f"Non-JSON constant: {value}")


def parse_event(raw):
    """Return the original object, leaving timestamps and nulls untouched."""
    event = json.loads(raw, parse_constant=_invalid_constant)
    if not isinstance(event, dict):
        raise ValueError("Expected a JSON object")
    return event


def inspection_line(raw, stream):
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    try:
        event = parse_event(raw)
    except (ValueError, TypeError) as error:
        return json.dumps({
            "kind": "PARSE_ERROR", "stream": stream, "topic": TOPICS[stream],
            "reason": str(error), "raw_sha256": digest, "raw_preview": raw[:240],
        }, ensure_ascii=False)
    fields = (
        "event_id", "run_id", "patient_key", "subject_id", "hadm_id", "stay_id",
        "signal_name", "event_type", "item_name", "value", "value_text", "unit",
        "source_event_time", "event_time", "producer_time", "chart_time",
        "available_time", "availability_status", "validation_flags",
        "replay_time_basis", "replay_session_id", "dataset_type", "is_virtual",
        "waveform_record_id", "start_time", "end_time", "sampling_rate_hz",
    )
    result = {field: event[field] for field in fields if field in event}
    result.update(kind="RAW_EVENT", stream=stream, topic=TOPICS[stream],
                  raw_sha256=digest, available_time_present="available_time" in event)
    samples = event.get("samples")
    if isinstance(samples, list):
        result.update(sample_count=len(samples), samples_preview=samples[:5],
                      missing_sample_count=event.get("missing_sample_count"))
    return json.dumps(result, ensure_ascii=False, allow_nan=False)
