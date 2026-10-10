"""Strict JSON on the wire, retaining nulls and all canonical fields."""
import json
import math
from datetime import date, datetime, timezone


def json_value(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_value(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def encode(payload):
    return json.dumps(json_value(payload), ensure_ascii=False,
                      allow_nan=False, separators=(",", ":")).encode("utf-8")


def message_key(payload):
    stay = payload.get("stay_id")
    if stay is None or not str(stay).strip():
        raise ValueError("Missing stay_id: refusing to send an unkeyed event")
    return str(stay).encode("utf-8")


def stamp(payload, session_id, now=None):
    result = dict(payload)
    result["producer_time"] = now or datetime.now(timezone.utc)
    result["replay_session_id"] = session_id
    return result
