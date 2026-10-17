"""Flink 15-minute sliding windows with incremental sample variance."""
import hashlib
import json
from datetime import datetime, timedelta, timezone
from dlq import encode
from registry import IDENTITY_FIELDS

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def iso_timestamp(timestamp_ms):
    return (EPOCH + timedelta(milliseconds=timestamp_ms)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def window_key(record):
    event = record["event"]
    return str(event["stay_id"]), event["signal_name"]


def create_accumulator():
    return {"count": 0, "mean": 0.0, "m2": 0.0, "min": None, "max": None, "identity": {}}


def add_record(accumulator, record):
    event = record["event"]
    value = event["valuenum"]
    count = accumulator["count"] + 1
    delta = value - accumulator["mean"]
    mean = accumulator["mean"] + delta / count
    return {
        "count": count, "mean": mean, "m2": accumulator["m2"] + delta * (value - mean),
        "min": value if count == 1 else min(accumulator["min"], value),
        "max": value if count == 1 else max(accumulator["max"], value),
        "identity": {field: event[field] for field in IDENTITY_FIELDS},
    }


def merge_accumulators(left, right):
    if not left["count"]:
        return dict(right)
    if not right["count"]:
        return dict(left)
    if left["identity"] != right["identity"]:
        raise ValueError("Cannot mix patients in a vital window")
    count = left["count"] + right["count"]
    delta = right["mean"] - left["mean"]
    return {
        "count": count, "mean": left["mean"] + delta * right["count"] / count,
        "m2": left["m2"] + right["m2"] + delta * delta * left["count"] * right["count"] / count,
        "min": min(left["min"], right["min"]), "max": max(left["max"], right["max"]),
        "identity": dict(left["identity"]),
    }


def aggregate_result(accumulator, key, window_start_ms, window_end_ms, emitted_at_ms=None):
    if not accumulator["count"]:
        return None
    if window_end_ms - window_start_ms != 900000:
        raise ValueError("Only 15-minute statistical windows are supported")
    count = accumulator["count"]
    identity = [int(key[0]), key[1], window_start_ms, window_end_ms]
    return {
        "schema_version": "2.0",
        "aggregate_id": "agg-" + hashlib.sha256(encode(identity).encode()).hexdigest(),
        **accumulator["identity"], "signal_name": key[1],
        "window_start": iso_timestamp(window_start_ms), "window_end": iso_timestamp(window_end_ms),
        "window_minutes": 15, "count": count, "mean": accumulator["mean"],
        "min": accumulator["min"], "max": accumulator["max"],
        "variance": accumulator["m2"] / (count - 1) if count > 1 else None,
        "emitted_at": iso_timestamp(emitted_at_ms) if emitted_at_ms is not None else datetime.now(timezone.utc).isoformat(),
    }


def attach_windows(clean, config):
    from pyflink.common import Time, Types
    from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
    from pyflink.datastream.window import SlidingEventTimeWindows
    config.validate()

    class WelfordAggregate(AggregateFunction):
        def create_accumulator(self):
            return create_accumulator()

        def add(self, value, accumulator):
            return add_record(accumulator, json.loads(value))

        def get_result(self, accumulator):
            return accumulator

        def merge(self, left, right):
            return merge_accumulators(left, right)

    class WindowResult(ProcessWindowFunction):
        def process(self, key, context, elements):
            window = context.window()
            yield encode(aggregate_result(next(iter(elements)), key, window.start, window.end,
                                          context.current_processing_time()))

    return clean.key_by(lambda value: window_key(json.loads(value)),
                        key_type=Types.TUPLE([Types.STRING(), Types.STRING()])).window(
        SlidingEventTimeWindows.of(Time.seconds(900), Time.seconds(60))).aggregate(
        WelfordAggregate(), WindowResult(), accumulator_type=Types.PICKLED_BYTE_ARRAY(),
        output_type=Types.STRING()).name("15-minute vital statistics").uid("v2-window-15m")