"""One wide per-stay mean for each five-minute event-time window, sliding 1m."""
import json
from datetime import datetime, timedelta, timezone
from business_contract import FIELDS, metadata, encode

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def iso_timestamp(timestamp_ms):
    return (EPOCH + timedelta(milliseconds=timestamp_ms)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def create_accumulator():
    return {field: [0.0, 0] for field in FIELDS.values()}


def add_record(accumulator, event):
    for field in accumulator:
        value = event.get(field)
        if value is not None:
            accumulator[field][0] += value
            accumulator[field][1] += 1
    return accumulator


def merge_accumulators(left, right):
    return {field: [left[field][0] + right[field][0], left[field][1] + right[field][1]] for field in left}


def aggregate_result(accumulator, stay, window_start_ms, window_end_ms, emitted_at_ms=None):
    if window_end_ms - window_start_ms != 300000:
        raise ValueError("Only five-minute windows are supported")
    if not any(count for total, count in accumulator.values()):
        return None
    return dict(metadata("VITALS_WINDOW_5M", [int(stay), window_end_ms],
                         produced_at=iso_timestamp(emitted_at_ms) if emitted_at_ms is not None else None),
                window_end=iso_timestamp(window_end_ms), stay_id=int(stay),
                **{"avg_" + field: total / count if count else None
                   for field, (total, count) in accumulator.items()})


def attach_windows(wide, config):
    from pyflink.common import Time, Types
    from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
    from pyflink.datastream.window import SlidingEventTimeWindows
    config.validate()

    class MeanAggregate(AggregateFunction):
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
            result = aggregate_result(next(iter(elements)), key, window.start, window.end,
                                      context.current_processing_time())
            if result is not None:
                yield encode(result)

    return wide.key_by(lambda value: str(json.loads(value)["stay_id"]), key_type=Types.STRING()).window(
        SlidingEventTimeWindows.of(Time.seconds(config.window_5m_seconds),
                                   Time.seconds(config.window_slide_seconds))).aggregate(
        MeanAggregate(), WindowResult(), accumulator_type=Types.PICKLED_BYTE_ARRAY(),
        output_type=Types.STRING()).name("Five-minute wide means").uid("business-window-5m")
