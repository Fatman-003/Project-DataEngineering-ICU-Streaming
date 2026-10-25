"""Per-signal means and population variances in five-minute windows, sliding 1m."""
import json
from datetime import datetime, timedelta, timezone
from business_contract import FIELDS, metadata, encode

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def iso_timestamp(timestamp_ms):
    return (EPOCH + timedelta(milliseconds=timestamp_ms)).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def create_accumulator():
    # Welford state: count, mean, sum of squared deviations (M2), per signal.
    return {field: [0, 0.0, 0.0] for field in FIELDS.values()}


def add_record(accumulator, event):
    for field in accumulator:
        value = event.get(field)
        if value is not None:
            count, mean, m2 = accumulator[field]
            count += 1
            delta = value - mean
            mean += delta / count
            accumulator[field] = [count, mean, m2 + delta * (value - mean)]
    return accumulator


def merge_accumulators(left, right):
    merged = {}
    for field in left:
        count_a, mean_a, m2_a = left[field]
        count_b, mean_b, m2_b = right[field]
        count = count_a + count_b
        if not count_a or not count_b:
            merged[field] = list(right[field] if count_b else left[field])
        else:
            delta = mean_b - mean_a
            merged[field] = [count, mean_a + delta * count_b / count,
                             m2_a + m2_b + delta * delta * count_a * count_b / count]
    return merged


def aggregate_result(accumulator, stay, window_start_ms, window_end_ms, emitted_at_ms=None):
    if window_end_ms - window_start_ms != 300000:
        raise ValueError("Only five-minute windows are supported")
    if not any(count for count, mean, m2 in accumulator.values()):
        return None
    return dict(metadata("VITALS_WINDOW_5M", [int(stay), window_end_ms],
                         produced_at=iso_timestamp(emitted_at_ms) if emitted_at_ms is not None else None),
                window_end=iso_timestamp(window_end_ms), stay_id=int(stay),
                **{"avg_" + field: mean if count else None
                   for field, (count, mean, m2) in accumulator.items()},
                **{"var_" + field: max(0.0, m2 / count) if count >= 2 else None
                   for field, (count, mean, m2) in accumulator.items()})


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
