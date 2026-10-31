"""Q_window formatting, engine-free: Spark count/avg/var_pop -> Flink's Welford accumulator."""
from business_contract import FIELDS
from window_aggregates import aggregate_result

PRODUCER = "icu-spark"


def accumulator(stats):
    """stats: {field: (count, mean, population variance)} from Spark's native aggregates.

    `window_aggregates` stores Welford [count, mean, M2] with variance = M2 / count,
    so M2 = var_pop * count. Empty fields keep Flink's [0, 0.0, 0.0].
    """
    result = {}
    for field in FIELDS.values():
        count, mean, variance = stats.get(field, (0, None, None))
        count = int(count or 0)
        result[field] = [count, float(mean), float(variance) * count] if count else [0, 0.0, 0.0]
    return result


def window_message(stay_id, start_ms, end_ms, stats):
    result = aggregate_result(accumulator(stats), stay_id, start_ms, end_ms)
    return None if result is None else dict(result, producer=PRODUCER)
