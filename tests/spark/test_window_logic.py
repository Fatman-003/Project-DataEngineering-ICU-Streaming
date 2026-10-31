"""Q_window formatting must match Flink's Welford aggregate within float tolerance."""
import statistics
import sys
import unittest
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
for folder in ("jobs/flink", "jobs/spark"):
    sys.path.insert(0, str(PROJECT / folder))
from business_contract import validate
from window_aggregates import add_record, aggregate_result, create_accumulator
from window_logic import accumulator, window_message

START, END = 1792180800000, 1792181100000          # 5 minutes, minute-aligned


def spark_stats(samples):
    """What Spark's count/avg/var_pop return for each field (NULLs ignored)."""
    stats = {}
    for field in ("heart_rate", "map", "sbp", "spo2"):
        values = [s[field] for s in samples if s.get(field) is not None]
        stats[field] = (len(values), statistics.fmean(values) if values else None,
                        statistics.pvariance(values) if values else None)
    return stats


def flink_message(samples):
    acc = create_accumulator()
    for sample in samples:
        acc = add_record(acc, sample)
    return aggregate_result(acc, 35727289, START, END)


class WindowLogicTests(unittest.TestCase):
    def test_matches_flink_welford_within_tolerance(self):
        samples = [{"heart_rate": 80 + i % 5, "map": 61 + i % 3, "sbp": 116.5 + i % 4, "spo2": 97 + i % 2}
                   for i in range(300)]
        samples[7]["spo2"] = None
        spark, flink = window_message(35727289, START, END, spark_stats(samples)), flink_message(samples)
        self.assertEqual(spark["message_id"], flink["message_id"])
        self.assertEqual((spark["window_end"], spark["producer"]), (flink["window_end"], "icu-spark"))
        for field in ("heart_rate", "map", "sbp", "spo2"):
            for prefix in ("avg_", "var_"):
                self.assertAlmostEqual(spark[prefix + field], flink[prefix + field], delta=1e-9)
        validate(spark, "icu.vitals.5m")

    def test_single_and_missing_values_follow_flink_nulls(self):
        samples = [{"heart_rate": 80, "map": None, "sbp": None, "spo2": None}]
        spark, flink = window_message(35727289, START, END, spark_stats(samples)), flink_message(samples)
        for field in ("avg_heart_rate", "var_heart_rate", "avg_map", "var_map"):
            self.assertEqual(spark[field], flink[field])
        self.assertIsNone(spark["var_heart_rate"])

    def test_empty_window_yields_nothing(self):
        self.assertIsNone(window_message(35727289, START, END, {}))
        self.assertEqual(accumulator({})["map"], [0, 0.0, 0.0])
        self.assertIsNone(aggregate_result(accumulator({}), 35727289, START, END))


if __name__ == "__main__":
    unittest.main()
