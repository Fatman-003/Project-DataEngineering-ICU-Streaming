"""Q_alert logic: the existing Flink alarm tests (cases A-K) re-run on Spark's operator."""
import base64
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

PROJECT = Path(__file__).resolve().parents[2]
for folder in ("jobs/flink", "jobs/spark", "scripts/demo", "tests"):
    sys.path.insert(0, str(PROJECT / folder))
import test_alarm_quality
import test_hypotension_context
from alert_logic import AlertLogic
from contracts import TOPICS
from dlq import rejection
from event_time import event_timestamp_ms
from runtime_pipeline import parse_and_validate
from test_map_operator import record
from wide_logic import NO_WATERMARK, MemoryMap, MemoryValue


class SparkAlertFixture:
    """Same surface as test_map_operator.ManagedOperatorFixture, backed by AlertLogic."""

    def __init__(self, with_classifier=False, factory=None):
        self.logic = AlertLogic(MemoryMap(), MemoryMap(), MemoryMap(), MemoryValue())
        self.pending = SimpleNamespace(stored=self.logic.pending.items)
        self.context = SimpleNamespace(stored=self.logic.context.items)
        self.episode = SimpleNamespace(value=self.logic.episode.get)
        self.watermark, self.dlq, self.offset = NO_WATERMARK, [], 0

    def deliver(self, incoming):
        self.logic.accept(incoming["event"], self.watermark, deduplicate=False)
        return []

    def deliver_preprocessed(self, incoming):
        event = incoming["event"]
        source = {"topic": TOPICS["chartevents"], "partition": 0, "offset": self.offset, "headers": {},
                  "key_base64": None, "payload_base64": base64.b64encode(json.dumps(event).encode()).decode()}
        self.offset += 1
        valid, failed = parse_and_validate(source, "chartevents")
        if failed is not None:
            self.dlq.append(failed)
            return []
        status = self.logic.accept(valid["event"], self.watermark)
        if status in ("DUPLICATE_EVENT", "LATE_EVENT"):
            self.dlq.append(rejection(source, "chartevents", status, status.lower(), event))
        return []

    def advance_watermark(self, seconds):
        self.watermark = event_timestamp_ms(record(seconds)["event"])
        return self.logic.finalize(self.watermark + 1)   # Spark watermark = Flink watermark + 1 ms


class SparkFixtureMixin:
    MODULE = None

    def setUp(self):
        patcher = patch.object(self.MODULE, "ManagedOperatorFixture", SparkAlertFixture)
        patcher.start()
        self.addCleanup(patcher.stop)


class SparkAlarmQualityTests(SparkFixtureMixin, test_alarm_quality.AlarmQualityTests):
    MODULE = test_alarm_quality


class SparkHypotensionContextTests(SparkFixtureMixin, test_hypotension_context.HypotensionContextTests):
    MODULE = test_hypotension_context

    def test_secondary_reservoirs_are_bounded_deterministic_and_cleaned_without_map(self):
        self.skipTest("Asserts Flink's MapStateDescriptor TTL; the bounded reservoir is the shared bucket_add")

    def test_attached_branch_routes_all_context_signals_and_keys_by_stay(self):
        self.skipTest("Exercises PyFlink stream wiring (filter/key_by/uid), not the operator")


class SparkProcessTests(unittest.TestCase):
    def test_process_sorts_by_offset_ignores_rejections_and_sets_next_timer(self):
        logic = AlertLogic(MemoryMap(), MemoryMap(), MemoryMap(), MemoryValue())
        rows = [{"status": "VALID", "offset": 2, "event": record(0.5, 63)["event"]},
                {"status": "REJECTED", "offset": 1, "dlq": {}},
                {"status": "VALID", "offset": 0, "event": record(0.2, 60)["event"]}]
        self.assertEqual(logic.process(rows, None), [])
        start = event_timestamp_ms(record(0)["event"])
        self.assertEqual(logic.next_timer(), start + 999)
        self.assertEqual(json.loads(logic.pending.get(start))["event"]["valuenum"], 60)   # first arrival
        self.assertEqual(logic.process([], start + 999), [])        # Spark wm == T: not yet
        logic.process([], start + 1000)                              # Spark wm > T: finalized
        self.assertIsNone(logic.next_timer())
        self.assertIsNotNone(logic.episode.get())


if __name__ == "__main__":
    unittest.main()
