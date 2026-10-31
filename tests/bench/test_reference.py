"""Offline reference on generator output: the documented six-minute expectations."""
import json
import sys
import unittest
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
for folder in ("bench", "jobs/flink", "scripts/demo"):
    sys.path.insert(0, str(PROJECT / folder))
from reference import build_reference
from run_icu_demo import DEFAULT_START, PATIENTS, demo_events


def raw_rows(events):
    offsets = {}
    rows = []
    for topic, event in events:
        offset = offsets[topic] = offsets.get(topic, -1) + 1
        rows.append({"topic": topic, "partition": 0, "offset": offset, "key": str(event.get("stay_id", event.get(
            "mapped_stay_id"))).encode(), "payload": json.dumps(event).encode()})
    return rows


class ReferenceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.reference = build_reference(raw_rows(demo_events(6, DEFAULT_START)))

    def test_standard_scenario_counts_and_tails(self):
        ref = self.reference
        self.assertEqual(len(ref["telemetry"]), 360 * 4)
        self.assertEqual(ref["telemetry_tail"], {})
        self.assertEqual(sorted({key[1] for key in ref["windows"]}),
                         [f"2026-10-16T20:0{m}:00.000Z" for m in range(1, 6)])
        self.assertEqual(len(ref["windows"]), 20)
        self.assertEqual(len(ref["windows_tail"]), 20)
        self.assertEqual(ref["dlq"], {"DUPLICATE_EVENT": 1, "VALUE_ERROR": 1})
        self.assertEqual(len(ref["engine_dependent"]), 1)
        self.assertTrue(ref["engine_dependent"][0].endswith("-late"))

    def test_only_p02_alerts_at_second_42(self):
        (alert,) = self.reference["alerts"]
        self.assertEqual((alert["stay_id"], alert["alert_time"], alert["trigger_value"]),
                         (PATIENTS[1]["stay_id"], "2026-10-16T20:00:42.000Z", 63))
        self.assertTrue(alert["trigger_message"].endswith("additional_abnormalities=0"))


if __name__ == "__main__":
    unittest.main()
