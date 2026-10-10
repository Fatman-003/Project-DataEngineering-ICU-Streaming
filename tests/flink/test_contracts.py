"""No Docker, Kafka, or host PyFlink installation required."""
import json
import sys
import unittest
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "jobs/flink"))
sys.path.insert(0, str(PROJECT / "docker/flink"))
from contracts import TOPICS, selected_topics, parse_event, inspection_line
from prepare_kafka import updated_properties, properties, PROPERTIES

NUMERIC = {
    "event_id": "evt-1", "stay_id": "37323533", "signal_name": "MAP",
    "value": 86.0, "unit": "mmHg", "source_event_time": "2143-09-19T21:11:00",
    "event_time": "2143-09-19T21:11:00",
    "producer_time": "2026-10-10T06:00:00.123456+00:00",
}


class ContractsTests(unittest.TestCase):
    def test_numeric_json_and_identifiers(self):
        self.assertEqual(parse_event(json.dumps(NUMERIC)), NUMERIC)
        view = json.loads(inspection_line(json.dumps(NUMERIC), "vitals"))
        for field in ("event_id", "stay_id", "signal_name", "value"):
            self.assertEqual(view[field], NUMERIC[field])
        self.assertEqual(view["kind"], "RAW_EVENT")

    def test_clinical_null_and_flags_preserved(self):
        event = dict(NUMERIC, event_type="GCS", chart_time="2143-09-20T16:00:00",
                     available_time=None, availability_status="STORETIME_BEFORE_CHARTTIME",
                     validation_flags=["STORETIME_BEFORE_CHARTTIME"])
        decoded = parse_event(json.dumps(event))
        self.assertEqual(decoded, event)
        view = json.loads(inspection_line(json.dumps(event), "clinical"))
        self.assertIn("available_time", view)
        self.assertIsNone(view["available_time"])
        self.assertTrue(view["available_time_present"])
        self.assertEqual(view["validation_flags"], event["validation_flags"])

    def test_verified_clinical_availability_is_not_chart_time(self):
        event = dict(NUMERIC, event_type="LAB", chart_time="2143-09-20T01:20:00",
                     available_time="2143-09-20T01:36:00", value_text="1.5")
        self.assertEqual(parse_event(json.dumps(event)), event)
        view = json.loads(inspection_line(json.dumps(event), "clinical"))
        self.assertEqual(view["available_time"], event["available_time"])
        self.assertNotEqual(view["available_time"], view["chart_time"])

    def test_waveform_chunk_samples_and_nulls(self):
        event = dict(NUMERIC, waveform_record_id="83411188", signal_name="II",
                     sampling_rate_hz=249.89, samples=[None, 0.2, -0.1],
                     missing_sample_count=1, start_time=NUMERIC["event_time"],
                     end_time="2143-09-19T21:11:00.012005")
        decoded = parse_event(json.dumps(event))
        self.assertEqual(decoded["samples"], [None, 0.2, -0.1])
        view = json.loads(inspection_line(json.dumps(event), "waveform"))
        self.assertEqual(view["sample_count"], 3)
        self.assertEqual(view["samples_preview"], event["samples"])
        self.assertEqual(view["sampling_rate_hz"], 249.89)

    def test_timestamps_are_preserved_as_original_strings(self):
        view = json.loads(inspection_line(json.dumps(NUMERIC), "vitals"))
        for field in ("source_event_time", "event_time", "producer_time"):
            self.assertEqual(view[field], NUMERIC[field])

    def test_exact_topic_configuration_and_subsets(self):
        self.assertEqual(set(TOPICS.values()), {
            "icu.v1.vitals.numeric", "icu.v1.clinical.events", "icu.v1.vitals.waveform"})
        self.assertEqual(selected_topics("all"), TOPICS)
        self.assertEqual(selected_topics("clinical"), {"clinical": TOPICS["clinical"]})
        with self.assertRaises(ValueError):
            selected_topics("processed")

    def test_malformed_json_is_visible_and_next_event_still_parses(self):
        for raw in ('{"unfinished":', 'not json', '[1,2]', 'null',
                    '{"value":NaN}', '{"value":Infinity}'):
            result = json.loads(inspection_line(raw, "vitals"))
            self.assertEqual(result["kind"], "PARSE_ERROR")
            self.assertIn("reason", result)
            self.assertNotIn("event_id", result)
        self.assertEqual(json.loads(inspection_line(json.dumps(NUMERIC), "vitals"))["kind"], "RAW_EVENT")

    def test_parser_does_not_redesign_contract_or_validate_business_values(self):
        event = dict(NUMERIC, value=-500, extra={"nested": [None, "original"]})
        self.assertEqual(parse_event(json.dumps(event)), event)
        self.assertEqual(json.loads(inspection_line(json.dumps(event), "vitals"))["value"], -500)

    def test_missing_field_is_distinct_from_explicit_null(self):
        missing = json.loads(inspection_line(json.dumps(NUMERIC), "vitals"))
        explicit = json.loads(inspection_line('{"available_time":null}', "clinical"))
        self.assertFalse(missing["available_time_present"])
        self.assertNotIn("available_time", missing)
        self.assertTrue(explicit["available_time_present"])
        self.assertIsNone(explicit["available_time"])

    def test_listener_patch_preserves_other_broker_properties_and_is_idempotent(self):
        before = ("# unchanged\nnode.id=1\ninter.broker.listener.name=PLAINTEXT\n"
                  "log.dirs=/tmp/kraft-combined-logs\nlisteners=PLAINTEXT://:9092,CONTROLLER://:9093\n"
                  "advertised.listeners=PLAINTEXT://localhost:9092\n"
                  "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT\n")
        updated = updated_properties(before)
        self.assertEqual(properties(updated)["log.dirs"], "/tmp/kraft-combined-logs")
        self.assertEqual(updated_properties(updated), updated)
        self.assertTrue(all(properties(updated)[key] == value for key, value in PROPERTIES.items()))
        with self.assertRaises(ValueError):
            updated_properties(before.replace("node.id=1", "node.id=2"))


if __name__ == "__main__":
    unittest.main()
