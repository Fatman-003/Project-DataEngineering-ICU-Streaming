"""Secondary context through actual alert callbacks, with no Kafka/DB writes."""
import json
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import patch

from test_map_operator import ManagedOperatorFixture, record
from business_contract import encode, headers, stable_uuid, validate
from contracts import TOPICS
from event_time import event_timestamp_ms
from hypotension import CONTEXT_SIGNALS, alert_identity, attach_hypotension
from run_icu_demo import DEFAULT_START, PATIENTS, chart_event, events_at
from runtime_config import WindowAlertConfig

NORMAL = {"HR": 80, "RR": 18, "SBP": 120, "SpO2": 98}
MIXED = {"HR": 112, "RR": 25, "SBP": 95, "SpO2": 97}
ABNORMAL = dict(MIXED, SpO2=90)
EXPECTED_CONTEXT = ("HR=112 HIGH_HR; RR=25 HIGH_RR; SBP=95 LOW_SBP; "
                    "SpO2=97 NORMAL; additional_abnormalities=3")


def pending_trigger(preprocessed=False):
    fixture = ManagedOperatorFixture(with_classifier=preprocessed)
    deliver = fixture.deliver_preprocessed if preprocessed else fixture.deliver
    for second in range(31):
        deliver(record(second))
        assert fixture.advance_watermark(second - 2) == []
    return fixture


def fire(values):
    fixture = pending_trigger()
    for signal, value in values.items():
        fixture.deliver(record(30.5, value, signal=signal))
    return fixture, fixture.advance_watermark(31)


class HypotensionContextTests(unittest.TestCase):
    def test_a_abnormal_secondary_signals_cannot_shorten_30s_confirmation(self):
        fixture = ManagedOperatorFixture()
        for second in range(30):
            fixture.deliver(record(second))
            for signal, value in ABNORMAL.items():
                fixture.deliver(record(second, value, signal=signal))
            self.assertEqual(fixture.advance_watermark(second - 2), [])
        self.assertEqual(fixture.advance_watermark(30), [])
        self.assertEqual(json.loads(fixture.episode.value())["low_ms"], 29000)

    def test_b_all_normal_context_still_emits_existing_map_alert(self):
        _, outputs = fire(NORMAL)
        self.assertEqual(len(outputs), 1)
        message = outputs[0]["trigger_message"]
        self.assertEqual(message.count(" NORMAL"), 4)
        self.assertTrue(message.endswith("additional_abnormalities=0"))
        self.assertEqual(outputs[0]["trigger_value"], 62)

    def test_c_mixed_context_counts_only_three_secondary_abnormalities(self):
        fixture, outputs = fire(MIXED)
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0]["trigger_message"],
                         "MAP 62 mmHg (< 65): >= 30s accumulated low evidence; " + EXPECTED_CONTEXT)
        # Changing context after activation cannot create another alert in this episode.
        for second in range(32, 36):
            fixture.deliver(record(second))
            for signal, value in ABNORMAL.items():
                fixture.deliver(record(second, value, signal=signal))
            self.assertEqual(fixture.advance_watermark(second + 1), [])

    def test_d_missing_signals_are_unknown_and_never_borrow_adjacent_buckets(self):
        for present in ({}, {"HR": 112, "SBP": 95}):
            with self.subTest(present=present):
                fixture = pending_trigger()
                for signal, value in ABNORMAL.items():
                    fixture.deliver(record(29.5, value, signal=signal))
                    fixture.deliver(record(31, value, signal=signal))
                for signal, value in present.items():
                    fixture.deliver(record(30.5, value, signal=signal))
                outputs = fixture.advance_watermark(31)
                self.assertEqual(len(outputs), 1)
                message = outputs[0]["trigger_message"]
                for signal in set(CONTEXT_SIGNALS) - set(present):
                    self.assertIn(signal + "=UNKNOWN", message)
                self.assertTrue(message.endswith(f"additional_abnormalities={len(present)}"))
                fixture.advance_watermark(32)
                self.assertEqual(fixture.context.stored, {})

    def test_e_threshold_boundaries_and_fractional_values(self):
        cases = [
            ({"HR": 100, "RR": 22, "SBP": 100, "SpO2": 91},
             ("HR=100 NORMAL", "RR=22 HIGH_RR", "SBP=100 LOW_SBP", "SpO2=91 LOW_SPO2"), 3),
            ({"HR": 100.000000001, "RR": 21.999, "SBP": 100.001, "SpO2": 91.001},
             ("HR=100.000000001 HIGH_HR", "RR=21.999 NORMAL", "SBP=100.001 NORMAL",
              "SpO2=91.001 NORMAL"), 1),
        ]
        for values, descriptions, count in cases:
            with self.subTest(values=values):
                _, outputs = fire(values)
                message = outputs[0]["trigger_message"]
                for description in descriptions:
                    self.assertIn(description, message)
                self.assertTrue(message.endswith(f"additional_abnormalities={count}"))

    def test_f_accepted_out_of_order_context_has_deterministic_finalized_medians(self):
        inputs = [record(30 + tick / 10, value, signal=signal)
                  for signal, values in {"HR": (102, 112, 122), "RR": (21, 25, 29),
                                         "SBP": (90, 95, 105), "SpO2": (95, 97, 99)}.items()
                  for tick, value in enumerate(values, 1)]
        results = []
        for ordering in (inputs, list(reversed(inputs))):
            fixture = pending_trigger(preprocessed=True)
            fixture.deliver_preprocessed(record(30.9))
            for incoming in ordering:
                fixture.deliver_preprocessed(incoming)
            self.assertEqual(fixture.dlq, [])
            self.assertEqual(fixture.advance_watermark(30.998), [])
            outputs = fixture.advance_watermark(30.999)
            self.assertEqual(len(outputs), 1)
            self.assertTrue(outputs[0]["trigger_message"].endswith(EXPECTED_CONTEXT))
            results.append({k: v for k, v in outputs[0].items() if k != "produced_at"})
        self.assertEqual(results[0], results[1])

    def test_g_validation_dedup_and_effective_watermark_protect_context(self):
        fixture = pending_trigger(preprocessed=True)
        for signal, value in MIXED.items():
            fixture.deliver_preprocessed(record(30.5, value, signal=signal))
        before = dict(fixture.context.stored)
        for signal in CONTEXT_SIGNALS:
            fixture.deliver_preprocessed(record(30.7, -5, signal=signal))
        bad_unit = record(30.8, 80, signal="HR")
        bad_unit["event"]["valueuom"] = "mmHg"
        fixture.deliver_preprocessed(bad_unit)
        fixture.deliver_preprocessed(record(30.5, 80, signal="HR"))  # Conflicting duplicate ID.
        fixture.advance_watermark(30.6)
        for signal, value in NORMAL.items():
            fixture.deliver_preprocessed(record(30.6, value, signal=signal))  # Exactly at WM.
        self.assertEqual(fixture.context.stored, before)
        self.assertEqual(len(fixture.dlq), 10)
        self.assertEqual(sum(e["error_message"].startswith("LATE_EVENT") for e in fixture.dlq), 4)
        outputs = fixture.advance_watermark(31)
        self.assertEqual(len(outputs), 1)
        self.assertTrue(outputs[0]["trigger_message"].endswith(EXPECTED_CONTEXT))
        state = fixture.episode.value()
        fixture.deliver_preprocessed(record(30.9, 80, signal="HR"))
        self.assertEqual(fixture.context.stored, {})
        self.assertEqual(fixture.episode.value(), state)
        self.assertEqual(fixture.advance_watermark(32), [])

    def test_h_normal_hf100_hf200_have_equal_one_second_interpretation(self):
        results = []
        for frequency in (1, 100, 200):
            with self.subTest(frequency=frequency):
                fixture = ManagedOperatorFixture()
                outputs = []
                for second in range(31):
                    for tick in range(frequency):
                        delta = 0 if frequency == 1 else (-1 if tick < frequency // 2 else 1)
                        for signal, center in dict(MIXED, MAP=62).items():
                            fixture.deliver(record(second + tick / frequency, center + delta, signal=signal))
                    for stored in fixture.context.stored.values():
                        self.assertEqual(len(json.loads(stored)), frequency)
                    outputs.extend(fixture.advance_watermark(second - 2))
                    self.assertLessEqual(len(fixture.pending.stored), 4)
                    self.assertLessEqual(len(fixture.context.stored), 4 * len(CONTEXT_SIGNALS))
                outputs.extend(fixture.advance_watermark(32))
                self.assertEqual(len(outputs), 1)
                self.assertEqual(fixture.pending.stored, {})
                self.assertEqual(fixture.context.stored, {})
                self.assertTrue(outputs[0]["trigger_message"].endswith(EXPECTED_CONTEXT))
                results.append({k: v for k, v in outputs[0].items() if k != "produced_at"})
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1], results[2])

    def test_i_j_existing_four_patient_scenarios_preserve_map_results(self):
        fixtures = {p["stay_id"]: ManagedOperatorFixture(with_classifier=True) for p in PATIENTS}
        map_only = {p["stay_id"]: ManagedOperatorFixture() for p in PATIENTS}
        outputs, baseline = [], []
        for second in range(360):
            for topic, event in events_at(second):
                if topic == TOPICS["chartevents"]:
                    outputs.extend(fixtures[event["stay_id"]].deliver_preprocessed({"event": event}))
                    if event["signal_name"] == "MAP":
                        baseline.extend(map_only[event["stay_id"]].deliver({"event": event}))
            for fixture in fixtures.values():
                outputs.extend(fixture.advance_watermark(second - 2))
            for fixture in map_only.values():
                baseline.extend(fixture.advance_watermark(second - 2))
        for fixture in fixtures.values():
            outputs.extend(fixture.advance_watermark(361))
            self.assertEqual(fixture.context.stored, {})
        for fixture in map_only.values():
            baseline.extend(fixture.advance_watermark(361))
        self.assertEqual(len(outputs), 1)
        alert = outputs[0]
        self.assertEqual(alert["stay_id"], PATIENTS[1]["stay_id"])
        self.assertEqual(event_timestamp_ms({"event_time": alert["alert_time"]}),
                         event_timestamp_ms(record(42)["event"]))
        self.assertEqual(alert["alert_id"], alert_identity(alert["stay_id"], event_timestamp_ms(record(12)["event"])))
        for field in ("alert_id", "message_id", "alert_time", "stay_id", "trigger_value", "severity", "status"):
            self.assertEqual(alert[field], baseline[0][field])
        self.assertTrue(alert["trigger_message"].endswith("additional_abnormalities=0"))

    def test_k_kafka_schema_headers_and_identity_remain_unchanged(self):
        for context in (NORMAL, MIXED, {}, {"HR": 100.000000001}):
            with self.subTest(context=context):
                _, outputs = fire(context)
                alert = json.loads(encode(outputs[0]))
                validate(alert, "icu.alerts", key=str(alert["stay_id"]).encode(),
                         kafka_headers=headers(alert), require_headers=True)
                self.assertEqual(set(alert), {"schema_version", "message_id", "event_type", "produced_at",
                                             "producer", "alert_id", "alert_time", "stay_id", "alert_type",
                                             "severity", "trigger_value", "trigger_message", "status"})
                self.assertEqual(alert["schema_version"], "1.0")
                self.assertEqual(alert["event_type"], "SEPSIS_ALERT_CREATED")
                self.assertEqual(alert["alert_type"], "HYPOTENSION_SUSTAINED")
                self.assertEqual((alert["severity"], alert["status"], alert["trigger_value"]), ("CRITICAL", "ACTIVE", 62))
                start = event_timestamp_ms(record(0)["event"])
                self.assertEqual(alert["alert_id"], alert_identity(alert["stay_id"], start))
                self.assertEqual(alert["message_id"], stable_uuid("SEPSIS_ALERT_CREATED", [alert["stay_id"], start]))
                self.assertLessEqual(len(alert["trigger_message"]), 1024)

    def test_secondary_reservoirs_are_bounded_deterministic_and_cleaned_without_map(self):
        records = [record(tick / 600, tick % 100, signal=signal)
                   for tick in range(600) for signal in CONTEXT_SIGNALS]
        retained = []
        for ordering in (records, list(reversed(records))):
            fixture = ManagedOperatorFixture()
            for incoming in ordering:
                fixture.deliver(incoming)
            self.assertEqual(len(fixture.context.stored), 4)
            retained.append({key: json.loads(value) for key, value in fixture.context.stored.items()})
            self.assertTrue(all(len(samples) == 256 for samples in retained[-1].values()))
            self.assertEqual(fixture.advance_watermark(1), [])
            self.assertEqual(fixture.context.stored, {})
            self.assertEqual(fixture.pending.stored, {})
            self.assertIsNone(fixture.episode.value())
            self.assertIsNotNone(fixture.descriptors["map-context-seconds"].ttl)
        self.assertEqual(retained[0], retained[1])

    def test_secondary_timestamps_cannot_bridge_a_map_gap(self):
        fixture = ManagedOperatorFixture()
        fixture.deliver(record(0.001))
        for second in range(4):
            for signal, value in ABNORMAL.items():
                fixture.deliver(record(second + .5, value, signal=signal))
        fixture.deliver(record(3.002))
        self.assertEqual(fixture.advance_watermark(5), [])
        self.assertEqual(json.loads(fixture.episode.value())["low_ms"], 0)

    def test_attached_branch_routes_all_context_signals_and_keys_by_stay(self):
        # Exercise the actual attachment filter; callbacks alone would miss a MAP-only branch.
        common = ModuleType("pyflink.common")
        common.Types = SimpleNamespace(STRING=lambda: "STRING")

        class Stream:
            def filter(self, predicate):
                self.predicate = predicate
                return self

            def key_by(self, selector, key_type):
                self.key = selector
                return self

            def process(self, operator, output_type):
                return self

            def name(self, value):
                return self

            def uid(self, value):
                self.operator_uid = value
                return self

        stream = Stream()
        with patch.dict("sys.modules", {"pyflink": ModuleType("pyflink"), "pyflink.common": common}), \
                patch("hypotension.flink_hypotension"):
            attach_hypotension(stream, WindowAlertConfig(), 24)
        for patient in PATIENTS:
            for signal in ("MAP", *CONTEXT_SIGNALS, "DBP"):
                value = encode({"event": chart_event(patient, signal, 0, DEFAULT_START)})
                self.assertEqual(stream.predicate(value), signal != "DBP")
                self.assertEqual(stream.key(value), str(patient["stay_id"]))
        self.assertEqual(stream.operator_uid, "business-map-rule")


if __name__ == "__main__":
    unittest.main()
