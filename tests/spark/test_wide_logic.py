"""Q_wide logic without a JVM, including parity with the real Flink operators."""
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import sentinel

PROJECT = Path(__file__).resolve().parents[2]
for folder in ("jobs/flink", "jobs/spark", "scripts/demo", "tests"):
    sys.path.insert(0, str(PROJECT / folder))
from business_contract import validate
from business_outputs import flink_telemetry
from contracts import TOPICS
from event_time import event_timestamp_ms
from records import source_record
from run_icu_demo import DEFAULT_START, PATIENTS, chart_event, events_at
from runtime_pipeline import parse_and_validate
from test_map_operator import ManagedOperatorFixture
from wide_logic import MemoryMap, MemoryValue, WideLogic, flink_watermark

P02 = PATIENTS[1]


class Feed:
    """Turns events into Q_wide rows with increasing offsets, like the mapInPandas step."""

    def __init__(self):
        self.offset = 0

    def row(self, event):
        source = source_record(TOPICS["chartevents"], 0, self.offset, str(event["stay_id"]).encode(),
                               json.dumps(event).encode(), [])
        self.offset += 1
        record, failed = parse_and_validate(source, "chartevents")
        if failed is not None:
            return {"status": "REJECTED", "offset": source["offset"], "dlq": failed}
        return {"status": "VALID", "offset": source["offset"], "source": source, "event": record["event"]}


def logic():
    return WideLogic(MemoryMap(), MemoryMap(), MemoryValue())


def ms(second):
    return event_timestamp_ms(chart_event(P02, "HR", second, DEFAULT_START))


def kinds(outputs):
    return [kind if kind == "TELEMETRY" else payload["error_message"].split(":", 1)[0] for kind, payload in outputs]


class WideLogicTests(unittest.TestCase):
    def setUp(self):
        self.feed, self.logic = Feed(), logic()

    def signals(self, second, names=("HR", "MAP", "SBP", "SpO2")):
        return [self.feed.row(chart_event(P02, name, second, DEFAULT_START)) for name in names]

    def test_complete_sample_emits_once_with_contract_and_spark_producer(self):
        outputs = self.logic.process(self.signals(0, ("HR", "MAP", "SBP")), None)
        self.assertEqual(outputs, [])
        outputs = self.logic.process(self.signals(0, ("SpO2", "DBP", "RR")), None)
        self.assertEqual(kinds(outputs), ["TELEMETRY"])
        message = outputs[0][1]
        validate(message, "icu.telemetry")
        self.assertEqual((message["producer"], message["sample_time"]), ("icu-spark", "2026-10-16T20:00:00.000000Z"))
        self.assertEqual(self.logic.next_timer(), ms(0))

    def test_missing_field_is_finalized_only_after_watermark_passes(self):
        self.logic.process(self.signals(0, ("HR", "MAP")), None)
        self.assertEqual(self.logic.process([], ms(0)), [])          # Spark wm == T: Flink wm = T - 1
        outputs = self.logic.process([], ms(0) + 1)                   # Spark wm > T: Flink wm >= T
        self.assertEqual(kinds(outputs), ["TELEMETRY"])
        self.assertIsNone(outputs[0][1]["sbp"])
        self.assertIsNone(self.logic.next_timer())

    def test_duplicate_conflict_and_ignored_signals(self):
        rows = self.signals(0)
        duplicate = dict(rows[0], offset=99)
        outputs = self.logic.process(rows + [duplicate], None)
        self.assertEqual(kinds(outputs), ["TELEMETRY", "DUPLICATE_EVENT"])
        correction = chart_event(P02, "HR", 0, DEFAULT_START, "-fix")
        correction["valuenum"] += 1
        same = chart_event(P02, "MAP", 0, DEFAULT_START, "-same")
        outputs = self.logic.process([self.feed.row(correction), self.feed.row(same)], None)
        self.assertEqual(kinds(outputs), ["IDEMPOTENCY_CONFLICT"])

    def test_rows_are_processed_in_offset_order_and_rejections_pass_through(self):
        bad = chart_event(P02, "SpO2", 0, DEFAULT_START, "-invalid")
        bad["valuenum"] = -5
        rows = self.signals(0) + [self.feed.row(bad)]
        outputs = self.logic.process(list(reversed(rows)), None)
        self.assertEqual(kinds(outputs), ["TELEMETRY", "VALUE_ERROR"])

    def test_out_of_order_sample_still_assembles(self):
        later, earlier = self.signals(5), self.signals(4)
        outputs = self.logic.process(later + earlier, None)
        self.assertEqual([payload["sample_time"][17:19] for _, payload in outputs], ["05", "04"])

    def test_late_classification_matches_flink_watermark(self):
        self.assertEqual(flink_watermark(0), -(2 ** 63))
        self.assertEqual(flink_watermark(ms(3)), ms(3) - 1)
        outputs = self.logic.process(self.signals(3, ("HR",)), ms(3) + 1)
        self.assertEqual(kinds(outputs), ["LATE_EVENT"])

    def test_expire_seen_uses_event_time_horizon(self):
        self.logic.process(self.signals(0, ("HR",)), None)
        self.logic.expire_seen(ms(10), 5000)
        self.assertEqual(self.logic.seen.keys(), [])


class FlinkParityTests(unittest.TestCase):
    """The six-minute demo through Spark's logic and through the real Flink operators."""

    def test_demo_scenario_matches_flink_outputs(self):
        # The fixture emulates one key, so each stay gets its own keyed-state instance.
        flink_by_stay = {patient["stay_id"]: ManagedOperatorFixture(
            with_classifier=True, factory=lambda: flink_telemetry(24, sentinel.dlq)) for patient in PATIENTS}
        feed, spark_by_stay = Feed(), {}
        flink_out, spark_out, spark_dropped = [], [], []
        spark_watermark = max_ts = None
        for second in range(360):
            rows = [feed.row(event) for topic, event in events_at(second) if topic == TOPICS["chartevents"]]
            for row in rows:
                if row["status"] == "VALID":
                    flink_out.extend(flink_by_stay[row["event"]["stay_id"]].deliver_preprocessed({"event": row["event"]}))
            # Spark: one micro-batch per second; rows <= previous batch watermark never reach the function.
            batch = {}
            for row in rows:
                ts = event_timestamp_ms(row["event"]) if row["status"] == "VALID" else None
                if ts is not None and spark_watermark is not None and ts <= spark_watermark:
                    spark_dropped.append(row["event"]["event_id"])
                    continue
                key = str(row["event"]["stay_id"]) if row["status"] == "VALID" else "__invalid__"
                batch.setdefault(key, []).append(row)
                if ts is not None:
                    max_ts = ts if max_ts is None else max(max_ts, ts)
            for key, items in batch.items():
                spark_out.extend(spark_by_stay.setdefault(key, logic()).process(items, spark_watermark))
            spark_watermark = max_ts - 2000
            for key, operator in spark_by_stay.items():          # no-data batch: timeouts
                spark_out.extend(operator.process([], spark_watermark))
            for flink in flink_by_stay.values():
                flink.watermark = max_ts - 2001
                for timer in sorted(t for t in flink.timers if t <= flink.watermark):
                    flink.timers.remove(timer)
                    flink_out.extend(json.loads(value) for value in flink.operator.on_timer(timer, flink) or [])
        rejected = [p for kind, p in spark_out if kind == "DLQ" and p["error_code"] != "PROCESSING_ERROR"]
        spark_tel = {p["message_id"]: p for kind, p in spark_out if kind == "TELEMETRY"}
        flink_tel = {p["message_id"]: p for p in flink_out}
        self.assertEqual(len(spark_tel), 360 * 4)
        self.assertEqual(spark_tel.keys(), flink_tel.keys())
        fields = ("sample_time", "stay_id", "heart_rate", "map", "sbp", "spo2")
        self.assertTrue(all({f: spark_tel[i][f] for f in fields} == {f: flink_tel[i][f] for f in fields}
                            for i in spark_tel))
        spark_dlq = sorted(p["error_message"].split(":")[0] for kind, p in spark_out if kind == "DLQ")
        flink_dlq = sorted([p["error_message"].split(":")[0] for f in flink_by_stay.values() for p in f.dlq]
                           + ["VALUE_ERROR"] * len(rejected))
        # Flink sends the late probe to the DLQ; Spark drops it before the operator (D7).
        self.assertEqual(spark_dropped, [e for e in spark_dropped if e.endswith("-late")])
        self.assertEqual(sorted(spark_dlq + ["LATE_EVENT"] * len(spark_dropped)), flink_dlq)
        self.assertEqual(spark_dlq, ["DUPLICATE_EVENT", "VALUE_ERROR"])


if __name__ == "__main__":
    unittest.main()
