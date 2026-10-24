"""Recovery/continuity requirements through the real managed MAP callbacks."""
import json
import unittest
from test_map_operator import ManagedOperatorFixture, record
from hypotension import median_map, advance_episode
from runtime_config import WindowAlertConfig
from event_time import event_timestamp_ms

def series(samples, preprocessed=False):
    fixture=ManagedOperatorFixture(with_classifier=preprocessed)
    outputs=[]
    for second,value in samples:
        method=fixture.deliver_preprocessed if preprocessed else fixture.deliver
        outputs.extend(method(record(second,value)))
        outputs.extend(fixture.advance_watermark(second-2))
    if samples:
        outputs.extend(fixture.advance_watermark(samples[-1][0]+2))
    return fixture,outputs


class AlarmQualityTests(unittest.TestCase):
    def test_29_and_30_seconds(self):
        fixture=ManagedOperatorFixture()
        outputs=[]
        for second in range(30):
            fixture.deliver(record(second))
            outputs+=fixture.advance_watermark(second-2)
        outputs+=fixture.advance_watermark(29.999)
        self.assertEqual(outputs,[])
        fixture.deliver(record(30))
        outputs=fixture.advance_watermark(31)
        self.assertEqual(len(outputs),1)
        self.assertEqual(outputs[0]["alert_time"],record(30)["event"]["event_time"].replace("Z",".000Z"))

    def test_four_positive_buckets_do_not_reset_but_five_do(self):
        for count in (4,5):
            fixture,outputs=series([(s,62) for s in range(10)]+[(s,66) for s in range(10,10+count)])
            self.assertEqual(outputs,[])
            state=fixture.episode.value()
            if count==4:
                self.assertIsNotNone(state)
                self.assertEqual(len(json.loads(state)["positives"]),4)
            else:
                self.assertIsNone(state)

    def test_15_low_three_normal_15_low_alerts_at33(self):
        samples=[(s,66 if 15<=s<18 else 62) for s in range(34)]
        fixture,outputs=series(samples)
        self.assertEqual(len(outputs),1)
        origin=event_timestamp_ms(record(0)["event"])
        self.assertEqual(event_timestamp_ms({"event_time":outputs[0]["alert_time"]})-origin,33000)

    def test_five_positives_outside_rolling10s_do_not_recover(self):
        fixture,outputs=series([(s,66 if s in (10,13,16,19,22) else 62) for s in range(23)])
        self.assertEqual(outputs,[])
        self.assertIsNotNone(fixture.episode.value())
        self.assertEqual(len(json.loads(fixture.episode.value())["positives"]),4)

    def test_second_episode_rearms_after_five_normal_buckets(self):
        fixture,outputs=series([(s,66 if 31<=s<=35 else 62) for s in range(67)])
        self.assertEqual(len(outputs),2)
        self.assertNotEqual(outputs[0]["alert_id"],outputs[1]["alert_id"])

    def test_gap3_is_inclusive_and_gap4_resets(self):
        _,outputs=series([(s,62) for s in range(0,31,3)])
        self.assertEqual(len(outputs),1)
        fixture,outputs=series([(0,62),(4,62)])
        self.assertEqual(outputs,[])
        self.assertEqual(json.loads(fixture.episode.value())["low_ms"],0)

    def test_actual_raw_gap_over3s_resets_even_when_bucket_starts_only3s_apart(self):
        fixture,outputs=series([(0.001,62),(3.002,62)])
        self.assertEqual(outputs,[])
        self.assertEqual(json.loads(fixture.episode.value())["low_ms"],0)

    def test_hf200_five_raw_records_in25ms_are_only_one_positive_bucket(self):
        fixture=ManagedOperatorFixture()
        fixture.deliver(record(0))
        fixture.advance_watermark(1)
        for i in range(5): fixture.deliver(record(2+i/200,66))
        fixture.advance_watermark(3)
        state=json.loads(fixture.episode.value())
        self.assertEqual(len(state["positives"]),1)

    def test_hf200_five_distinct_positive_seconds_confirm_recovery(self):
        fixture=ManagedOperatorFixture()
        fixture.deliver(record(0))
        fixture.advance_watermark(1)
        for second in range(2,7):
            for tick in range(200): fixture.deliver(record(second+tick/200,66))
            fixture.advance_watermark(second-2)
        fixture.advance_watermark(8)
        self.assertIsNone(fixture.episode.value())

    def test_duplicate_invalid_and_late_normal_records_cannot_recover(self):
        fixture=ManagedOperatorFixture(with_classifier=True)
        fixture.deliver_preprocessed(record(0))
        for i in range(5): fixture.deliver_preprocessed(record(1,66))
        fixture.deliver_preprocessed(record(1.1,-5))
        fixture.advance_watermark(2)
        self.assertEqual(len(json.loads(fixture.episode.value())["positives"]),1)
        before=fixture.episode.value()
        fixture.deliver_preprocessed(record(1.5,66))
        self.assertEqual(fixture.episode.value(),before)
        self.assertEqual(len(fixture.dlq),6)

    def test_accepted_ooo_median_changes_unfinalized_bucket_deterministically(self):
        fixture=ManagedOperatorFixture(with_classifier=True)
        outputs=[]
        for s in range(31):
            outputs+=fixture.deliver_preprocessed(record(s))
            outputs+=fixture.advance_watermark(s-2)
        fixture.deliver_preprocessed(record(29.2,66))
        fixture.deliver_preprocessed(record(29.4,66))
        fixture.deliver_preprocessed(record(31))
        outputs+=fixture.advance_watermark(31)
        self.assertEqual(outputs,[])
        outputs+=fixture.advance_watermark(32)
        self.assertEqual(len(outputs),1)

    def test_too_late_recovery_does_not_retract_emitted_alert(self):
        fixture,outputs=series([(s,62) for s in range(31)],preprocessed=True)
        self.assertEqual(len(outputs),1)
        previous=fixture.episode.value()
        fixture.deliver_preprocessed(record(28,66,event_id="late-normal"))
        self.assertEqual(fixture.episode.value(),previous)
        self.assertTrue(fixture.dlq[-1]["error_message"].startswith("LATE_EVENT"))

    def test_watermark_without_observations_cannot_make30s_evidence(self):
        fixture,outputs=series([(0,62)])
        self.assertEqual(fixture.advance_watermark(100),[])
        self.assertEqual(json.loads(fixture.episode.value())["low_ms"],0)

    def test_second_median_is_exact_at_hf200_and_bounded_above_profile_limit(self):
        from hypotension import bucket_add
        a,b={},{}
        records=[record(i/200,64.9 if i<100 else 65.1)['event'] for i in range(200)]
        for e in records: bucket_add(a,e)
        self.assertEqual(median_map(a),65)
        records=[record(i/5000,60+i%20)['event'] for i in range(5000)]
        a={}
        for e in records: bucket_add(a,e)
        for e in reversed(records): bucket_add(b,e)
        self.assertEqual(a,b)
        self.assertEqual(len(a),256)

    def test_normal_other_vitals_do_not_change_map_episode(self):
        fixture,_=series([(0,62)])
        before=fixture.episode.value()
        fixture.deliver(record(2,80,signal="HR"))
        fixture.advance_watermark(4)
        self.assertEqual(fixture.episode.value(),before)

    def test_pending_buckets_compact_instead_of_retaining_full_episode(self):
        fixture=ManagedOperatorFixture()
        for tick in range(200*40):
            second=tick/200
            fixture.deliver(record(second))
            fixture.advance_watermark(second-2)
            self.assertLessEqual(len(fixture.pending.stored),4)
        self.assertLessEqual(len(json.loads(fixture.episode.value())["positives"]),10)
