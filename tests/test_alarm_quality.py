"""Engineering recovery debounce through actual managed MAP callbacks."""
import json
import unittest
from unittest.mock import patch

from test_map_operator import ManagedOperatorFixture, record
from event_time import event_timestamp_ms
from runtime_config import RuntimeConfig


def observed(fixture):
    return json.loads(fixture.observed.value())['state']


def timestamp(seconds):
    return event_timestamp_ms(record(seconds)['event'])


def deliver_series(fixture, samples):
    outputs = []
    progress = -1
    for seconds, value in samples:
        outputs.extend(fixture.deliver(record(seconds, value)))
        progress = max(progress, seconds)
        fixture.advance_watermark(progress - RuntimeConfig().max_out_of_orderness_seconds)
    return outputs


class AlarmQualityTests(unittest.TestCase):
    def test_short_low_dip_then_confirmed_recovery_has_no_alert(self):
        fixture = ManagedOperatorFixture()
        output = deliver_series(fixture, [(0, 62), (60, 61), (120, 63), (150, 66), (180, 68)])
        self.assertEqual(output, [])
        self.assertIsNone(observed(fixture))

    def test_150_low_20_normal_150_low_confirms_at_320_not_300(self):
        fixture = ManagedOperatorFixture()
        before = [(0, 62), (60, 61), (120, 63), (150, 66), (160, 67), (170, 62),
                  (230, 61), (290, 60), (300, 62)]
        self.assertEqual(deliver_series(fixture, before), [])
        self.assertEqual(observed(fixture)['start_ms'], timestamp(0))
        self.assertEqual(observed(fixture)['low_ms'], 280000)
        output = fixture.deliver(record(320))
        self.assertEqual(len(output), 1)
        self.assertEqual(output[0]['trigger_time'], record(320)['event']['event_time'])
        self.assertEqual(observed(fixture)['low_ms'], 300000)

    def test_recovery_29_vs_30_seconds_on_normal_or_returning_low(self):
        for duration in (29, 30):
            for value in (62, 66):
                with self.subTest(duration=duration, value=value):
                    fixture = ManagedOperatorFixture()
                    self.assertEqual(deliver_series(fixture, [(0, 62), (60, 62), (120, 62), (150, 66)]), [])
                    self.assertEqual(fixture.deliver(record(150 + duration, value)), [])
                    state = observed(fixture)
                    if duration == 29:
                        self.assertEqual(state['start_ms'], timestamp(0))
                        self.assertEqual(state['low_ms'], 150000)
                        self.assertEqual(state['recovery_start_ms'], None if value < 65 else timestamp(150))
                    elif value >= 65:
                        self.assertIsNone(state)
                    else:
                        self.assertEqual(state['start_ms'], timestamp(180))
                        self.assertEqual(state['low_ms'], 0)

    def test_confirmed_recovery_discards_old_low_evidence(self):
        fixture = ManagedOperatorFixture()
        initial = [(0, 62), (60, 61), (120, 63)] + [(s, 68) for s in range(150, 181, 6)]
        self.assertEqual(deliver_series(fixture, initial), [])
        self.assertIsNone(observed(fixture))
        self.assertEqual(deliver_series(fixture, [(s, 62) for s in range(181, 481, 60)]), [])
        output = fixture.deliver(record(481))
        self.assertEqual(len(output), 1)
        self.assertEqual(event_timestamp_ms({'event_time': output[0]['episode_start']}), timestamp(181))

    def test_oscillation_preserves_episode_without_counting_normal_intervals(self):
        fixture = ManagedOperatorFixture()
        samples = list(zip(range(0, 37, 6), (64, 66, 64, 66, 63, 67, 64)))
        self.assertEqual(deliver_series(fixture, samples), [])
        self.assertEqual(observed(fixture)['start_ms'], timestamp(0))
        self.assertEqual(observed(fixture)['low_ms'], 18000)
        self.assertEqual(deliver_series(fixture, [(s, 62) for s in (90, 150, 210, 270, 300)]), [])
        output = fixture.deliver(record(318))
        self.assertEqual(len(output), 1)
        self.assertEqual(fixture.deliver(record(324, 66)), [])
        self.assertEqual(fixture.deliver(record(330, 62)), [])
        self.assertEqual(fixture.deliver(record(390)), [])
        self.assertEqual(event_timestamp_ms({'event_time': output[0]['episode_start']}), timestamp(0))

    def test_gap_over_90_breaks_continuity_during_or_after_candidate(self):
        for gap_at in ('candidate', 'resumed-low'):
            fixture = ManagedOperatorFixture()
            self.assertEqual(deliver_series(fixture, [(0, 62), (60, 62), (120, 62), (150, 66)]), [])
            next_time = 241
            if gap_at == 'resumed-low':
                self.assertEqual(fixture.deliver(record(170)), [])
                next_time = 261
            self.assertEqual(fixture.deliver(record(next_time)), [])
            self.assertEqual(observed(fixture)['start_ms'], timestamp(next_time))
            self.assertEqual(observed(fixture)['low_ms'], 0)

    def test_duplicate_and_invalid_inputs_have_zero_state_contribution(self):
        fixture = ManagedOperatorFixture(with_classifier=True)
        self.assertEqual(fixture.deliver_preprocessed(record(0)), [])
        self.assertEqual(fixture.deliver_preprocessed(record(60, event_id='low-id')), [])
        before = fixture.observed.value()
        self.assertEqual(fixture.deliver_preprocessed(record(60, event_id='low-id')), [])
        self.assertEqual(fixture.deliver_preprocessed(record(120, event_id='low-id')), [])
        self.assertEqual(fixture.deliver_preprocessed(record(120, -1)), [])
        self.assertEqual(fixture.deliver_preprocessed(record(120, 999)), [])
        self.assertEqual(fixture.observed.value(), before)
        self.assertEqual([e['error_type'] for e in fixture.dlq],
                         ['DUPLICATE_EVENT', 'DUPLICATE_EVENT', 'VALUE_ERROR', 'VALUE_ERROR'])

    def test_stale_recovery_cannot_mutate_cached_recovery_or_alert(self):
        fixture = ManagedOperatorFixture(with_classifier=True)
        for seconds in range(0, 301, 60):
            fixture.deliver_preprocessed(record(seconds))
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        before = fixture.observed.value()
        self.assertEqual(fixture.deliver_preprocessed(record(240, 68, 'old-recovery')), [])
        self.assertEqual(fixture.observed.value(), before)
        self.assertEqual(fixture.dlq[-1]['error_type'], 'LATE_EVENT')
        self.assertEqual(fixture.deliver_preprocessed(record(360)), [])

    def test_rejected_recovery_cannot_confirm_a_candidate(self):
        fixture = ManagedOperatorFixture(with_classifier=True)
        for seconds in (0, 60, 120):
            fixture.deliver_preprocessed(record(seconds))
        fixture.deliver_preprocessed(record(150, 68, 'recovery-id'))
        before = fixture.observed.value()
        self.assertEqual(fixture.deliver_preprocessed(record(180, 68, 'recovery-id')), [])
        self.assertEqual(fixture.deliver_preprocessed(record(180, 999)), [])
        self.assertEqual(fixture.observed.value(), before)
        self.assertEqual(fixture.deliver_preprocessed(record(179)), [])
        self.assertEqual(observed(fixture)['start_ms'], timestamp(0))
        self.assertEqual(observed(fixture)['low_ms'], 150000)

    def test_normal_other_vitals_cannot_confirm_recovery(self):
        fixture = ManagedOperatorFixture()
        deliver_series(fixture, [(0, 62), (60, 62), (120, 62), (150, 68)])
        before = fixture.observed.value()
        for signal, value in (('HR', 80), ('SpO2', 98), ('RR', 16), ('SBP', 120), ('DBP', 80)):
            self.assertEqual(fixture.deliver(record(180, value, signal=signal)), [])
        self.assertEqual(fixture.observed.value(), before)

    def test_out_of_order_transient_recovery_excludes_six_seconds(self):
        fixture = ManagedOperatorFixture(with_classifier=True)
        for seconds in (0, 60, 120, 144):
            self.assertEqual(fixture.deliver_classified(record(seconds)), [])
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        self.assertEqual(fixture.deliver_classified(record(138, 68)), [])
        self.assertEqual(observed(fixture)['start_ms'], timestamp(0))
        self.assertEqual(observed(fixture)['low_ms'], 138000)
        for seconds in (204, 264, 300):
            self.assertEqual(fixture.deliver_classified(record(seconds)), [])
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        self.assertEqual(len(fixture.deliver_classified(record(306))), 1)
        self.assertEqual(fixture.dlq, [])

    def test_accepted_ooo_low_can_interrupt_unfinalized_confirmed_recovery(self):
        fixture = ManagedOperatorFixture(with_classifier=True)
        for seconds, value in ((0, 62), (60, 62), (120, 68), (150, 68), (156, 62)):
            self.assertEqual(fixture.deliver_classified(record(seconds, value)), [])
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        self.assertEqual(observed(fixture)['start_ms'], timestamp(156))
        self.assertEqual(fixture.deliver_classified(record(140)), [])
        self.assertEqual(observed(fixture)['start_ms'], timestamp(0))
        self.assertEqual(observed(fixture)['low_ms'], 130000)
        for seconds in (216, 276, 300):
            self.assertEqual(fixture.deliver_classified(record(seconds)), [])
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        self.assertEqual(len(fixture.deliver_classified(record(326))), 1)

    def test_transient_after_alert_does_not_rearm_but_confirmed_recovery_does(self):
        fixture = ManagedOperatorFixture()
        first = deliver_series(fixture, [(s, 62) for s in range(0, 301, 60)])
        self.assertEqual(len(first), 1)
        self.assertEqual(deliver_series(fixture, [(310, 66), (329, 62), (389, 62)]), [])
        self.assertEqual(fixture.deliver(record(400, 68)), [])
        self.assertEqual(fixture.deliver(record(430, 68)), [])
        self.assertEqual(deliver_series(fixture, [(s, 62) for s in range(440, 740, 60)]), [])
        second = fixture.deliver(record(740))
        self.assertEqual(len(second), 1)
        self.assertNotEqual(first[0]['alert_id'], second[0]['alert_id'])

    def test_in_order_updates_do_not_scan_pending_history(self):
        fixture = ManagedOperatorFixture()
        fixture.deliver(record(0))
        with patch.object(fixture.pending, 'items', side_effect=AssertionError('In-order history rescan')):
            outputs = deliver_series(fixture, [(s, 62) for s in range(6, 301, 6)])
        self.assertEqual(len(outputs), 1)

    def test_watermark_does_not_invent_recovery_confirmation(self):
        fixture = ManagedOperatorFixture()
        self.assertEqual(deliver_series(fixture, [(0, 62), (60, 62), (120, 62), (150, 66)]), [])
        self.assertEqual(fixture.advance_watermark(300), [])
        state = json.loads(fixture.episode.value())
        self.assertEqual(state['recovery_start_ms'], timestamp(150))
        self.assertEqual(state['low_ms'], 150000)

    def test_recovery_uses_time_independently_of_sample_count(self):
        for normal_times in ((150, 180), tuple(range(150, 181))):
            fixture = ManagedOperatorFixture()
            self.assertEqual(deliver_series(fixture, [(0, 62), (60, 62), (120, 62)] +
                                             [(s, 68) for s in normal_times]), [])
            self.assertIsNone(observed(fixture))


if __name__ == '__main__':
    unittest.main()
