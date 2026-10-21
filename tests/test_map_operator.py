"""Actual MAP operator callbacks under a deterministic managed-state/timer fixture."""
import base64
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch, sentinel
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs/flink'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts/demo'))
from hypotension import flink_hypotension
from runtime_config import WindowAlertConfig, RuntimeConfig
from run_icu_demo import chart_event, demo_events, PATIENTS, DEFAULT_START
from contracts import TOPICS
from event_time import event_timestamp_ms, flink_time_classifier
from runtime_pipeline import parse_and_validate
from dedup import dedup_identity, check_and_remember
from dlq import rejection
CONFIG = WindowAlertConfig()
def record(seconds, value=62, event_id=None, signal='MAP'):
    event = chart_event(PATIENTS[1], signal, seconds, DEFAULT_START)
    event['valuenum'] = value
    if event_id: event['event_id'] = event_id
    return {'event': event}
def alerts(outputs):
    return [o for o in outputs if o.get('alert_type') == 'SUSTAINED_HYPOTENSION']

class MemoryValueState:

    def __init__(self):
        self.stored = None

    def value(self):
        return self.stored

    def update(self, value):
        self.stored = value

    def clear(self):
        self.stored = None

class MemoryMapState:

    def __init__(self):
        self.stored = {}

    def get(self, key):
        return self.stored.get(key)

    def put(self, key, value):
        self.stored[key] = value

    def remove(self, key):
        self.stored.pop(key, None)

    def items(self):
        return self.stored.items()

    def contains(self, key):
        return key in self.stored

class ManagedOperatorFixture:
    """Execute actual callbacks; emulate one key and Flink's ascending timer contract.

    This fixture checks the Python operator's buffering/state wiring, while the
    live integration harness verifies the real Flink timer/checkpoint runtime.
    """

    def __init__(self, with_classifier=False):
        self.pending = MemoryMapState()
        self.emitted = MemoryMapState()
        self.episode = MemoryValueState()
        self.observed = MemoryValueState()
        self.maximum = MemoryValueState()
        self.dlq = []
        self.seen = {}
        self.descriptors = {}
        self.timers = set()
        self.watermark = -2 ** 63
        common = ModuleType('pyflink.common')
        common.Types = SimpleNamespace(LONG=lambda: 'LONG', STRING=lambda: 'STRING', BOOLEAN=lambda: 'BOOLEAN')
        datastream = ModuleType('pyflink.datastream')
        datastream.KeyedProcessFunction = type('KeyedProcessFunction', (), {})
        state = ModuleType('pyflink.datastream.state')

        class Descriptor:

            def __init__(self, name, *type_information):
                self.name = name
                self.type_information = type_information
                self.ttl = None

            def enable_time_to_live(self, ttl):
                self.ttl = ttl
        state.MapStateDescriptor = Descriptor
        state.ValueStateDescriptor = Descriptor
        modules = {'pyflink': ModuleType('pyflink'), 'pyflink.common': common, 'pyflink.datastream': datastream, 'pyflink.datastream.state': state}
        with patch.dict(sys.modules, modules), patch('dedup.managed_ttl', return_value=sentinel.ttl) as ttl:
            self.operator = flink_hypotension(CONFIG, 24)
            self.operator.open(self)
            if with_classifier:
                self.classifier = flink_time_classifier(24, sentinel.dlq)
                self.classifier.open(self)
            self.ttl_arguments = [call.args for call in ttl.call_args_list]

    def get_map_state(self, descriptor):
        self.descriptors[descriptor.name] = descriptor
        return self.emitted if descriptor.name == 'map-emitted-episodes' else self.pending

    def get_state(self, descriptor):
        self.descriptors[descriptor.name] = descriptor
        return {'max-clean-event-time': self.maximum, 'map-observed': self.observed,
                'map-episode': self.episode}[descriptor.name]

    def timestamp(self):
        return self.incoming_timestamp

    def timer_service(self):
        return self

    def register_event_time_timer(self, timestamp):
        self.timers.add(timestamp)

    def current_watermark(self):
        return self.watermark

    def deliver(self, incoming):
        result = self.operator.process_element(json.dumps(incoming), self)
        return [json.loads(value) for value in result or []]

    def deliver_classified(self, incoming):
        event = incoming['event']
        self.incoming_timestamp = event_timestamp_ms(event)
        envelope = dict(incoming, stream_type='chartevents', source=incoming.get('source') or {
            'topic': 'icu.v2.chartevents', 'partition': 0, 'offset': self.incoming_timestamp,
            'payload_base64': base64.b64encode(json.dumps(event).encode()).decode()})
        outputs = []
        for value in self.classifier.process_element(json.dumps(envelope), self):
            if isinstance(value, tuple):
                tag, rejected = value
                assert tag is sentinel.dlq
                self.dlq.append(json.loads(rejected))
            else:
                outputs.extend(self.deliver(json.loads(value)))
        return outputs

    def deliver_preprocessed(self, incoming):
        event = incoming['event']
        source = {'topic': TOPICS['chartevents'], 'partition': 0, 'offset': len(self.seen) + len(self.dlq),
                  'payload_base64': base64.b64encode(json.dumps(event).encode()).decode()}
        valid, failed = parse_and_validate(source, 'chartevents')
        if failed is not None:
            self.dlq.append(failed)
            return []
        seen = self.seen.setdefault(dedup_identity(event, 'chartevents'), MemoryValueState())
        if check_and_remember(seen):
            self.dlq.append(rejection(source, 'chartevents', 'DUPLICATE_EVENT', 'Already seen', event))
            return []
        return self.deliver_classified(valid)

    def advance_watermark(self, seconds):
        self.watermark = event_timestamp_ms(record(seconds)['event'])
        emitted = []
        for timestamp in sorted((timer for timer in self.timers if timer <= self.watermark)):
            self.timers.remove(timestamp)
            emitted.extend((json.loads(value) for value in self.operator.on_timer(timestamp, self) or []))
        return emitted

class HypotensionOperatorTests(unittest.TestCase):

    def test_first_qualifying_arrival_emits_without_watermark(self):
        fixture = ManagedOperatorFixture()
        for seconds in range(0, 300, 60):
            self.assertEqual(fixture.deliver(record(seconds)), [])
        self.assertEqual(fixture.deliver(record(299)), [])
        outputs = fixture.deliver(record(300))
        self.assertEqual(len(alerts(outputs)), 1)
        self.assertIn('alert_emitted_at', outputs[0])
        self.assertIsNone(fixture.episode.value())
        self.assertEqual(fixture.advance_watermark(300), [])
        self.assertTrue(json.loads(fixture.episode.value())['alerted'])
        self.assertEqual(fixture.pending.stored, {})

    def test_known_out_of_order_recovery_prevents_false_alert(self):
        fixture = ManagedOperatorFixture()
        for seconds in range(0, 300, 60):
            fixture.deliver(record(seconds))
        fixture.deliver(record(290))
        fixture.deliver(record(280, 65, 'late-arrival-recovery'))
        self.assertEqual(fixture.deliver(record(300)), [])
        outputs = fixture.advance_watermark(300)
        self.assertEqual(alerts(outputs), [])
        self.assertEqual(outputs, [])
        self.assertEqual(json.loads(fixture.episode.value())['start_ms'], event_timestamp_ms(record(0)['event']))
        self.assertEqual(json.loads(fixture.episode.value())['low_ms'], 290000)

    def test_same_time_recovery_dominates_through_actual_bucket_callbacks(self):
        for simultaneous in ([record(240, 62), record(240, 65, 'recovery')], [record(240, 65, 'recovery'), record(240, 62)]):
            fixture = ManagedOperatorFixture()
            for incoming in [record(t) for t in range(0, 240, 60)] + simultaneous:
                self.assertEqual(fixture.deliver(incoming), [])
            self.assertEqual(fixture.deliver(record(270, 65)), [])
            outputs = fixture.advance_watermark(300)
            self.assertEqual(alerts(outputs), [])
            self.assertEqual(outputs, [])
            self.assertIsNone(fixture.episode.value())

    def test_future_observations_remain_pending_until_their_timer_fires(self):
        fixture = ManagedOperatorFixture()
        fixture.deliver(record(0))
        fixture.deliver(record(60))
        self.assertEqual(fixture.advance_watermark(0), [])
        self.assertEqual(len(fixture.pending.stored), 1)
        self.assertEqual(json.loads(fixture.episode.value())['last_map_ms'], event_timestamp_ms(record(0)['event']))
        self.assertEqual(fixture.advance_watermark(60), [])
        self.assertEqual(fixture.pending.stored, {})

    def test_watermark_alone_cannot_invent_sustained_low_observations(self):
        fixture = ManagedOperatorFixture()
        fixture.deliver(record(0))
        self.assertEqual(alerts(fixture.advance_watermark(600)), [])
        self.assertEqual(fixture.timers, set())
        self.assertFalse(json.loads(fixture.episode.value())['alerted'])

    def test_expired_pending_bucket_is_ignored_when_native_timer_fires(self):
        fixture = ManagedOperatorFixture()
        fixture.deliver(record(0))
        fixture.pending.stored.clear()
        self.assertEqual(fixture.advance_watermark(0), [])
        self.assertIsNone(fixture.episode.value())
        self.assertEqual(fixture.timers, set())

    def test_actual_operator_declares_all_managed_states_with_ttl(self):
        fixture = ManagedOperatorFixture()
        self.assertEqual(set(fixture.descriptors), {'map-pending', 'map-episode', 'map-observed', 'map-emitted-episodes'})
        self.assertEqual(fixture.ttl_arguments, [(24,), (24,), (24,), (24,)])
        for descriptor in fixture.descriptors.values():
            self.assertIs(descriptor.ttl, sentinel.ttl)
        self.assertEqual(fixture.descriptors['map-pending'].type_information, ('LONG', 'STRING'))
        self.assertEqual(fixture.descriptors['map-episode'].type_information, ('STRING',))

    def test_duplicate_and_additional_low_never_emit_twice(self):
        fixture = ManagedOperatorFixture()
        outputs = []
        for seconds in range(0, 361, 60):
            outputs.extend(fixture.deliver(record(seconds)))
            outputs.extend(fixture.deliver(record(seconds)))
            self.assertEqual(fixture.advance_watermark(seconds - 30), [])
        self.assertEqual(len(outputs), 1)

    def test_recovery_rearms_with_distinct_episode_identity(self):
        fixture, outputs = ManagedOperatorFixture(), []
        for seconds in range(0, 301, 60):
            outputs.extend(fixture.deliver(record(seconds)))
        self.assertEqual(fixture.deliver(record(310, 65)), [])
        self.assertEqual(fixture.deliver(record(340, 65)), [])
        for seconds in range(350, 651, 60):
            outputs.extend(fixture.deliver(record(seconds)))
            fixture.advance_watermark(seconds - 30)
        self.assertEqual(len(outputs), 2)
        self.assertNotEqual(outputs[0]['alert_id'], outputs[1]['alert_id'])

    def test_late_history_cannot_create_or_rearm_an_alert(self):
        fixture = ManagedOperatorFixture()
        for seconds in range(0, 301, 60):
            fixture.deliver(record(seconds))
        fixture.advance_watermark(300)
        self.assertEqual(fixture.deliver(record(280, 65)), [])
        self.assertEqual(fixture.deliver(record(299)), [])
        self.assertEqual(fixture.deliver(record(360)), [])

    def test_gap_greater_than_90_seconds_breaks_continuity(self):
        fixture = ManagedOperatorFixture()
        for seconds in (0, 60, 120, 211, 271, 300):
            self.assertEqual(fixture.deliver(record(seconds)), [])

    def test_one_recovery_inside_five_minutes_starts_a_new_episode(self):
        fixture = ManagedOperatorFixture()
        for seconds, value in ((0, 62), (60, 61), (120, 70), (180, 62), (240, 61), (300, 60)):
            self.assertEqual(fixture.deliver(record(seconds, value)), [])
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        for seconds in (360, 420):
            self.assertEqual(fixture.deliver(record(seconds)), [])
        output = fixture.deliver(record(480))
        self.assertEqual(len(output), 1)
        self.assertEqual(event_timestamp_ms({'event_time': output[0]['episode_start']}),
                         event_timestamp_ms(record(180)['event']))

    def test_multiple_normal_maps_do_not_accumulate_low_periods(self):
        fixture = ManagedOperatorFixture()
        for seconds, value in ((0, 62), (60, 61), (120, 68), (180, 73), (240, 69), (300, 62), (360, 61)):
            self.assertEqual(fixture.deliver(record(seconds, value)), [])
        fixture.advance_watermark(360)
        self.assertEqual(json.loads(fixture.episode.value())['start_ms'], event_timestamp_ms(record(300)['event']))

    def test_91_second_gap_does_not_accumulate_from_the_old_start(self):
        fixture = ManagedOperatorFixture()
        for seconds in (0, 60, 151, 211, 271, 331):
            self.assertEqual(fixture.deliver(record(seconds)), [])
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        self.assertEqual(json.loads(fixture.episode.value())['start_ms'], event_timestamp_ms(record(151)['event']))

    def test_exact_90_second_gap_is_inclusive_and_can_confirm(self):
        fixture = ManagedOperatorFixture()
        for seconds in (0, 90, 180, 270):
            self.assertEqual(fixture.deliver(record(seconds)), [])
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        output = fixture.deliver(record(300))
        self.assertEqual(len(output), 1)
        self.assertEqual(event_timestamp_ms({'event_time': output[0]['episode_start']}),
                         event_timestamp_ms(record(0)['event']))

    def test_accepted_out_of_order_transient_pauses_evidence_across_compaction(self):
        fixture = ManagedOperatorFixture(with_classifier=True)
        for seconds in (0, 60, 130):
            self.assertEqual(fixture.deliver_classified(record(seconds)), [])
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        self.assertEqual(fixture.deliver_classified(record(120, 70)), [])
        for seconds in (190, 250, 300):
            self.assertEqual(fixture.deliver_classified(record(seconds)), [])
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        output = fixture.deliver_classified(record(310))
        self.assertEqual(len(output), 1)
        self.assertEqual(event_timestamp_ms({'event_time': output[0]['episode_start']}),
                         event_timestamp_ms(record(0)['event']))
        self.assertEqual(fixture.dlq, [])

    def test_too_late_recovery_routes_to_dlq_without_mutating_alert_state(self):
        fixture, outputs = ManagedOperatorFixture(with_classifier=True), []
        for seconds in range(0, 301, 60):
            outputs.extend(fixture.deliver_classified(record(seconds)))
            fixture.advance_watermark(seconds - RuntimeConfig().max_out_of_orderness_seconds)
        self.assertEqual(len(outputs), 1)
        before = (dict(fixture.pending.stored), fixture.episode.value(), dict(fixture.emitted.stored),
                  fixture.maximum.value())
        self.assertEqual(fixture.deliver_classified(record(240, 71, 'stale-recovery')), [])
        self.assertEqual(fixture.dlq[0]['error_type'], 'LATE_EVENT')
        self.assertEqual(fixture.dlq[0]['event_id'], 'stale-recovery')
        self.assertEqual(before, (dict(fixture.pending.stored), fixture.episode.value(),
                                  dict(fixture.emitted.stored), fixture.maximum.value()))
        self.assertEqual(fixture.deliver_classified(record(360)), [])

    def test_at_watermark_is_stale_but_one_millisecond_after_is_accepted(self):
        fixture = ManagedOperatorFixture(with_classifier=True)
        fixture.advance_watermark(100)
        self.assertEqual(fixture.deliver_classified(record(100, 71)), [])
        self.assertEqual(len(fixture.dlq), 1)
        self.assertEqual(fixture.deliver_classified(record(100.001)), [])
        self.assertEqual(len(fixture.pending.stored), 1)

    def test_normal_other_vitals_do_not_reset_map_state(self):
        fixture, outputs = ManagedOperatorFixture(), []
        for seconds in range(0, 301, 60):
            outputs.extend(fixture.deliver(record(seconds)))
            before = (dict(fixture.pending.stored), fixture.episode.value(), dict(fixture.emitted.stored))
            for signal, value in (('HR', 80), ('SpO2', 98), ('RR', 16), ('SBP', 120), ('DBP', 80)):
                self.assertEqual(fixture.deliver(record(seconds + 1, value, signal=signal)), [])
            self.assertEqual(before, (dict(fixture.pending.stored), fixture.episode.value(), dict(fixture.emitted.stored)))
        self.assertEqual(len(outputs), 1)

    def test_200_second_low_then_recovery_then_independent_300_second_episode(self):
        fixture = ManagedOperatorFixture()
        for seconds in (0, 60, 120, 180, 200):
            self.assertEqual(fixture.deliver(record(seconds)), [])
        self.assertEqual(fixture.deliver(record(210, CONFIG.map_threshold)), [])
        for seconds in range(240, 540, 60):
            self.assertEqual(fixture.deliver(record(seconds)), [])
        output = fixture.deliver(record(540))
        self.assertEqual(len(output), 1)
        self.assertEqual(event_timestamp_ms({'event_time': output[0]['episode_start']}),
                         event_timestamp_ms(record(240)['event']))

    def test_team_manual_p03_example_has_no_alert_at_five_minutes(self):
        fixture = ManagedOperatorFixture()
        for seconds, value in ((30, 62), (90, 61), (120, 71), (180, 62), (240, 61), (300, 60)):
            self.assertEqual(fixture.deliver(record(seconds, value)), [])
        fixture.advance_watermark(300)
        self.assertEqual(json.loads(fixture.episode.value())['start_ms'], event_timestamp_ms(record(180)['event']))

    def test_fast_scenarios_through_ordered_state_and_real_classifier_callbacks(self):
        fixtures = {p['stay_id']: ManagedOperatorFixture(with_classifier=True) for p in PATIENTS}
        outputs = []
        origin = event_timestamp_ms(record(0)['event'])
        progress = {stay: 0 for stay in fixtures}
        for topic, event in demo_events(map_episode_checks=True):
            if topic != TOPICS['chartevents']:
                continue
            fixture = fixtures[event['stay_id']]
            outputs.extend(fixture.deliver_classified({'event': event}))
            progress[event['stay_id']] = max(progress[event['stay_id']],
                                             (event_timestamp_ms(event) - origin) / 1000)
            fixture.advance_watermark(progress[event['stay_id']] - RuntimeConfig().max_out_of_orderness_seconds)
        self.assertEqual([output['stay_id'] for output in outputs], [PATIENTS[1]['stay_id']])
        self.assertEqual(event_timestamp_ms({'event_time': outputs[0]['trigger_time']}) - origin, 313000)
        self.assertTrue(any(e['event_id'].endswith('-map-recovery-stale') and e['error_type'] == 'LATE_EVENT'
                            for e in fixtures[PATIENTS[1]['stay_id']].dlq))
        self.assertFalse(any(e['event_id'].endswith('-map-recovery-ooo')
                             for e in fixtures[PATIENTS[2]['stay_id']].dlq))

    def test_short_low_episode_then_recovery_does_not_alert(self):
        fixture = ManagedOperatorFixture()
        for seconds in (61, 67, 73, 121, 175):
            self.assertEqual(fixture.deliver(record(seconds)), [])
        self.assertEqual(fixture.deliver(record(181, 65)), [])
        self.assertEqual(fixture.advance_watermark(600), [])

    def test_arrival_alert_is_not_retracted_by_later_recovery_history(self):
        fixture = ManagedOperatorFixture()
        for seconds in range(0, 300, 60):
            fixture.deliver(record(seconds))
        self.assertEqual(len(fixture.deliver(record(300))), 1)
        self.assertEqual(fixture.deliver(record(280, 65)), [])
        self.assertEqual(fixture.deliver(record(360)), [])
