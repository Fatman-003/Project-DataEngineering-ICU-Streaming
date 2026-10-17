"""Actual MAP operator callbacks under a deterministic managed-state/timer fixture."""
import json
import sys
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch, sentinel
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'jobs/flink'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts/demo'))
from hypotension import flink_hypotension
from runtime_config import WindowAlertConfig
from run_icu_demo import chart_event, PATIENTS, DEFAULT_START
from event_time import event_timestamp_ms
CONFIG = WindowAlertConfig()
def record(seconds, value=62, event_id=None):
    event = chart_event(PATIENTS[1], 'MAP', seconds, DEFAULT_START)
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

    def __init__(self):
        self.pending = MemoryMapState()
        self.emitted = MemoryMapState()
        self.episode = MemoryValueState()
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
            self.ttl_arguments = [call.args for call in ttl.call_args_list]

    def get_map_state(self, descriptor):
        self.descriptors[descriptor.name] = descriptor
        return self.emitted if descriptor.name == 'map-emitted-episodes' else self.pending

    def get_state(self, descriptor):
        self.descriptors[descriptor.name] = descriptor
        return self.episode

    def timer_service(self):
        return self

    def register_event_time_timer(self, timestamp):
        self.timers.add(timestamp)

    def current_watermark(self):
        return self.watermark

    def deliver(self, incoming):
        result = self.operator.process_element(json.dumps(incoming), self)
        return [json.loads(value) for value in result or []]

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
        self.assertEqual(json.loads(fixture.episode.value())['start_ms'], event_timestamp_ms(record(290)['event']))

    def test_same_time_recovery_dominates_through_actual_bucket_callbacks(self):
        for simultaneous in ([record(240, 62), record(240, 65, 'recovery')], [record(240, 65, 'recovery'), record(240, 62)]):
            fixture = ManagedOperatorFixture()
            for incoming in [record(t) for t in range(0, 240, 60)] + simultaneous:
                self.assertEqual(fixture.deliver(incoming), [])
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
        self.assertEqual(json.loads(fixture.episode.value())['last_low_ms'], event_timestamp_ms(record(0)['event']))
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
        self.assertEqual(set(fixture.descriptors), {'map-pending', 'map-episode', 'map-emitted-episodes'})
        self.assertEqual(fixture.ttl_arguments, [(24,), (24,), (24,)])
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
        for seconds in range(320, 621, 60):
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
