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

    def __init__(self, with_classifier=False, factory=None):
        self.pending = MemoryMapState()
        self.context = MemoryMapState()
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
            self.operator = factory() if factory is not None else flink_hypotension(CONFIG, 24)
            self.operator.open(self)
            if with_classifier:
                self.classifier = flink_time_classifier(24, sentinel.dlq)
                self.classifier.open(self)
            self.ttl_arguments = [call.args for call in ttl.call_args_list]

    def get_map_state(self, descriptor):
        self.descriptors[descriptor.name] = descriptor
        if descriptor.name == 'map-context-seconds':
            return self.context
        return self.emitted if descriptor.name == 'map-emitted-episodes' else self.pending

    def get_state(self, descriptor):
        self.descriptors[descriptor.name] = descriptor
        return {'max-clean-event-time': self.maximum, 'map-observed': self.observed,
                'map-episode': self.episode, 'map-episode-v3':self.episode}[descriptor.name]

    def timestamp(self):
        return self.incoming_timestamp

    def timer_service(self):
        return self

    def register_event_time_timer(self, timestamp):
        self.timers.add(timestamp)

    def current_watermark(self):
        return self.watermark

    def deliver(self, incoming):
        self.incoming_timestamp=event_timestamp_ms(incoming['event'])
        result=self.operator.process_element(json.dumps(incoming),self)
        outputs=[]
        for value in result or []:
            if isinstance(value,tuple): self.dlq.append(json.loads(value[1]))
            else: outputs.append(json.loads(value))
        return outputs

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
