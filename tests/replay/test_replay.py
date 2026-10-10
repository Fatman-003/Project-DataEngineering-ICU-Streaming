"""Broker-free tests of replay timing, wire semantics, decoding and failures."""
import contextlib
import io
import json
import math
import random
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch, Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "scripts/replay"))
import replay
import waveforms
from serializers import encode, message_key, stamp

T = datetime(2143, 9, 19, 21, 9)


class FakeClock:
    def __init__(self):
        self.value = 100.0
    def __call__(self):
        return self.value
    def advance(self, seconds):
        self.value += seconds


class FakeProducer:
    def __init__(self, error=None, full_once=False, pending=0):
        self.error = error
        self.full_once = full_once
        self.pending = pending
        self.messages = []
        self.callbacks = []
        self.flushed = False
    def produce(self, topic, key, value, on_delivery):
        if self.full_once:
            self.full_once = False
            raise BufferError()
        self.messages.append((topic, key, json.loads(value)))
        self.callbacks.append(on_delivery)
    def poll(self, seconds):
        callbacks, self.callbacks = self.callbacks, []
        for callback in callbacks:
            callback(self.error, None)
    def flush(self, seconds):
        self.flushed = True
        self.poll(0)
        return self.pending


def vital(identifier="b", seconds=0):
    return dict(event_id=identifier, stay_id="37323533", event_time=T + timedelta(seconds=seconds),
                source_event_time=T + timedelta(seconds=seconds), producer_time=None,
                signal_name="HR", value=82.0, extra_canonical_field="retained")


def clinical(available=None):
    return dict(event_id="lab", stay_id="37323533", chart_time=T, available_time=available,
                event_type="LAB", availability_status="UNKNOWN_STORETIME",
                validation_flags=["UNKNOWN_STORETIME"], value_text="1.5")


def manifest(signal="II", fs=62.4725, spf=4, first=0, last=320):
    return dict(stay_id="37323533", waveform_record_id="83411188",
                waveform_segment_id="83411188_0001", channel_index=0,
                raw_segment_start_time=T, source_start_time=T + timedelta(seconds=first / fs),
                source_end_time=T + timedelta(seconds=last / fs),
                source_frame_start=first, source_frame_end=last,
                signal_name=signal, frame_frequency_hz=fs, samples_per_frame=spf,
                sampling_rate_hz=fs * spf)


class ReplayTests(unittest.TestCase):
    def test_ordering_is_stable_across_input_permutations(self):
        rows = [vital("b"), vital("a"), vital("z", 60)]
        for seed in range(8):
            random.Random(seed).shuffle(rows)
            self.assertEqual([e["event_id"] for e in replay.prepare_events(rows, "vitals")],
                             ["a", "b", "z"])

    def test_delays_at_all_supported_speeds(self):
        for speed, expected in [(1, 60), (10, 6), (60, 1)]:
            self.assertEqual(replay.delay_seconds(T, T + timedelta(seconds=60), speed), expected)
        for speed in (0, -1, math.nan, math.inf):
            with self.assertRaises(ValueError):
                replay.delay_seconds(T, T, speed)
        with self.assertRaises(ValueError):
            replay.delay_seconds(T, T - timedelta(seconds=1), 1)

    def test_monotonic_pacing_and_no_cumulative_drift(self):
        for speed in (1, 10, 60):
            clock = FakeClock()
            pacer = replay.Pacer(speed, clock)
            pacer.wait(T, clock.advance)
            clock.advance(0.15)  # serialization/decoder overhead
            pacer.wait(T + timedelta(seconds=60), clock.advance)
            self.assertAlmostEqual(clock(), 100 + 60 / speed, places=7)
            pacer.wait(T + timedelta(seconds=120), clock.advance)
            self.assertAlmostEqual(clock(), 100 + 120 / speed, places=7)

    def test_known_clinical_availability_controls_pacing(self):
        row = clinical(T + timedelta(minutes=12))
        event = replay.prepare_events([row], "clinical")[0]
        self.assertEqual(event["event_time"], row["available_time"])
        self.assertEqual(event["source_event_time"], T)
        self.assertEqual(row, {k: event[k] for k in row})

    def test_null_availability_remains_null_on_wire(self):
        row = clinical()
        event = replay.prepare_events([row], "clinical")[0]
        payload = json.loads(encode(stamp(event, "session")))
        self.assertIsNone(payload["available_time"])
        self.assertEqual(payload["replay_time_basis"], "chart_time_unknown_availability")
        self.assertEqual(payload["validation_flags"], ["UNKNOWN_STORETIME"])
        self.assertEqual(payload["chart_time"], T.isoformat())
        self.assertIsNone(row["available_time"])

    def test_json_dates_unicode_nested_null_nonfinite(self):
        payload = json.loads(encode({"time": T, "nested": [None, math.nan, math.inf, "µV"]}))
        self.assertEqual(payload, {"time": T.isoformat(), "nested": [None, None, None, "µV"]})

    def test_key_is_stay_id_and_missing_is_error(self):
        self.assertEqual(message_key(vital()), b"37323533")
        for value in (None, "", " "):
            with self.assertRaises(ValueError):
                message_key({"stay_id": value})

    def test_runtime_stamp_preserves_source_and_canonical_fields(self):
        row = vital()
        now = datetime(2026, 10, 10, tzinfo=timezone.utc)
        payload = stamp(row, "session", now)
        self.assertEqual(payload["producer_time"], now)
        self.assertEqual(payload["source_event_time"], T)
        self.assertEqual(payload["extra_canonical_field"], "retained")
        self.assertIsNone(row["producer_time"])

    def test_frame_chunks_cover_once_at_fractional_rate(self):
        frames = list(waveforms.chunk_frames(19, 1000, 62.4725))
        self.assertEqual(frames[0][0], 19)
        self.assertEqual(frames[-1][1], 1000)
        self.assertEqual(sum(b - a for a, b in frames), 981)
        self.assertTrue(all(frames[i][1] == frames[i+1][0] for i in range(len(frames)-1)))
        self.assertTrue(all(b - a in (62, 63) for a, b in frames[:-1]))

    def test_waveform_window_limit_and_stable_order(self):
        rows = [manifest(), dict(manifest("ABP", spf=2), channel_index=1)]
        events = list(waveforms.descriptors(rows, 2))
        repeated = list(waveforms.descriptors(list(reversed(rows)), 2))
        self.assertEqual(events, repeated)
        self.assertEqual(len(events), 4)
        self.assertTrue(all(event["end_time"] <= T + timedelta(seconds=2) for event in events))
        for channel in (0, 1):
            selected = [e for e in events if e["channel_index"] == channel]
            self.assertEqual(sum(e["chunk_frame_end"]-e["chunk_frame_start"] for e in selected), 124)

    def test_window_is_global_not_per_record(self):
        later = manifest()
        later["raw_segment_start_time"] = T + timedelta(days=1)
        later["source_start_time"] = T + timedelta(days=1)
        later["source_end_time"] = T + timedelta(days=1, seconds=5)
        events = list(waveforms.descriptors([manifest(), later], 10))
        self.assertTrue(all(e["start_time"] < T + timedelta(seconds=10) for e in events))

    def test_waveform_clipped_source_frames(self):
        events = list(waveforms.descriptors([manifest(first=30, last=200)], 2))
        self.assertEqual(events[0]["chunk_frame_start"], 30)
        self.assertLessEqual(events[-1]["chunk_frame_end"], 154)

    def test_bounded_decoder_retains_expanded_multirate_samples(self):
        import wfdb
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp).resolve()
            header, data = root / "segment.hea", root / "segment.dat"
            header.touch()
            data.touch()
            event = next(waveforms.descriptors([manifest()], 1))
            event.update(source_header_path=str(header), source_data_reference=str(data))
            description = SimpleNamespace(fs=62.4725, n_sig=1, samps_per_frame=[4],
                                          file_name=["segment.dat"], sig_name=["II"])
            samples = [0.25] * 247 + [math.nan]
            decoded = SimpleNamespace(e_p_signal=[Mock(tolist=lambda: samples)])
            with patch.object(wfdb, "rdheader", return_value=description), patch.object(
                    wfdb, "rdrecord", return_value=decoded) as read:
                result = waveforms.decode_chunk(event, root)
                self.assertEqual(result["sample_count"], 248)
                self.assertEqual(result["missing_sample_count"], 1)
                self.assertIsNone(result["samples"][-1])
                self.assertFalse(read.call_args.kwargs["smooth_frames"])
                self.assertEqual(read.call_args.kwargs["sampto"], 62)
            with self.assertRaises(ValueError):
                waveforms.decode_chunk(event, root / "other")

    def test_delivery_acks_and_flush(self):
        producer = FakeProducer()
        delivery = replay.Delivery(producer, "session")
        delivery.send("topic", vital())
        self.assertEqual(delivery.published, 1)
        self.assertEqual(producer.messages[0][1], b"37323533")
        self.assertEqual(delivery.flush(), 0)
        self.assertTrue(producer.flushed)

    def test_delivery_failure_detected_and_reported(self):
        producer = FakeProducer(error="test delivery failure")
        delivery = replay.Delivery(producer, "session")
        with contextlib.redirect_stderr(io.StringIO()) as output:
            with self.assertRaisesRegex(RuntimeError, "delivery failed"):
                delivery.send("topic", vital())
        self.assertIn("DELIVERY ERROR", output.getvalue())
        self.assertEqual(delivery.failed, 1)
        self.assertEqual(delivery.published, 0)
        delivery.flush()
        self.assertTrue(producer.flushed)

    def test_queue_backpressure_retries_and_restamps(self):
        now = Mock(side_effect=[datetime(2026, 10, 10, tzinfo=timezone.utc),
                               datetime(2026, 10, 10, 0, 0, 1, tzinfo=timezone.utc)])
        producer = FakeProducer(full_once=True)
        delivery = replay.Delivery(producer, "session", now=now)
        delivery.send("topic", vital())
        self.assertEqual(delivery.queued, 1)
        self.assertEqual(now.call_count, 2)
        self.assertEqual(producer.messages[0][2]["producer_time"], "2026-10-10T00:00:01+00:00")

    def run_with(self, producer):
        args = SimpleNamespace(stream="vitals", stay_id="37323533", speed=10,
                               max_events=1, start_time=None, waveform_seconds=10)
        table = Mock(to_pylist=lambda: [vital()])
        with patch.object(replay, "settings", return_value={"reference": Path("."), "bootstrap": "unused"}), \
                patch.object(replay.pq, "read_table", return_value=table), \
                patch.object(replay, "ensure_topics"), \
                patch.object(replay, "Producer", return_value=producer), \
                contextlib.redirect_stdout(io.StringIO()) as output, \
                contextlib.redirect_stderr(io.StringIO()):
            code = replay.run(args)
        return code, json.loads(output.getvalue())

    def test_run_failure_still_flushes_and_nonzero_exit(self):
        producer = FakeProducer(error="failure")
        code, summary = self.run_with(producer)
        self.assertEqual(code, 1)
        self.assertTrue(producer.flushed)
        self.assertEqual(summary["status"], "FAIL")

    def test_flush_pending_prevents_false_success(self):
        producer = FakeProducer(pending=1)
        code, summary = self.run_with(producer)
        self.assertEqual(code, 1)
        self.assertEqual(summary["pending_after_flush"], 1)

    def test_interruption_flushes_and_returns_130(self):
        producer = FakeProducer()
        producer.produce = Mock(side_effect=KeyboardInterrupt())
        code, summary = self.run_with(producer)
        self.assertEqual(code, 130)
        self.assertTrue(producer.flushed)
        self.assertEqual(summary["status"], "INTERRUPTED")


if __name__ == "__main__":
    unittest.main()
