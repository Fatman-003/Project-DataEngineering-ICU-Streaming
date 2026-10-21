"""HF workloads preserve scalar contracts, sparse probes and event-time evidence."""
import contextlib
import io
import math
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock, patch

PROJECT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT / "scripts/demo"), str(PROJECT / "jobs/flink")]
import orchestrate
import run_icu_demo as normal
import high_frequency as hf
from contracts import CHART_FIELDS, LAB_FIELDS, SIGNALS, TOPICS
from event_time import event_timestamp_ms
from hypotension import advance_episode
from runtime_config import WindowAlertConfig
from validation import validate_event
from test_orchestration import Clock, FakeProducer


class ProfileTests(unittest.TestCase):
    def test_rates_intervals_counts_and_existing_duration_option(self):
        for frequency, interval, rate in ((100, 10, 2400), (200, 5, 4800)):
            with self.subTest(frequency=frequency):
                settings = hf.profile(frequency, 10 / 60)
                self.assertEqual((settings["patients"], settings["signals_per_patient"],
                                  settings["events_per_tick"]), (4, 6, 24))
                self.assertEqual(settings["tick_interval_ms"], interval)
                self.assertEqual(settings["target_numeric_events_per_second"], rate)
                self.assertEqual(settings["ticks"], frequency * 10)
                self.assertEqual(settings["expected_numeric_events"], rate * 10)
                self.assertEqual(hf.profile(frequency, 1)["expected_numeric_events"], rate * 60)
                for parse in (orchestrate.arguments, normal.arguments):
                    args = parse(["--high-frequency", str(frequency), "--duration-minutes", "1"])
                    self.assertEqual((args.high_frequency, args.duration_minutes), (frequency, 1))

    def test_invalid_profiles_and_durations_fail_before_kafka(self):
        for frequency in (0, -1, 300, 100.0, True, None):
            with self.subTest(frequency=frequency), self.assertRaises(ValueError):
                hf.profile(frequency)
        for duration in (0, -1, math.inf, math.nan, 1e-12):
            with self.subTest(duration=duration), self.assertRaises(ValueError):
                hf.profile(100, duration)
        for parse in (orchestrate.arguments, normal.arguments):
            for value in ("0", "300"):
                with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    parse(["--high-frequency", value])

    def test_conflicting_modes_rejected_before_orchestration(self):
        for parse, options in ((orchestrate.arguments, ("--fast-verify", "--loop", "--raw")),
                               (normal.arguments, ("--loop", "--raw"))):
            for option in options:
                with self.subTest(option=option), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    parse(["--high-frequency", "100", option])
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                parse(["--high-frequency", "200", "--interval-seconds", "0.5"])
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            normal.arguments(["--high-frequency", "100", "--speed", "24"])

    def test_default_and_fast_verify_remain_opt_out(self):
        for parse in (orchestrate.arguments, normal.arguments):
            args = parse([])
            self.assertEqual((args.high_frequency, args.duration_minutes, args.interval_seconds), (None, 6, 1))
        self.assertIsNone(orchestrate.arguments(["--fast-verify"]).high_frequency)


class GenerationTests(unittest.TestCase):
    def test_every_tick_has_all_signals_separate_and_exact_clock(self):
        for frequency, step in ((100, 10), (200, 5)):
            for tick in (0, 1, 2, 90123):
                with self.subTest(frequency=frequency, tick=tick):
                    records = [event for kind, topic, event in hf.tick_events(tick, frequency)
                               if kind == "numeric"]
                    self.assertEqual(len(records), 24)
                    self.assertEqual(len({event["event_time"] for event in records}), 1)
                    self.assertEqual(event_timestamp_ms(records[0]) - event_timestamp_ms(
                        normal.chart_event(normal.PATIENTS[0], "HR", 0, normal.DEFAULT_START)), tick * step)
                    for patient in normal.PATIENTS:
                        self.assertEqual({e["signal_name"] for e in records if e["stay_id"] == patient["stay_id"]}, set(SIGNALS))
                    for event in records:
                        self.assertEqual(set(event), CHART_FIELDS)
                        validate_event(event, "chartevents")

    def test_adjacent_ticks_ids_are_unique_and_repeatable(self):
        for frequency in (100, 200):
            records = list(hf.high_frequency_events(frequency, 2 / 60))
            self.assertEqual(records, list(hf.high_frequency_events(frequency, 2 / 60)))
            self.assertEqual(len(records), 24 * frequency * 2 + 4)
            self.assertEqual(len({event["event_id"] for _, event in records}), len(records))
            for topic, event in records:
                self.assertEqual(set(event), CHART_FIELDS if topic == TOPICS["chartevents"] else LAB_FIELDS)
                validate_event(event, "chartevents" if topic == TOPICS["chartevents"] else "labevents")

    def test_labs_and_p04_probes_stay_sparse_and_separate(self):
        for frequency in (100, 200):
            labs, extras = [], []
            for second in (0, 45, 120, 210, 240, 300):
                for offset in (0, 1):
                    for kind, _, event in hf.tick_events(second * frequency + offset, frequency):
                        if kind == "lab":
                            labs.append(event)
                        elif kind == "extra":
                            extras.append(event)
            self.assertEqual(len(labs), 8)
            self.assertEqual(len(extras), 4)
            self.assertEqual(sum(e["event_id"].endswith("-ooo") for e in extras), 1)
            self.assertEqual(sum(e["event_id"].endswith("-late") for e in extras), 1)
            self.assertEqual(sum(e["valuenum"] == -5 for e in extras), 1)
            at_duplicate = list(hf.tick_events(120 * frequency, frequency))
            ids = [e["event_id"] for kind, _, e in at_duplicate if kind != "lab"]
            self.assertEqual(len(ids) - len(set(ids)), 1)  # Deliberate duplicate only.

    def test_patient_values_and_variation_reuse_normal_scenarios(self):
        for second in (0, 12, 36, 119, 120, 179, 180, 313):
            numeric = [e for kind, _, e in hf.tick_events(second * 100, 100) if kind == "numeric"]
            expected = [normal.chart_event(p, s, second, normal.DEFAULT_START)
                        for p in normal.PATIENTS for s in SIGNALS]
            self.assertEqual(numeric, expected)
        self.assertNotEqual(normal.chart_event(normal.PATIENTS[0], "HR", 0, normal.DEFAULT_START)["valuenum"],
                            normal.chart_event(normal.PATIENTS[0], "HR", 36, normal.DEFAULT_START)["valuenum"])

    def test_long_workload_is_lazy(self):
        stream = hf.high_frequency_events(200, 600)
        self.assertIs(iter(stream), stream)
        self.assertEqual(next(stream)[1]["signal_name"], "HR")

    def test_required_evidence_is_300_event_seconds_at_every_frequency(self):
        for frequency in (1 / 6, 100, 200):
            state, outputs = None, []
            for tick in range(round(300 * frequency) + 1):
                event = normal.chart_event(normal.PATIENTS[1], "MAP", tick / frequency, normal.DEFAULT_START)
                event["valuenum"] = 62
                state, emitted = advance_episode(state, {"event": event}, WindowAlertConfig())
                if tick < 300 * frequency:
                    self.assertEqual(emitted, [])
                outputs.extend(emitted)
            self.assertEqual(len(outputs), 1)
            self.assertEqual(event_timestamp_ms({"event_time": outputs[0]["trigger_time"]}) -
                             event_timestamp_ms({"event_time": outputs[0]["episode_start"]}), 300000)


class ProducerTests(unittest.TestCase):
    def run_with(self, producer, clock, frequency=200, duration=0.05 / 60, quiet=True):
        with patch("confluent_kafka.Producer", return_value=producer) as factory, \
             patch.object(hf.time, "perf_counter", clock.monotonic), patch.object(hf.time, "sleep", clock.sleep):
            result = hf.run_high_frequency(frequency, duration, start=normal.DEFAULT_START, quiet=quiet)
        factory.assert_called_once()
        return result

    def test_one_producer_deadline_pacing_and_one_final_flush(self):
        clock = Clock()
        producer = FakeProducer(clock)
        producer.flush = Mock(wraps=producer.flush)
        result = self.run_with(producer, clock)
        numeric = [r for r in producer.records if r[1] == TOPICS["chartevents"]]
        self.assertEqual(len(numeric), 240)
        for tick in range(10):
            self.assertEqual([r[0] for r in numeric[tick * 24:(tick + 1) * 24]], [1000 + tick / 200] * 24)
        self.assertAlmostEqual(result["actual_seconds"], 0.05)
        self.assertAlmostEqual(result["actual_numeric_events_per_second"], 4800)
        self.assertEqual((result["attempted"], result["queued"], result["delivered"]), (244, 244, 244))
        self.assertEqual(result["counts"]["lab"]["delivered"], 4)
        producer.flush.assert_called_once_with(20)

    def test_stall_keeps_every_tick_and_reports_actual_rate_and_lag(self):
        clock, stalled = Clock(), []
        producer = FakeProducer(clock)
        original = producer.produce
        def send(*args, **kwargs):
            if not stalled:
                clock.sleep(0.1)
                stalled.append(True)
            original(*args, **kwargs)
        producer.produce = send
        result = self.run_with(producer, clock)
        self.assertEqual(result["counts"]["numeric"]["delivered"], 240)
        self.assertLess(result["rate_achievement_percent"], 51)
        self.assertGreater(result["max_scheduling_lag_ms"], 80)
        self.assertGreater(result["ticks_late_by_one_interval"], 0)
        expected = list(hf.high_frequency_events(200, 0.05 / 60))
        self.assertEqual([(r[1], r[3]) for r in producer.records], expected)

    def test_buffer_full_retries_same_event_without_loss(self):
        clock, blocked = Clock(), []
        producer = FakeProducer(clock)
        original = producer.produce
        def send(*args, **kwargs):
            if not blocked:
                blocked.append(True)
                raise BufferError("queue full")
            original(*args, **kwargs)
        producer.produce = send
        producer.poll = lambda seconds: clock.sleep(seconds)
        result = self.run_with(producer, clock)
        self.assertEqual((result["queue_full_retries"], result["attempted"], result["delivered"]), (1, 244, 244))

    def test_deliveries_count_callbacks_including_final_drain(self):
        clock, pending = Clock(), []
        producer = FakeProducer(clock)
        def send(topic, key, value, on_delivery):
            pending.append(on_delivery)
        def flush(seconds):
            for callback in pending:
                callback(None, None)
            return 0
        producer.produce, producer.flush = send, Mock(side_effect=flush)
        result = self.run_with(producer, clock)
        self.assertEqual(result["delivered"], 244)
        producer.flush.assert_called_once()

    def test_undelivered_is_failure_even_if_flush_reports_zero(self):
        clock = Clock()
        producer = FakeProducer(clock)
        producer.produce = Mock()  # Queued, with no delivery confirmations.
        with self.assertRaises(hf.HighFrequencyError) as failure:
            self.run_with(producer, clock)
        result = failure.exception.result
        self.assertEqual((result["queued"], result["delivered"], result["undelivered"]), (244, 0, 244))
        self.assertEqual(result["actual_numeric_events_per_second"], 0)

    def test_callback_failure_reports_and_flushes(self):
        clock = Clock()
        producer = FakeProducer(clock)
        producer.fail = True
        with self.assertRaises(hf.HighFrequencyError) as failure:
            self.run_with(producer, clock)
        result = failure.exception.result
        self.assertEqual((result["attempted"], result["queued"], result["delivered"], result["failures"]), (1, 1, 0, 1))
        self.assertTrue(producer.flushed)

    def test_permanently_full_queue_fails_with_bounded_wait(self):
        clock = Clock()
        producer = FakeProducer(clock)
        producer.produce = Mock(side_effect=BufferError("queue full"))
        producer.poll = lambda seconds: clock.sleep(seconds)
        with self.assertRaises(hf.HighFrequencyError) as failure:
            self.run_with(producer, clock)
        result = failure.exception.result
        self.assertEqual((result["attempted"], result["queued"], result["failures"]), (1, 0, 1))
        self.assertLess(result["actual_seconds"], 15.02)
        self.assertTrue(producer.flushed)

    def test_synchronous_failure_reports_and_flushes(self):
        clock = Clock()
        producer = FakeProducer(clock)
        producer.produce = Mock(side_effect=RuntimeError("broken connection"))
        with self.assertRaises(hf.HighFrequencyError) as failure:
            self.run_with(producer, clock)
        result = failure.exception.result
        self.assertEqual((result["attempted"], result["queued"], result["failures"]), (1, 0, 1))
        self.assertTrue(producer.flushed)

    def test_interruption_drains_and_retains_partial_summary(self):
        clock = Clock()
        producer = FakeProducer(clock)
        producer.poll = Mock(side_effect=KeyboardInterrupt)
        with self.assertRaises(KeyboardInterrupt) as stopped:
            self.run_with(producer, clock)
        self.assertEqual(stopped.exception.result["outcome"], "interrupted")
        self.assertEqual(stopped.exception.result["delivered"], 28)
        self.assertTrue(producer.flushed)

    def test_public_orchestrator_selects_hf_and_preserves_summary(self):
        instance = orchestrate.Orchestrator(orchestrate.arguments(["--high-frequency", "100"]))
        instance.prepare = Mock()
        result = {"event_start": normal.DEFAULT_START.isoformat(), "delivered": 24000}
        with patch.object(orchestrate, "demo_lock", contextlib.nullcontext), \
             patch.object(orchestrate, "run_demo") as rotating, \
             patch.object(hf, "run_high_frequency", return_value=result) as producer, \
             contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(instance.run(), 0)
        producer.assert_called_once_with(100, 6, start=None)
        rotating.assert_not_called()
        self.assertEqual(instance.report["high_frequency"], result)

    def test_public_orchestrator_retains_failed_delivery_summary(self):
        instance = orchestrate.Orchestrator(orchestrate.arguments(["--high-frequency", "200"]))
        instance.prepare = Mock()
        result = {"outcome": "failed", "queued": 24, "delivered": 0, "undelivered": 24}
        with patch.object(orchestrate, "demo_lock", contextlib.nullcontext), \
             patch.object(hf, "run_high_frequency", side_effect=hf.HighFrequencyError("undelivered", result)), \
             contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(instance.run(), 1)
        self.assertEqual(instance.report["high_frequency"], result)
        self.assertEqual(instance.report["outcome"], "failed")


if __name__ == "__main__":
    unittest.main()
