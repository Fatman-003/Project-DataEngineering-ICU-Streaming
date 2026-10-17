"""Fast pacing, real-clock latency validation and team/private document boundaries."""
import contextlib
import io
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts/demo"))
import orchestrate
import run_icu_demo as simulator
from fast_verify import transport_timings, SPEED
from contracts import TOPICS, CORE_TOPICS
from test_orchestration import Clock, FakeProducer


class FastVerifyTests(unittest.TestCase):
    def test_fast_flag_is_explicit_and_normal_defaults_remain_realtime(self):
        self.assertFalse(orchestrate.arguments([]).fast_verify)
        self.assertTrue(orchestrate.arguments(["--fast-verify"]).fast_verify)
        self.assertEqual(simulator.arguments([]).speed, 1)
        wrapper = (PROJECT / "run_icu_demo.ps1").read_text(encoding="utf-8-sig")
        self.assertIn("[switch]$FastVerify", wrapper)
        self.assertIn("if ($FastVerify)", wrapper)
        self.assertIn("'--fast-verify'", wrapper)

    def test_fast_mode_rejects_changed_scenario(self):
        for options in (["--loop"], ["--raw"], ["--duration-minutes", "1"], ["--interval-seconds", ".5"]):
            with self.subTest(options=options), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                orchestrate.arguments(["--fast-verify", *options])

    def test_fast_pacing_preserves_every_logical_timestamp_and_raw_payload(self):
        clock = Clock()
        producer = FakeProducer(clock)
        # Include an occasional producer stall: FastVerify must never skip a tick.
        original_poll = producer.poll
        def poll(seconds):
            if len(producer.records) == 8:
                clock.sleep(.2)
            original_poll(seconds)
        producer.poll = poll
        with patch("confluent_kafka.Producer", return_value=producer), patch.object(
                simulator.time, "monotonic", clock.monotonic), patch.object(simulator.time, "sleep", clock.sleep):
            simulator.run_demo(speed=SPEED, start=simulator.DEFAULT_START, quiet=True, skip_missed_ticks=False)
        self.assertEqual([(r[1], r[3]) for r in producer.records], list(simulator.demo_events()))
        self.assertAlmostEqual(clock.now, 1000 + 360 / SPEED, places=6)
        self.assertEqual(len(producer.records), 1452)

    def test_latency_is_real_clock_not_trigger_event_time(self):
        payload = {"alert_emitted_at": "2026-10-17T00:00:00.100Z",
                   "gateway_received_at": "2026-10-17T00:00:00.150Z",
                   "trigger_time": "2026-10-17T00:05:13Z"}
        result = transport_timings(payload, "2026-10-17T00:00:00.175Z")
        self.assertEqual((result["flink_to_gateway_ms"], result["flink_to_websocket_ms"]), (50, 75))

    def test_hard_latency_limit_and_clock_skew_fail(self):
        payload = {"alert_emitted_at": "2026-10-17T00:00:00Z",
                   "gateway_received_at": "2026-10-17T00:00:00.050Z"}
        for received in ("2026-10-17T00:00:02Z", "2026-10-16T23:59:59Z"):
            with self.subTest(received=received), self.assertRaises(AssertionError):
                transport_timings(payload, received)

    def test_microsecond_host_container_difference_preserves_measured_deltas(self):
        payload = {"alert_emitted_at": "2026-10-17T00:00:00Z",
                   "gateway_received_at": "2026-10-17T00:00:00.008686Z"}
        result = transport_timings(payload, "2026-10-17T00:00:00.008644Z")
        self.assertEqual(result["flink_to_gateway_ms"], 8.686)
        self.assertEqual(result["flink_to_websocket_ms"], 8.644)

    def test_fast_orchestrator_uses_verifier_after_preparation(self):
        instance = orchestrate.Orchestrator(orchestrate.arguments(["--fast-verify"]))
        instance.prepare = lambda: instance.report.update(ready=True)
        with patch.object(orchestrate, "demo_lock", contextlib.nullcontext), patch(
                "fast_verify.verify", return_value={"result": "PASS"}) as verify, patch(
                "db_verify.verify_database", return_value={"result": "PASS"}), patch.object(
                orchestrate, "run_demo") as normal, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(instance.run(), 0)
        verify.assert_called_once()
        normal.assert_not_called()
        self.assertEqual(instance.report["outcome"], "verified")


class DocumentationBoundaryTests(unittest.TestCase):
    def test_root_readme_is_current_team_guide(self):
        text = (PROJECT / "README.md").read_text(encoding="utf-8-sig")
        for obsolete in ("TICKET-008", "T001", "MANUAL VERIFICATION PENDING", "tests passed", "next ticket"):
            self.assertNotIn(obsolete.lower(), text.lower())
        self.assertIn("-FastVerify", text)
        self.assertIn("TimescaleDB", text)
        self.assertIn("not original MIMIC", text)

    def test_root_flow_explains_seven_topics_and_implemented_db(self):
        text = (PROJECT / "KAFKA_FLINK_FLOW.md").read_text(encoding="utf-8")
        for topic in CORE_TOPICS:
            self.assertIn(f"`{topic}` |", text)
        for required in ("Storage Writer", "TimescaleDB", "icu-timescale-writer-v2",
                         "mapped_stay_id", "WebSocket", "300", "90", "sample variance"):
            self.assertIn(required, text)
        self.assertFalse((PROJECT / "docs/KAFKA_FLINK_FLOW.md").exists())

    def test_private_history_is_ignored_and_never_tracked(self):
        target = ".local/PROJECT_PROGRESS_PRIVATE.md"
        # The ignored artifact exists on the development machine, but a fresh
        # clone need not contain another developer's private progress history.
        ignored = subprocess.run(["git", "check-ignore", "-v", target], cwd=PROJECT,
                                 text=True, capture_output=True, check=True)
        self.assertIn(".local/", ignored.stdout)
        tracked = subprocess.run(["git", "ls-files", "--", ".local"], cwd=PROJECT,
                                 text=True, capture_output=True, check=True)
        self.assertEqual(tracked.stdout, "")

    def test_no_present_tracked_completion_reports_or_latency_evidence(self):
        tracked = subprocess.run(["git", "ls-files"], cwd=PROJECT, text=True,
                                 capture_output=True, check=True).stdout.splitlines()
        reports = [name for name in tracked if (PROJECT / name).is_file() and any(
            part in name.lower() for part in ("completion_report", "project_progress_private", "latency_report"))]
        self.assertEqual(reports, [])


if __name__ == "__main__":
    unittest.main()
