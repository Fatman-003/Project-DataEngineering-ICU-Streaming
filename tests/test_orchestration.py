"""Lifecycle isolation and real-time scheduling with in-memory Kafka and fake clocks."""
import contextlib
import copy
from concurrent.futures import Future
import io
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "scripts/demo"))
sys.path.insert(0, str(PROJECT / "jobs/flink"))
import orchestrate as lifecycle
import run_icu_demo as simulator
from contracts import CORE_TOPICS, TOPICS, SIGNALS, CHART_FIELDS, LAB_FIELDS
from kafka_topics import reset_demo_topics, DEMO_GROUPS
from hypotension import advance_episode
from event_time import event_timestamp_ms
from runtime_config import WindowAlertConfig
from confluent_kafka import KafkaError, KafkaException


class Clock:
    def __init__(self):
        self.now = 1000.0

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.now += max(0, seconds)


def future(value=None, error=None):
    result = Future()
    result.set_exception(error) if error else result.set_result(value)
    return result


def metadata():
    return SimpleNamespace(error=None, partitions={0: SimpleNamespace(replicas=[1])})


class MemoryAdmin:
    def __init__(self):
        self.topics = {name: metadata() for name in (*CORE_TOPICS, "test-events", "__consumer_offsets")}
        self.deleted, self.created, self.groups = [], [], []
        self.keep_deleted = False
        self.group_errors = []

    def list_topics(self, timeout):
        return SimpleNamespace(topics=dict(self.topics))

    def delete_topics(self, topics, operation_timeout):
        self.deleted.extend(topics)
        if not self.keep_deleted:
            for topic in topics:
                del self.topics[topic]
        return {topic: future() for topic in topics}

    def create_topics(self, topics):
        for topic in topics:
            self.created.append(topic)
            self.topics[topic.topic] = metadata()
        return {topic.topic: future() for topic in topics}

    def delete_consumer_groups(self, groups):
        self.groups.extend(groups)
        error = self.group_errors.pop(0) if self.group_errors else None
        return {group: future(error=error) for group in groups}


def bounds(topics):
    return {topic: {"0": [0, 9] if topic == "test-events" else [0, 0]} for topic in topics}


class ResetTests(unittest.TestCase):
    def test_exact_scope_empty_recreation_and_unrelated_preservation(self):
        admin = MemoryAdmin()
        report = reset_demo_topics(admin=admin, bounds_reader=bounds)
        self.assertEqual(set(admin.deleted), set(CORE_TOPICS))
        self.assertEqual({topic.topic for topic in admin.created}, set(CORE_TOPICS))
        self.assertTrue(all(topic.num_partitions == 1 and topic.replication_factor == 1 for topic in admin.created))
        self.assertEqual(admin.groups, list(DEMO_GROUPS))
        self.assertEqual(report["topics"], {topic: {"0": [0, 0]} for topic in CORE_TOPICS})
        self.assertEqual(report["unrelated_before"], {"test-events": {"0": [0, 9]}})
        self.assertEqual(report["unrelated_before"], report["unrelated_after"])
        self.assertIn("__consumer_offsets", admin.topics)

    def test_reset_twice_has_the_same_safe_scope(self):
        admin = MemoryAdmin()
        for _ in range(2):
            reset_demo_topics(admin=admin, bounds_reader=bounds)
        self.assertEqual(len(admin.deleted), 12)
        self.assertNotIn("test-events", admin.deleted)
        self.assertEqual(len(admin.topics), 8)

    def test_topic_creation_waits_for_broker_metadata_visibility(self):
        admin, clock = MemoryAdmin(), Clock()
        original = admin.list_topics
        missed = []
        def stale_once(timeout):
            if admin.created and not missed:
                missed.append(True)
                return SimpleNamespace(topics={"test-events": metadata(), "__consumer_offsets": metadata()})
            return original(timeout)
        admin.list_topics = stale_once
        reset_demo_topics(admin=admin, bounds_reader=bounds, monotonic=clock.monotonic, sleep=clock.sleep)
        self.assertEqual(missed, [True])
        self.assertGreater(clock.now, 1000)

    def test_waits_for_topic_deletion_before_creation(self):
        admin, clock = MemoryAdmin(), Clock()
        admin.keep_deleted = True
        def complete_deletion(delay):
            clock.sleep(delay)
            for topic in CORE_TOPICS:
                admin.topics.pop(topic, None)
        reset_demo_topics(admin=admin, bounds_reader=bounds,
                          monotonic=clock.monotonic, sleep=complete_deletion)
        self.assertGreater(clock.now, 1000)

    def test_topic_deletion_timeout_aborts_before_recreation(self):
        admin, clock = MemoryAdmin(), Clock()
        admin.keep_deleted = True
        with self.assertRaisesRegex(RuntimeError, "disappear"):
            reset_demo_topics(admin=admin, bounds_reader=bounds, monotonic=clock.monotonic,
                              sleep=clock.sleep, timeout=1)
        self.assertEqual(admin.created, [])

    def test_nonempty_recreated_topic_is_refused(self):
        admin = MemoryAdmin()
        def contaminated(topics):
            return {name: {"0": [0, 1]} for name in topics}
        with self.assertRaisesRegex(RuntimeError, "another producer"):
            reset_demo_topics(admin=admin, bounds_reader=contaminated)

    def test_unexpected_v2_topic_refused_without_deletion(self):
        admin = MemoryAdmin()
        admin.topics["icu.v2.extra"] = metadata()
        with self.assertRaisesRegex(RuntimeError, "partial reset"):
            reset_demo_topics(admin=admin, bounds_reader=bounds)
        self.assertEqual(admin.deleted, [])

    def test_busy_group_retries_and_missing_group_is_allowed(self):
        admin, clock = MemoryAdmin(), Clock()
        admin.group_errors = [KafkaException(KafkaError(KafkaError.NON_EMPTY_GROUP)),
                              KafkaException(KafkaError(KafkaError.GROUP_ID_NOT_FOUND))]
        reset_demo_topics(admin=admin, bounds_reader=bounds, monotonic=clock.monotonic, sleep=clock.sleep)
        self.assertEqual(set(admin.groups), set(DEMO_GROUPS))
        self.assertGreater(clock.now, 1000)


def container(name="icu-flink-jobmanager", running=True):
    service, image = lifecycle.SERVICES[name]
    return {"Name": "/" + name, "Id": name, "State": {"Running": running},
            "Config": {"Image": image, "Labels": {
                "com.docker.compose.project": "icu-flink", "com.docker.compose.service": service,
                "com.docker.compose.project.config_files": str(PROJECT / "docker/flink/compose.yaml")}},
            "NetworkSettings": {"Ports": {"8081/tcp": [{"HostPort": "8081", "HostIp": "127.0.0.1"}]}}}


class OwnershipTests(unittest.TestCase):
    def test_exact_project_service_ownership(self):
        for name in lifecycle.SERVICES:
            self.assertTrue(lifecycle.project_container(container(name)))
        foreign = container()
        foreign["Config"]["Labels"]["com.docker.compose.project.config_files"] = r"D:\other\compose.yaml"
        self.assertFalse(lifecycle.project_container(foreign))
        foreign = container()
        foreign["Config"]["Image"] = "somebody/other:latest"
        self.assertFalse(lifecycle.project_container(foreign))

    def test_broker_requires_its_known_image(self):
        self.assertTrue(lifecycle.project_container({"Name": "/icu-kafka", "Config": {"Image": "apache/kafka:4.2.2"}}))
        self.assertFalse(lifecycle.project_container({"Name": "/icu-kafka", "Config": {"Image": "other"}}))

    def test_known_docker_proxy_with_expected_mapping_is_safe(self):
        process = dict(Port=8081, Pid=123, Name="com.docker.backend.exe",
                       Path=r"D:\Docker\DockerDesktop\resources\com.docker.backend.exe")
        self.assertEqual(lifecycle.check_ports([container()], [process], r"D:\Docker\DockerDesktop\resources\bin\docker.exe"), set())

    def test_unknown_host_conflict_refused_with_pid_and_name(self):
        for name in ("java.exe", None):
            with self.subTest(name=name), self.assertRaisesRegex(RuntimeError, r"8082.*PID 123"):
                lifecycle.check_ports([], [dict(Port=8082, Pid=123, Name=name)], r"D:\Docker\DockerDesktop\resources\bin\docker.exe")

    def test_unknown_container_mapping_refused_even_on_docker_proxy(self):
        foreign = container()
        foreign["Name"] = "/other-service"
        with self.assertRaisesRegex(RuntimeError, "non-project container"):
            lifecycle.check_ports([foreign], [dict(Port=8081, Pid=123, Name="com.docker.backend.exe")],
                                  r"D:\Docker\DockerDesktop\resources\bin\docker.exe")

    def test_same_port_unknown_address_owner_is_not_hidden_by_expected_container(self):
        with self.assertRaisesRegex(RuntimeError, "non-project process"):
            lifecycle.check_ports([container()], [dict(Port=8081, Pid=456, Name="java.exe", Address="::1")],
                                  r"D:\Docker\DockerDesktop\resources\bin\docker.exe")

    def test_host_process_needs_an_exact_project_script_argument(self):
        script = str(PROJECT / "services/realtime_gateway/gateway.py")
        self.assertTrue(lifecycle.project_process(dict(Name="python.exe", Arguments=["python.exe", script])))
        self.assertFalse(lifecycle.project_process(dict(Name="python.exe", Arguments=["python.exe", "-c", "print('" + script + "')"])))
        self.assertFalse(lifecycle.project_process(dict(Name="python.exe", Arguments=["python.exe", "services/realtime_gateway/gateway.py"])))
        self.assertTrue(lifecycle.project_process(dict(Name="python.exe", Arguments=["python.exe", "-u", "-B", script])))
        self.assertFalse(lifecycle.project_process(dict(Name="python.exe", Arguments=["python.exe", "other-app.py", "--input", script])))
        self.assertFalse(lifecycle.project_process(dict(Name="python.exe", Arguments=["python.exe", "-m", "http.server", script])))

    def test_owned_host_gateway_port_can_be_reset(self):
        process = dict(Port=8083, Pid=456, Name="python.exe",
                       Arguments=["python.exe", str(PROJECT / "services/realtime_gateway/gateway.py")])
        self.assertEqual(lifecycle.check_ports([], [process], r"D:\Docker\DockerDesktop\resources\bin\docker.exe"), {456})

    def test_only_exact_active_v2_jobs_are_selected(self):
        jobs = [dict(jid="a", name=lifecycle.V2_JOB_NAME, state="RUNNING"),
                dict(jid="b", name=lifecycle.V2_JOB_NAME, state="CANCELED"),
                dict(jid="c", name="Unrelated job", state="RUNNING"),
                dict(jid="d", name="ICU old V1", state="RUNNING")]
        self.assertEqual([j["jid"] for j in lifecycle.project_jobs(jobs)], ["a"])

    def test_desktop_discovery_uses_cli_install_and_standard_locations(self):
        paths = lifecycle.desktop_candidates(r"D:\Docker\DockerDesktop\resources\bin\docker.exe", {"ProgramFiles": r"C:\Program Files"})
        self.assertIn(Path(r"D:\Docker\DockerDesktop\Docker Desktop.exe"), paths)
        self.assertIn(Path(r"C:\Program Files\Docker\Docker\Docker Desktop.exe"), paths)
        paths = lifecycle.desktop_candidates(r"C:\docker.exe", {"ProgramFiles": r"C:\Program Files"})
        self.assertIn(Path(r"C:\Program Files\Docker\Docker\Docker Desktop.exe"), paths)

    def test_mutex_prevents_duplicate_orchestrators(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(lifecycle, "PROJECT", Path(directory)):
            with lifecycle.demo_lock():
                with self.assertRaisesRegex(RuntimeError, "already running"):
                    with lifecycle.demo_lock():
                        self.fail("Second orchestrator acquired mutex")
            with lifecycle.demo_lock():
                pass


class LifecycleTests(unittest.TestCase):
    def test_defaults_and_ps_wrapper(self):
        args = lifecycle.arguments([])
        self.assertEqual((args.duration_minutes, args.interval_seconds, args.loop), (6, 1, False))
        text = (PROJECT / "run_icu_demo.ps1").read_text()
        self.assertIn("$DurationMinutes = 6", text)
        self.assertIn("$IntervalSeconds = 1", text)
        self.assertIn("scripts\\demo\\orchestrate.py", text)

    def test_simulator_not_started_after_failed_reset(self):
        instance = lifecycle.Orchestrator(lifecycle.arguments([]))
        instance.prepare = Mock(side_effect=RuntimeError("topic reset failed"))
        with patch.object(lifecycle, "demo_lock", contextlib.nullcontext), patch.object(lifecycle, "run_demo") as producer, contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(instance.run(), 1)
        producer.assert_not_called()

    def test_unknown_port_aborts_before_any_reset_or_project_stop(self):
        instance = lifecycle.Orchestrator(lifecycle.arguments([]))
        instance.prerequisites, instance.ensure_docker = Mock(), Mock()
        broker = {"Name": "/icu-kafka", "Id": "preserved", "Config": {"Image": "apache/kafka:4.2.2"},
                  "State": {"Running": True}, "NetworkSettings": {"Ports": {}}}
        instance.containers = Mock(return_value=[broker])
        instance.listeners = Mock(return_value=[dict(Port=8082, Pid=123, Name="java.exe")])
        instance.command, instance.stop_owned_producers = Mock(), Mock()
        with patch.object(lifecycle, "demo_lock", contextlib.nullcontext), patch.object(lifecycle, "reset_demo_topics") as reset, patch.object(lifecycle, "run_demo") as producer, contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(instance.run(), 1)
        instance.command.assert_not_called()
        instance.stop_owned_producers.assert_not_called()
        reset.assert_not_called()
        producer.assert_not_called()

    def test_foreign_flink_job_aborts_before_service_changes(self):
        instance = lifecycle.Orchestrator(lifecycle.arguments([]))
        broker = {"Name": "/icu-kafka", "Id": "preserved", "Config": {"Image": "apache/kafka:4.2.2"}}
        instance.containers = Mock(return_value=[broker, container()])
        instance.listeners = Mock(return_value=[])
        instance.reachable = Mock(return_value=True)
        instance.http = Mock(return_value={"jobs": [dict(name="Unrelated job", state="RUNNING")]})
        instance.command = Mock()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "Unrelated Flink jobs"):
            instance.inspect_ownership()
        instance.command.assert_not_called()

    def test_duplicate_job_aborts_before_submission(self):
        instance = lifecycle.Orchestrator(lifecycle.arguments([]))
        instance.http = Mock(return_value={"jobs": [dict(name=lifecycle.V2_JOB_NAME, state="RUNNING")]})
        instance.compose_command = Mock()
        with contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "duplicate"):
            instance.submit_job()
        instance.compose_command.assert_not_called()

    def test_no_matching_job_does_not_cancel_foreign_jobs(self):
        instance = lifecycle.Orchestrator(lifecycle.arguments([]))
        instance.compose_command = Mock()
        with contextlib.redirect_stdout(io.StringIO()):
            instance.cancel_jobs([dict(name="unrelated", state="RUNNING", jid="1" * 32)])
        instance.compose_command.assert_not_called()

    def test_fresh_snapshot_requires_four_empty_patients(self):
        snapshot = {p["demo_id"]: dict(p, vitals={}, summaries={}, alerts=[]) for p in simulator.PATIENTS}
        self.assertTrue(lifecycle.fresh_snapshot(snapshot))
        snapshot["P02"]["alerts"].append({"alert_id": "stale"})
        self.assertFalse(lifecycle.fresh_snapshot(snapshot))

    def test_docker_already_ready_does_not_launch_desktop(self):
        instance = lifecycle.Orchestrator(lifecycle.arguments([]))
        instance.command = Mock(return_value=SimpleNamespace(returncode=0))
        with patch.object(subprocess, "Popen") as launch, contextlib.redirect_stdout(io.StringIO()):
            instance.ensure_docker()
        launch.assert_not_called()

    def test_stopped_docker_is_started_hidden_then_polled(self):
        instance = lifecycle.Orchestrator(lifecycle.arguments([]))
        instance.command = Mock(side_effect=[SimpleNamespace(returncode=1), SimpleNamespace(returncode=0), SimpleNamespace(returncode=0)])
        with tempfile.TemporaryDirectory() as directory:
            desktop = Path(directory) / "Docker Desktop.exe"
            desktop.touch()
            with patch.object(lifecycle, "desktop_candidates", return_value=[desktop]), patch.object(subprocess, "Popen") as launch, contextlib.redirect_stdout(io.StringIO()):
                instance.ensure_docker()
            self.assertEqual(launch.call_args.args[0], [str(desktop)])
            self.assertEqual(launch.call_args.kwargs["startupinfo"].wShowWindow, 0)

    def test_missing_docker_desktop_fails_clearly(self):
        instance = lifecycle.Orchestrator(lifecycle.arguments([]))
        instance.command = Mock(return_value=SimpleNamespace(returncode=1))
        with patch.object(lifecycle, "desktop_candidates", return_value=[]), contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "not found"):
            instance.ensure_docker()

    def test_docker_readiness_timeout_is_bounded_without_real_sleep(self):
        instance = lifecycle.Orchestrator(lifecycle.arguments([]))
        instance.command = Mock(return_value=SimpleNamespace(returncode=1))
        clock = Clock()
        with tempfile.TemporaryDirectory() as directory:
            desktop = Path(directory) / "Docker Desktop.exe"
            desktop.touch()
            with patch.object(lifecycle, "desktop_candidates", return_value=[desktop]), patch.object(subprocess, "Popen"), patch.object(lifecycle.time, "monotonic", clock.monotonic), patch.object(lifecycle.time, "sleep", clock.sleep), contextlib.redirect_stdout(io.StringIO()), self.assertRaisesRegex(RuntimeError, "four minutes"):
                instance.ensure_docker()
        self.assertEqual(clock.now, 1240)


class FakeProducer:
    def __init__(self, clock):
        self.clock, self.records, self.flushed = clock, [], False
        self.fail = False

    def produce(self, topic, key, value, on_delivery):
        self.records.append((self.clock.monotonic(), topic, key, json.loads(value)))
        on_delivery("test error" if self.fail else None, None)

    def poll(self, seconds):
        pass

    def flush(self, seconds):
        self.flushed = True
        return 0


class SimulatorTests(unittest.TestCase):
    def test_fractional_minutes_from_powershell_do_not_add_an_eleventh_tick(self):
        duration=float("0.166666666666667")
        records=list(simulator.demo_events(duration))
        self.assertEqual(sum(t==TOPICS["chartevents"] for t,e in records),240)
        clock=Clock();producer=FakeProducer(clock)
        with patch("confluent_kafka.Producer",return_value=producer), patch.object(
                simulator.time,"monotonic",clock.monotonic),patch.object(simulator.time,"sleep",clock.sleep):
            simulator.run_demo(duration_minutes=duration,start=simulator.DEFAULT_START,quiet=True)
        self.assertEqual(len(producer.records),244)

    def test_defaults_and_all_signals_per_patient_per_second(self):
        args = simulator.arguments([])
        self.assertEqual((args.duration_minutes, args.interval_seconds, args.speed, args.loop), (6, 1, 1, False))
        for second, signal in enumerate(SIGNALS):
            records = [value for topic, value in simulator.events_at(second) if topic == TOPICS["chartevents"]]
            self.assertEqual(len(records), 24)
            self.assertEqual({e["signal_name"] for e in records}, set(SIGNALS))

    def test_all_six_minutes_keep_four_real_identities_and_contracts(self):
        identities = {p["stay_id"] for p in simulator.PATIENTS}
        for topic, value in simulator.demo_events():
            self.assertIn(value.get("stay_id", value.get("mapped_stay_id")), identities)
            self.assertEqual(set(value), CHART_FIELDS if topic == TOPICS["chartevents"] else LAB_FIELDS)

    def test_sparse_edges_and_labs(self):
        raw = list(simulator.demo_events())
        charts = [e for topic, e in raw if topic == TOPICS["chartevents"]]
        self.assertEqual(len(charts), 24 * 360 + 4)
        for suffix, at in (("-ooo", 44), ("-invalid", 210), ("-late", 130)):
            selected = [e for e in charts if e["event_id"].endswith(suffix)]
            self.assertEqual(len(selected), 1)
            self.assertEqual(event_timestamp_ms(selected[0]) - event_timestamp_ms(simulator.chart_event(simulator.PATIENTS[3], "HR", 0, simulator.DEFAULT_START)), at * 1000)
        labs = [e for topic, e in raw if topic == TOPICS["labevents"]]
        self.assertEqual(len(labs), 8)
        self.assertEqual(len({e["event_id"] for e in charts}), len(charts) - 1)

    def test_p02_threshold_fits_six_minutes_and_p03_never_alerts(self):
        states, outputs = {}, []
        for topic, value in simulator.demo_events():
            if topic != TOPICS["chartevents"] or value["signal_name"] != "MAP":
                continue
            stay = value["stay_id"]
            states[stay], emitted = advance_episode(states.get(stay), {"event": value}, WindowAlertConfig())
            outputs.extend(emitted)
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0]["stay_id"], simulator.PATIENTS[1]["stay_id"])
        first = event_timestamp_ms(simulator.chart_event(simulator.PATIENTS[1], "MAP", 0, simulator.DEFAULT_START))
        from hypotension import alert_identity
        self.assertEqual(outputs[0]["alert_id"],alert_identity(simulator.PATIENTS[1]["stay_id"],first+12000))
        self.assertEqual(event_timestamp_ms({"event_time": outputs[0]["alert_time"]}) - first, 42000)

    def test_monotonic_pacing_has_no_accumulated_sleep_drift(self):
        clock = Clock()
        producer = FakeProducer(clock)
        with patch("confluent_kafka.Producer", return_value=producer), patch.object(simulator.time, "monotonic", clock.monotonic), patch.object(simulator.time, "sleep", clock.sleep):
            simulator.run_demo(duration_minutes=0.1, start=simulator.DEFAULT_START, quiet=True)
        charts = [r for r in producer.records if r[1] == TOPICS["chartevents"]]
        self.assertEqual([r[0] for r in charts], [1000 + second for second in range(6) for _ in range(24)])
        self.assertEqual(clock.now, 1006)
        self.assertTrue(producer.flushed)

    def test_fractional_interval_rotates_signals_and_uses_event_clock(self):
        clock, producer = Clock(), None
        producer = FakeProducer(clock)
        with patch("confluent_kafka.Producer", return_value=producer), patch.object(simulator.time, "monotonic", clock.monotonic), patch.object(simulator.time, "sleep", clock.sleep):
            simulator.run_demo(duration_minutes=0.05, interval_seconds=0.5, start=simulator.DEFAULT_START, quiet=True)
        charts = [r for r in producer.records if r[1] == TOPICS["chartevents"]]
        self.assertEqual([charts[i][3]["signal_name"] for i in range(6)], list(SIGNALS))
        self.assertEqual(event_timestamp_ms(charts[24][3]) - event_timestamp_ms(charts[0][3]), 500)

    def test_delivery_failure_still_flushes_and_stops(self):
        clock = Clock()
        producer = FakeProducer(clock)
        producer.fail = True
        with patch("confluent_kafka.Producer", return_value=producer), patch.object(simulator.time, "monotonic", clock.monotonic), patch.object(simulator.time, "sleep", clock.sleep):
            with self.assertRaisesRegex(RuntimeError, "delivery failed"):
                simulator.run_demo(start=simulator.DEFAULT_START, quiet=True)
        self.assertTrue(producer.flushed)

    def test_ctrl_c_flushes_and_propagates_cleanly(self):
        clock = Clock()
        producer = FakeProducer(clock)
        producer.poll = Mock(side_effect=KeyboardInterrupt)
        with patch("confluent_kafka.Producer", return_value=producer), patch.object(simulator.time, "monotonic", clock.monotonic), patch.object(simulator.time, "sleep", clock.sleep):
            with self.assertRaises(KeyboardInterrupt):
                simulator.run_demo(start=simulator.DEFAULT_START, quiet=True)
        self.assertTrue(producer.flushed)

    def test_loop_never_reuses_event_ids_between_cycles(self):
        first = {e["event_id"] for _, e in simulator.events_at(1)}
        second = {e["event_id"] for _, e in simulator.events_at(361)}
        self.assertFalse(first & second)

    def test_invalid_cadence_rejected_before_production(self):
        for value in (0, -1, math.inf, math.nan):
            with self.subTest(value=value), self.assertRaises(ValueError):
                simulator.run_demo(interval_seconds=value, start=simulator.DEFAULT_START, quiet=True)


if __name__ == "__main__":
    unittest.main()
