"""Live V2 checks, including two short executions of the canonical reset command."""
import argparse
import asyncio
import json
import math
import statistics
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
import aiohttp
from confluent_kafka import Consumer, TopicPartition

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "jobs/flink"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from contracts import TOPICS, BUSINESS_TOPICS, CORE_TOPICS, DLQ_TOPIC, stay_key, parse_event
from registry import validate_source_identities, load_patients, IDENTITY_FIELDS
from validation import parse_timestamp
from run_icu_demo import run_demo, next_demo_start, chart_event
from kafka_topics import ensure_topics, remove_verified_v1


def orchestration_smoke(check_port_conflict=False):
    """Exercise real startup/reset twice without a six-minute automated sleep."""
    import urllib.request
    from kafka_topics import topic_offsets
    checks = Checks()

    def http(path):
        with urllib.request.urlopen(path, timeout=5) as response:
            return json.load(response)

    def broker_id():
        return docker("inspect", "icu-kafka", "--format", "{{.Id}}").stdout.strip()

    preserved_id = broker_id()
    with tempfile.TemporaryDirectory(prefix="icu-orchestration-") as directory:
        previous = None
        for run in (1, 2):
            report_path = Path(directory) / f"run-{run}.json"
            result = subprocess.run(["powershell.exe", "-NoProfile", "-File",
                                     str(PROJECT / "run_icu_demo.ps1"), "-DurationMinutes", "0.2",
                                     "-StateReport", str(report_path)], cwd=PROJECT, text=True,
                                    capture_output=True, timeout=600)
            if result.returncode:
                raise RuntimeError(f"One-command run {run} failed:\n{result.stdout}\n{result.stderr}")
            report = json.loads(report_path.read_text(encoding="utf-8"))
            checks.require(report["ready"] and report["outcome"] == "completed", f"run {run}: one command establishes and runs a fresh environment")
            checks.require(report["broker_id_before"] == report["broker_id_after"] == preserved_id == broker_id(), f"run {run}: existing broker container preserved")
            checks.require(report["reset"]["topics"] == {name: {"0": [0, 0]} for name in CORE_TOPICS}, f"run {run}: exact seven topics start empty")
            checks.require(report["reset"]["unrelated_before"] == report["reset"]["unrelated_after"], f"run {run}: unrelated topic bounds unchanged")
            checks.require(set(report["initial_snapshot"]) == {"P01", "P02", "P03", "P04"}
                           and all(not patient[field] for patient in report["initial_snapshot"].values() for field in ("vitals", "summaries", "alerts")), f"run {run}: Gateway starts with four empty patients")
            jobs = http("http://localhost:8081/jobs/overview")["jobs"]
            active = [j for j in jobs if j["state"] == "RUNNING"]
            checks.require(len(active) == 1 and active[0]["jid"] == report["job_id"], f"run {run}: exactly one fresh V2 job RUNNING")
            checkpoints = http(f"http://localhost:8081/jobs/{report['job_id']}/checkpoints")
            checks.require(checkpoints["counts"]["completed"] > 0, f"run {run}: checkpointing active")
            checks.require(report["producer_started"] and report["elapsed_seconds"] >= 12, f"run {run}: producer uses real wall-clock cadence")
            offsets = topic_offsets(CORE_TOPICS)
            checks.require(offsets[TOPICS["chartevents"]]["0"][1] == 48 and offsets[TOPICS["labevents"]]["0"][1] == 4, f"run {run}: only this run's 48 rotating vitals and four sparse labs exist")
            checks.require(http("http://localhost:8083/api/health")["kafka_connected"], f"run {run}: Gateway healthy")
            checks.require(any(c["name"] == "ICU-LOCAL" and c["status"] == "online" for c in http("http://localhost:8082/api/clusters")), f"run {run}: Kafka UI reachable and connected")
            if previous:
                checks.require(previous["job_id"] != report["job_id"] and previous["job_id"] in report["canceled_job_ids"], "second run cancels the first job and submits a different fresh job")
                snapshot = http("http://localhost:8083/api/snapshot")
                start = parse_timestamp(report["event_start"])
                checks.require(all(parse_timestamp(vital["event_time"]) >= start for patient in snapshot.values() for vital in patient["vitals"].values()), "second run's Gateway contains no first-run vital timestamps")
            previous = report
        if check_port_conflict:
            # Bind an unrelated host process while the project UI is stopped.
            # The orchestrator must abort before canceling jobs or deleting topics.
            original_bounds = topic_offsets(CORE_TOPICS)
            docker("stop", "icu-kafka-ui")
            server = None
            try:
                server = subprocess.Popen([getattr(sys, "_base_executable", sys.executable), "-m", "http.server", "8082", "--bind", "127.0.0.1"],
                                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                          creationflags=subprocess.CREATE_NO_WINDOW)
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    try:
                        with urllib.request.urlopen("http://localhost:8082", timeout=1):
                            break
                    except OSError:
                        time.sleep(0.1)
                rejected = subprocess.run(["powershell.exe", "-NoProfile", "-File",
                                           str(PROJECT / "run_icu_demo.ps1"), "-DurationMinutes", "0.1"],
                                          cwd=PROJECT, text=True, capture_output=True, timeout=120)
                checks.require(rejected.returncode != 0 and "Port 8082" in rejected.stderr
                               and f"PID {server.pid}" in rejected.stderr, "unknown occupied port is refused with port/PID/process details")
                checks.require(server.poll() is None, "unknown host process is not killed")
                checks.require(original_bounds == topic_offsets(CORE_TOPICS), "port-conflict refusal leaves Kafka topic data unchanged")
                checks.require(any(j["jid"] == previous["job_id"] and j["state"] == "RUNNING" for j in http("http://localhost:8081/jobs/overview")["jobs"]), "port-conflict refusal leaves existing Flink job running")
            finally:
                if server is not None:
                    server.terminate()
                    server.wait(timeout=10)
                docker("start", "icu-kafka-ui")
    print(f"{checks.count} orchestration live checks passed. IMPLEMENTED - MANUAL VERIFICATION PENDING.")


class Checks:
    def __init__(self):
        self.count = 0

    def require(self, condition, description):
        if not condition:
            raise AssertionError(description)
        self.count += 1
        print(f"OK {self.count:02}: {description}", flush=True)


def capture_consumer():
    consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "icu-v2-smoke-reader",
                         "enable.auto.commit": False})
    positions = []
    for topic in CORE_TOPICS:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=10)
        positions.append(TopicPartition(topic, 0, high))
    consumer.assign(positions)
    return consumer


async def capture_run(start, speed, minutes, session, websocket=True):
    consumer = capture_consumer()
    captured = defaultdict(list)
    updates = []
    ws = await session.ws_connect("http://localhost:8083/ws") if websocket else None

    async def read_ws():
        async for message in ws:
            if message.type == aiohttp.WSMsgType.TEXT:
                updates.append(json.loads(message.data))

    listener = asyncio.create_task(read_ws()) if ws else None
    producer = asyncio.create_task(asyncio.to_thread(run_demo, speed=speed, duration_minutes=minutes,
                                                    start=start, quiet=True))
    deadline = time.monotonic() + minutes * 60 / speed + 150
    settled = None
    try:
        while time.monotonic() < deadline:
            messages = await asyncio.to_thread(consumer.consume, 1000, 0.1)
            for message in messages:
                if message.error():
                    raise RuntimeError(message.error())
                event = parse_event(message.value().decode())
                captured[message.topic()].append((event, message.key()))
            if producer.done():
                await producer
                # Wait until the last ordinary second has reached both independent paths.
                chart = [e for e, _ in captured[BUSINESS_TOPICS["chartevents"]]]
                expected = start + timedelta(seconds=math.ceil(minutes * 60) - 1)
                complete = chart and max(parse_timestamp(e["event_time"]) for e in chart) >= expected
                complete = complete and bool(captured[BUSINESS_TOPICS["alerts"]])
                if complete:
                    if settled is None:
                        settled = time.monotonic()
                    if time.monotonic() - settled >= 3:
                        break
            await asyncio.sleep(0)
        if not producer.done():
            await producer
        if settled is None:
            raise AssertionError("Flink did not drain this demo within 150 seconds")
    finally:
        consumer.close()
        if ws:
            await ws.close()
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)
        if not producer.done():
            await producer
    return captured, updates


def verify(captured, updates, start, checks):
    patients = load_patients()
    triples = {tuple(p[f] for f in IDENTITY_FIELDS[:3]) for p in patients}
    charts = [e for e, _ in captured[TOPICS["chartevents"]]]
    labs = [e for e, _ in captured[TOPICS["labevents"]]]
    clean = [e for e, _ in captured[BUSINESS_TOPICS["chartevents"]]]
    clean_labs = [e for e, _ in captured[BUSINESS_TOPICS["labevents"]]]
    aggregates = [e for e, _ in captured[BUSINESS_TOPICS["aggregates"]]]
    alerts = [e for e, _ in captured[BUSINESS_TOPICS["alerts"]]]
    dlq = [e for e, _ in captured[DLQ_TOPIC]]
    checks.require(bool(charts), "RAW chartevents receive data")
    checks.require(bool(labs), "RAW labevents receive data")
    checks.require(bool(clean), "cleaned telemetry exists")
    checks.require(bool(clean_labs), "cleaned labs exist")
    for events, lab in ((charts, False), (labs, True), (clean, False), (clean_labs, True)):
        observed = {(e["subject_id"], e["hadm_id"], e["mapped_stay_id" if lab else "stay_id"]) for e in events}
        checks.require(observed == triples, "stream contains all four real identities and no fifth")
    checks.require(all(e["quality_status"] == "VALID" for e in clean + clean_labs), "normal events pass quality validation")
    for patient in patients:
        first = chart_event(patient, "HR", 0, start)["event_id"]
        checks.require(any(e["event_id"] == first for e in clean), f"{patient['demo_id']} normal event passes")
    checks.require({a["stay_id"] for a in alerts} == {patients[1]["stay_id"]}, "only sustained P02 alerts; short-low P03 does not")
    checks.require(any(parse_timestamp(a["trigger_time"]) - parse_timestamp(a["episode_start"]) == timedelta(minutes=5)
                       for a in alerts), "alert triggers at exactly five observed low minutes")
    for error in ("DUPLICATE_EVENT", "VALUE_ERROR", "LATE_EVENT"):
        checks.require(any(e["error_type"] == error for e in dlq), f"DLQ contains {error}")
    checks.require(any(e["event_id"].endswith("-ooo") and e["ordering_status"] == "OUT_OF_ORDER" for e in clean),
                   "tolerated out-of-order event is accepted")
    checks.require(all(not e["event_id"].endswith(("-invalid", "-late")) for e in clean), "rejected values excluded from cleaned telemetry")
    checks.require(len({e["event_id"] for e in clean}) == len(clean), "managed dedup removes repeated event identity")
    checks.require(bool(aggregates) and all(a["window_minutes"] == 15 for a in aggregates), "15m outputs exist and no other aggregate window exists")
    by_key = defaultdict(list)
    for event in clean:
        by_key[(event["stay_id"], event["signal_name"])].append(event)
    verified = set()
    for aggregate in aggregates:
        left, right = map(parse_timestamp, (aggregate["window_start"], aggregate["window_end"]))
        if left < start or right > start + timedelta(minutes=19):
            continue
        key = aggregate["stay_id"], aggregate["signal_name"]
        values = [e["valuenum"] for e in by_key[key] if left <= parse_timestamp(e["event_time"]) < right]
        if not values:
            continue
        valid = (right - left == timedelta(minutes=15) and aggregate["count"] == len(values)
                 and math.isclose(aggregate["mean"], statistics.mean(values), abs_tol=1e-9)
                 and aggregate["min"] == min(values) and aggregate["max"] == max(values)
                 and math.isclose(aggregate["variance"], statistics.variance(values), abs_tol=1e-9))
        if not valid:
            raise AssertionError(f"15m oracle mismatch: {key}, {aggregate}")
        verified.add(key)
    checks.require(len(verified) == 24, "all four patients/six signals match an independent full 15m statistics oracle")
    checks.require(all(key == stay_key(event).encode() for topic, records in captured.items()
                       for event, key in records if topic != DLQ_TOPIC), "all stay-scoped Kafka keys match numeric stay identity")
    checks.require(all(e["original_topic"] in TOPICS.values() and e["original_partition"] == 0
                       and isinstance(e["original_offset"], int) and e["raw_payload"] for e in dlq), "DLQ preserves human-readable payload and Kafka coordinates")
    types = {event["message_type"] for event in updates}
    for message_type in ("SNAPSHOT", "VITAL_UPDATE", "VITAL_15M", "ALERT"):
        checks.require(message_type in types, f"one WebSocket receives {message_type}")
    checks.require(set(next(e["payload"] for e in updates if e["message_type"] == "SNAPSHOT")) == {"P01", "P02", "P03", "P04"},
                   "Gateway snapshot contains only P01-P04")


async def get_snapshot(session):
    async with session.get("http://localhost:8083/api/snapshot") as response:
        response.raise_for_status()
        return await response.json()


def docker(*args):
    return subprocess.run(["docker", *args], check=True, capture_output=True, text=True, timeout=60)


async def independence(session, checks, speed):
    start = next_demo_start() + timedelta(minutes=2)
    await asyncio.to_thread(docker, "stop", "icu-realtime-gateway", "icu-kafka-ui")
    try:
        captured, _ = await capture_run(start, speed, 8, session, websocket=False)
        checks.require(bool(captured[TOPICS["chartevents"]]), "producer/Kafka continue with gateway and Kafka UI stopped")
        checks.require(bool(captured[BUSINESS_TOPICS["chartevents"]]), "Flink continues while gateway and Kafka UI are stopped")
        checks.require(bool(captured[BUSINESS_TOPICS["alerts"]]), "Flink alerts continue while gateway is stopped")
    finally:
        await asyncio.to_thread(docker, "start", "icu-realtime-gateway", "icu-kafka-ui")
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            snapshot = await get_snapshot(session)
            latest = snapshot["P02"]["vitals"].get("MAP")
            new_alerts = [a for a in snapshot["P02"]["alerts"] if parse_timestamp(a["trigger_time"]) >= start]
            if latest and parse_timestamp(latest["event_time"]) >= start + timedelta(minutes=7) and new_alerts:
                checks.require(True, "Gateway restart rebuilds current vitals and missed alerts from retained Kafka history")
                return
        except (aiohttp.ClientError, KeyError):
            pass
        await asyncio.sleep(1)
    raise AssertionError("Gateway did not rebuild after restart")


async def main(args):
    checks = Checks()
    checks.require(len(validate_source_identities()) == 4, "four frozen identity triples are valid in local MIMIC")
    checks.require(set(ensure_topics()) == set(CORE_TOPICS), "exactly seven correctly configured V2 topics")
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
        before = await get_snapshot(session)
        checks.require(set(before) == {"P01", "P02", "P03", "P04"}, "Gateway starts without any database")
        # Simulated blocked storage consumer: assigned but paused, with no DB/client/writes.
        storage = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "icu-timescale-writer-v2",
                            "enable.auto.commit": False, "auto.offset.reset": "earliest"})
        try:
            storage.subscribe(list(BUSINESS_TOPICS.values()))
            deadline = time.monotonic() + 20
            while not storage.assignment() and time.monotonic() < deadline:
                await asyncio.to_thread(storage.poll, 0.2)
            checks.require(bool(storage.assignment()), "simulated stalled storage uses its separate consumer group")
            storage.pause(storage.assignment())
            start = next_demo_start() + timedelta(minutes=2)
            captured, updates = await capture_run(start, args.speed, 20, session)
            verify(captured, updates, start, checks)
            after = await get_snapshot(session)
            checks.require(before["P01"]["vitals"] != after["P01"]["vitals"], "snapshot updates while storage consumer is paused/unavailable")
            checks.require(any(parse_timestamp(a["trigger_time"]) >= start for a in after["P02"]["alerts"]), "alerts reach Gateway while storage is unavailable")
        finally:
            storage.close()
        if args.check_independence:
            await independence(session, checks, args.speed)
    if args.remove_v1:
        if not args.check_independence:
            raise ValueError("V1 cleanup requires --check-independence")
        print("V1 topics removed: " + ", ".join(remove_verified_v1()))
    print(f"{checks.count} live checks passed. IMPLEMENTED — MANUAL VERIFICATION PENDING.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--speed", type=float, default=5)
    parser.add_argument("--check-independence", action="store_true", help="Stop/restart only gateway and Kafka UI during a second demo")
    parser.add_argument("--remove-v1", action="store_true", help="Delete only eight obsolete V1 topics, after every live check passes")
    parser.add_argument("--orchestration", action="store_true", help="Run the canonical command twice with short real-time simulators")
    parser.add_argument("--check-port-conflict", action="store_true", help="With --orchestration, verify refusal of an unknown host listener")
    args = parser.parse_args()
    if args.orchestration:
        orchestration_smoke(args.check_port_conflict)
    else:
        asyncio.run(main(args))
