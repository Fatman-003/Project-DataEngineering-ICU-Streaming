"""Small labeled TICKET-006 live probes; never writes raw or Reference files.

Run against one RUNNING runtime_pipeline.py job consuming all three RAW topics.
The harness gates ordering probes on the classifier's real Flink watermark.
"""
import argparse
import base64
import hashlib
import json
import subprocess
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "jobs" / "flink"))
sys.path.insert(0, str(PROJECT / "scripts" / "replay"))
from contracts import TOPICS
from runtime_config import RuntimeConfig, DLQ_TOPIC
from config import settings


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--job-id")
    parser.add_argument("--bootstrap-servers", default=settings()["bootstrap"])
    parser.add_argument("--flink-rest", default="http://localhost:8081")
    parser.add_argument("--taskmanager-container", default="icu-flink-taskmanager")
    parser.add_argument("--timeout-seconds", type=int, default=120)
    parser.add_argument("--tolerance-seconds", type=int,
                        default=RuntimeConfig.from_env().max_out_of_orderness_seconds)
    parser.add_argument("--output", type=Path, default=PROJECT / "data/working/runtime")
    return parser.parse_args()


def b64(value):
    return None if value is None else base64.b64encode(value).decode("ascii")


def source_key(topic, partition, offset):
    return topic, partition, offset


def utf8_or_none(raw):
    if raw is None:
        return None
    try:
        return raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError:
        return None


def main():
    from confluent_kafka import Consumer, Producer, TopicPartition

    args = arguments()
    if args.tolerance_seconds < 2 or args.timeout_seconds < 20:
        raise SystemExit("Probes require watermark tolerance >= 2s and timeout >= 20s")
    args.output.mkdir(parents=True, exist_ok=True)
    session = "T006-" + uuid.uuid4().hex
    started = datetime.now(timezone.utc)
    deadline = time.monotonic() + args.timeout_seconds
    report = {"status": "FAIL", "probe_kind": "ENGINEERING_PROBE",
              "replay_session_id": session, "started_at": started.isoformat(),
              "published": [], "checks": {}, "watermark_observation": None}
    clean_by_source, dlq_by_source = {}, {}
    consumer = None

    def require(condition, message):
        if not condition:
            raise AssertionError(message)

    def remaining():
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError("TICKET-006 probe verification exceeded its bounded timeout")
        return left

    def rest(path):
        with urlopen(args.flink_rest.rstrip("/") + path,
                     timeout=min(5, remaining())) as response:
            return json.load(response)

    def job_state():
        details = rest(f"/jobs/{report['job_id']}")
        require(details["state"] == "RUNNING", f"Flink job state is {details['state']}")
        return details

    def collect():
        # Read only bounded recent logs, retaining structured audit JSON fields.
        result = subprocess.run(
            ["docker", "logs", "--since", started.isoformat(), "--tail", "5000",
             args.taskmanager_container], capture_output=True, text=True,
            encoding="utf-8", errors="replace", timeout=min(10, remaining()))
        require(result.returncode == 0, "Could not read TaskManager logs: " + result.stderr[-600:])
        for line in (result.stdout + "\n" + result.stderr).splitlines():
            position = line.find("{")
            if position < 0:
                continue
            try:
                item, _ = json.JSONDecoder().raw_decode(line[position:])
            except ValueError:
                continue
            if not isinstance(item, dict) or item.get("kind") not in {
                    "CLEAN_EVENT", "OUT_OF_ORDER_EVENT"}:
                continue
            if item.get("replay_session_id") == session:
                key = source_key(item["topic"], item["partition"], item["offset"])
                clean_by_source[key] = item
        while True:
            message = consumer.poll(0)
            if message is None:
                break
            require(not message.error(), f"DLQ Kafka consumer error: {message.error()}")
            item = json.loads(message.value().decode("utf-8"))
            key = source_key(item.get("original_topic"), item.get("original_partition"),
                             item.get("original_offset"))
            published = {(p["topic"], p["partition"], p["offset"])
                         for p in report["published"]}
            if key in published:
                dlq_by_source[key] = {
                    "kafka_topic": message.topic(), "kafka_partition": message.partition(),
                    "kafka_offset": message.offset(), "kafka_key_base64": b64(message.key()),
                    "event": item,
                }

    def pause():
        time.sleep(min(0.5, remaining()))

    try:
        jobs = rest("/jobs/overview")["jobs"]
        matches = [j for j in jobs if j["state"] == "RUNNING"
                   and "TICKET-006" in j["name"]
                   and (args.job_id is None or j["jid"] == args.job_id)]
        require(len(matches) == 1, "Expected exactly one selected RUNNING TICKET-006 runtime job")
        report["job_id"] = matches[0]["jid"]
        details = job_state()
        checkpoints_before = rest(f"/jobs/{report['job_id']}/checkpoints")
        require(checkpoints_before["counts"]["completed"] > 0,
                "Wait for the job's first completed checkpoint before publishing probes")
        report["checkpoints_before"] = checkpoints_before["counts"]
        baseline_cp = checkpoints_before["latest"]["completed"]
        baseline_details = rest(f"/jobs/{report['job_id']}/checkpoints/details/{baseline_cp['id']}")
        dedup_vertices = {stream: next(v for v in details["vertices"]
                                     if f"Managed dedup: {stream}" in v["name"])
                          for stream in TOPICS}
        report["checkpoint_id_before"] = baseline_cp["id"]
        report["dedup_state_before"] = {
            stream: dict(vertex_id=vertex["id"],
                         state_size=baseline_details["tasks"][vertex["id"]]["state_size"],
                         checkpointed_size=baseline_details["tasks"][vertex["id"]]["checkpointed_size"])
            for stream, vertex in dedup_vertices.items()}

        for stream in TOPICS:
            require(any(f"RAW metadata: {stream}" in v["name"]
                        and v["status"] == "RUNNING" for v in details["vertices"]),
                    f"Runtime source {stream} is not RUNNING; submit with --stream all")
        vertices = [v for v in details["vertices"]
                    if "Classify event-time: vitals" in v["name"]]
        require(len(vertices) == 1, "Could not identify numeric event-time classifier vertex")
        metric_path = (f"/jobs/{report['job_id']}/vertices/{vertices[0]['id']}"
                       "/subtasks/0/metrics")
        metrics = rest(metric_path)
        watermark_names = [m["id"] for m in metrics
                           if "Classify_event-time__vitals" in m["id"]
                           and m["id"].endswith("currentInputWatermark")]
        require(len(watermark_names) == 1, "Could not identify classifier input-watermark metric")
        watermark_name = watermark_names[0]
        consumer = Consumer({"bootstrap.servers": args.bootstrap_servers,
                             "group.id": session + "-dlq-verify", "enable.auto.commit": False,
                             "auto.offset.reset": "error"})
        _, dlq_start = consumer.get_watermark_offsets(TopicPartition(DLQ_TOPIC, 0), timeout=10)
        consumer.assign([TopicPartition(DLQ_TOPIC, 0, dlq_start)])
        report["dlq_start_offset"] = dlq_start
        producer = Producer({"bootstrap.servers": args.bootstrap_servers,
                             "enable.idempotence": True, "acks": "all",
                             "request.timeout.ms": 10000, "delivery.timeout.ms": 30000})
        base = datetime.now(timezone.utc).replace(year=2160, microsecond=0)
        base_ms = int(base.timestamp() * 1000)
        stay = session + "-STAY"

        def timestamp(seconds=0):
            return (base + timedelta(seconds=seconds)).replace(tzinfo=None).isoformat()

        def event(case, stream="vitals", seconds=0):
            body = {"schema_version": "1.0.0", "event_id": session + "-" + case,
                    "run_id": session, "stay_id": stay, "subject_id": session + "-SUBJECT",
                    "hadm_id": session + "-HADM", "patient_key": session,
                    "event_time": timestamp(seconds), "source_event_time": "2143-09-20T00:00:00",
                    "producer_time": datetime.now(timezone.utc).isoformat(),
                    "dataset_type": "ENGINEERING_PROBE", "test_mode": "TICKET006",
                    "is_virtual": True, "virtual_patient_id": session,
                    "replay_session_id": session}
            if stream == "vitals":
                body.update(signal_name="MAP", value=62, unit="mmHg")
            elif stream == "clinical":
                body.update(event_type="GCS", chart_time=timestamp(seconds), available_time=None,
                            availability_status="UNKNOWN", validation_flags=["ENGINEERING_PROBE"],
                            value=15, value_text="15", unit="score")
            else:
                body.update(waveform_record_id=session + "-RECORD", signal_name="II",
                            start_time=timestamp(seconds), end_time=timestamp(seconds + 1),
                            sampling_rate_hz=4.0, samples=[0.1, None, -0.2, 0.3],
                            sample_count=4, missing_sample_count=1, unit="mV")
            return body

        default_key = object()

        def publish(case, stream, body, expected, key=default_key):
            raw = body if isinstance(body, bytes) or body is None else json.dumps(
                body, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")
            actual_key = stay.encode() if key is default_key else key
            probe = {"case": case, "stream_type": stream, "expected": expected,
                     "topic": TOPICS[stream], "key_base64": b64(actual_key),
                     "payload_base64": b64(raw), "raw_sha256": hashlib.sha256(raw if raw is not None else b"").hexdigest(),
                     "event": body if isinstance(body, dict) else None}
            failures = []

            def delivered(error, message):
                if error:
                    failures.append(str(error))
                else:
                    probe.update(partition=message.partition(), offset=message.offset())

            producer.produce(TOPICS[stream], key=actual_key, value=raw, on_delivery=delivered)
            require(producer.flush(min(10, remaining())) == 0, f"Producer flush timed out: {case}")
            require(not failures, f"Producer failed {case}: {failures}")
            require("offset" in probe, f"Delivery metadata absent: {case}")
            report["published"].append(probe)
            return probe

        anchor = event("VALID_NUMERIC")
        publish("VALID_NUMERIC_MAP_62", "vitals", anchor, "CLEAN_EVENT")
        watermark_target = base_ms - args.tolerance_seconds * 1000 - 1
        while True:
            collect()
            values = rest(metric_path + "?get=" + quote(watermark_name))
            watermark = int(values[0]["value"])
            if watermark >= watermark_target:
                report["watermark_observation"] = {
                    "metric": watermark_name, "vertex_id": vertices[0]["id"],
                    "value_ms": watermark, "target_ms": watermark_target,
                    "anchor_event_time_ms": base_ms, "observed_at": datetime.now(timezone.utc).isoformat()}
                break
            job_state()
            pause()
        require(watermark < base_ms, "Configured watermark no longer tolerates the anchor time")
        publish("MALFORMED_JSON", "vitals",
                ('{"engineering_probe":"TICKET006","replay_session_id":"' + session + '",').encode(),
                "PARSE_ERROR")
        missing = event("MISSING_STAY"); del missing["stay_id"]
        publish("MISSING_STAY", "vitals", missing, "SCHEMA_ERROR")
        negative = event("NEGATIVE_MAP"); negative["value"] = -500
        publish("NEGATIVE_MAP", "vitals", negative, "VALUE_ERROR")
        publish("DUPLICATE_NUMERIC", "vitals", anchor, "DUPLICATE_EVENT")
        clinical = event("CLINICAL_NULL", "clinical")
        publish("CLINICAL_NULL", "clinical", clinical, "CLEAN_EVENT")
        publish("DUPLICATE_CLINICAL", "clinical", clinical, "DUPLICATE_EVENT")
        waveform = event("VALID_WAVEFORM", "waveform")
        publish("VALID_WAVEFORM", "waveform", waveform, "CLEAN_EVENT")
        publish("DUPLICATE_WAVEFORM", "waveform", waveform, "DUPLICATE_EVENT")
        within = min(10, args.tolerance_seconds // 2)
        publish("OUT_OF_ORDER_WITHIN_TOLERANCE", "vitals",
                event("OUT_OF_ORDER", seconds=-within), "OUT_OF_ORDER_EVENT")
        publish("LATE_EVENT", "vitals",
                event("LATE", seconds=-(args.tolerance_seconds + 30)), "LATE_EVENT")
        publish("INVALID_UTF8", "vitals", bytes([0xff, 0x00, 0x7b]), "PARSE_ERROR",
                key=bytes([0xfe, 0x61]))
        prefix = ("TICKET006_ENGINEERING_PROBE:" + session + ":").encode()
        publish("LARGE_MALFORMED_JSON", "vitals", prefix + b"x" * (450000 - len(prefix)), "PARSE_ERROR")
        publish("TOMBSTONE_NULL_KEY", "vitals", None, "PARSE_ERROR", key=None)
        bad_id = event("UNPAIRED_SURROGATE_ID")
        bad_id["event_id"] = session + "-\ud800"
        publish("UNPAIRED_SURROGATE_ID", "vitals", bad_id, "SCHEMA_ERROR")
        fallback = event("COMPOSITE_WAVEFORM", "waveform", seconds=1)
        del fallback["event_id"]
        publish("WAVEFORM_COMPOSITE_ID", "waveform", fallback, "CLEAN_EVENT")
        publish("DUPLICATE_COMPOSITE_WAVEFORM", "waveform", fallback, "DUPLICATE_EVENT")
        report["checks"]["engineering_labels"] = all(
            p["event"] is None or p["event"].get("dataset_type") == "ENGINEERING_PROBE"
            for p in report["published"])
        while True:
            collect()
            received = set(clean_by_source) | set(dlq_by_source)
            expected_sources = {(p["topic"], p["partition"], p["offset"])
                                for p in report["published"]}
            if expected_sources <= received:
                break
            job_state()
            pause()
        outputs_observed_epoch_ms = int(time.time() * 1000)
        # Require a checkpoint triggered after all probe outputs were observed.
        while True:
            after = rest(f"/jobs/{report['job_id']}/checkpoints")
            latest_after = after.get("latest", {}).get("completed")
            if (latest_after is not None
                    and latest_after["trigger_timestamp"] >= outputs_observed_epoch_ms):
                report["checkpoints_after"] = after["counts"]
                report["latest_completed_checkpoint"] = latest_after
                cp_details = rest(f"/jobs/{report['job_id']}/checkpoints/details/{latest_after['id']}")
                report["dedup_state_after"] = {
                    stream: dict(vertex_id=vertex["id"],
                                 state_size=cp_details["tasks"][vertex["id"]]["state_size"],
                                 checkpointed_size=cp_details["tasks"][vertex["id"]]["checkpointed_size"])
                    for stream, vertex in dedup_vertices.items()}
                require(all(report["dedup_state_after"][stream]["state_size"] >
                            report["dedup_state_before"][stream]["state_size"] for stream in TOPICS),
                        "Managed dedup checkpoint state did not increase for all three streams")
                break
            job_state()
            pause()
        collect()
        required_dlq = {"schema_version", "dlq_id", "error_type", "error_reason", "failed_at",
                        "original_topic", "original_partition", "original_offset", "original_key",
                        "original_key_base64", "stream_type", "event_id", "stay_id", "run_id",
                        "raw_payload", "raw_payload_base64", "raw_payload_is_null", "raw_sha256",
                        "dataset_type", "replay_session_id"}
        for probe in report["published"]:
            key = source_key(probe["topic"], probe["partition"], probe["offset"])
            expected = probe["expected"]
            if expected in {"CLEAN_EVENT", "OUT_OF_ORDER_EVENT"}:
                require(key in clean_by_source and key not in dlq_by_source,
                        f"Accepted case misrouted: {probe['case']}")
                clean = clean_by_source[key]
                require(clean["kind"] == expected, f"Wrong clean classification: {probe['case']}")
                original = probe["event"]
                for field in ("event_time", "source_event_time", "producer_time"):
                    require(clean[field] == original[field], f"Changed timestamp {field}: {probe['case']}")
                if probe["stream_type"] == "clinical":
                    require("available_time" in clean and clean["available_time"] is None,
                            "Clinical NULL availability was replaced or omitted")
                if probe["stream_type"] == "waveform":
                    require(clean["sample_count"] == 4 and clean["samples_preview"] == original["samples"],
                            "Waveform chunk/null sample preview changed")
            else:
                require(key in dlq_by_source and key not in clean_by_source,
                        f"Rejected case entered clean stream or lacks DLQ: {probe['case']}")
                delivery = dlq_by_source[key]; envelope = delivery["event"]
                require(required_dlq <= set(envelope), f"DLQ missing fields: {probe['case']}")
                require(envelope["schema_version"] == "1.0.0", "Unexpected DLQ schema version")
                require(envelope["error_type"] == expected, f"Wrong rejection type: {probe['case']}")
                require(bool(envelope["error_reason"]), "Empty DLQ error reason")
                require(envelope["original_key_base64"] == probe["key_base64"], "Original Kafka key changed")
                require(envelope["raw_payload_base64"] == probe["payload_base64"], "Original payload bytes changed")
                require(envelope["raw_sha256"] == probe["raw_sha256"], "Original payload SHA-256 changed")
                raw = None if probe["payload_base64"] is None else base64.b64decode(probe["payload_base64"])
                raw_key = None if probe["key_base64"] is None else base64.b64decode(probe["key_base64"])
                require(envelope["raw_payload"] == utf8_or_none(raw), "Original payload text changed")
                require(envelope["original_key"] == utf8_or_none(raw_key), "Original key text changed")
                require(datetime.fromisoformat(envelope["failed_at"]).tzinfo is not None,
                        "DLQ failed_at lacks timezone")
                require(envelope["stream_type"] == probe["stream_type"], "DLQ stream type changed")
                original = probe["event"] or {}
                for field in ("event_id", "stay_id", "run_id", "dataset_type", "replay_session_id"):
                    recoverable = original.get(field)
                    if isinstance(recoverable, str):
                        try:
                            recoverable.encode("utf-8", errors="strict")
                        except UnicodeEncodeError:
                            recoverable = None
                    require(envelope[field] == recoverable, f"DLQ recoverable field changed: {field}")
                expected_key = original.get("stay_id")
                expected_key = (expected_key.encode() if expected_key else
                                base64.b64decode(probe["key_base64"]) if probe["key_base64"] is not None
                                else envelope["dlq_id"].encode())
                require(envelope["raw_payload_is_null"] == (probe["payload_base64"] is None),
                        "Tombstone/null payload audit changed")
                require(delivery["kafka_key_base64"] == b64(expected_key), "DLQ Kafka key precedence failed")
            report["checks"][probe["case"]] = "PASS"
        report["checks"].update(real_watermark_gate="PASS", source_metadata_and_payload_audit="PASS",
                                 no_rejected_case_in_clean="PASS", checkpoint_continued="PASS",
                                 managed_dedup_checkpoint_state_growth="PASS")
        require(report["checkpoints_after"]["failed"] == report["checkpoints_before"]["failed"],
                "Additional checkpoint failure occurred during probes")
        report["job_state_after"] = job_state()["state"]
        report["exceptions_after"] = rest(f"/jobs/{report['job_id']}/exceptions?maxExceptions=5")
        require(report["exceptions_after"].get("root-exception") is None
                and not report["exceptions_after"].get("exceptionHistory", {}).get("entries", []),
                "Flink reported a runtime exception")
        report["status"] = "PASS"
    except Exception as error:
        report["error"] = f"{type(error).__name__}: {error}"
    finally:
        if consumer is not None:
            consumer.close()
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        report["clean_count"] = len(clean_by_source)
        report["dlq_count"] = len(dlq_by_source)
        for name, items in (("clean_events.jsonl", clean_by_source.values()),
                            ("dlq_events.jsonl", dlq_by_source.values())):
            (args.output / name).write_text("".join(json.dumps(item, ensure_ascii=True) + "\n"
                                                     for item in items), encoding="utf-8")
        (args.output / "probe_report.json").write_text(
            json.dumps(report, ensure_ascii=True, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "job_id": report.get("job_id"),
                      "replay_session_id": session, "published": len(report["published"]),
                      "clean": report["clean_count"], "dlq": report["dlq_count"],
                      "error": report.get("error"), "report": str(args.output / "probe_report.json")}))
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
