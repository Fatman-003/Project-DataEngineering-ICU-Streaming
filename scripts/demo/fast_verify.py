"""Live verification of the migrated Kafka/Flink/Gateway contract."""
import asyncio
from collections import Counter
from datetime import datetime, timezone, timedelta
import json
import math
import time
from aiohttp import ClientSession, WSMsgType
from contracts import CORE_TOPICS, TOPICS, BUSINESS_TOPICS, DLQ_TOPIC, stay_key
from business_contract import validate, headers, header_dict, FIELDS, timestamp
from run_icu_demo import run_demo, demo_events, PATIENTS
from runtime_config import WindowAlertConfig, RuntimeConfig

SPEED = 24


def transport_timings(payload, websocket_received_at):
    emitted = timestamp(payload["alert_emitted_at"])
    gateway = timestamp(payload["gateway_received_at"])
    websocket = timestamp(websocket_received_at)
    deltas = ((gateway - emitted).total_seconds() * 1000, (websocket - emitted).total_seconds() * 1000)
    if any(not 0 <= value < 2000 for value in deltas):
        raise AssertionError(f"Flink alert transport exceeds 2s or clock order invalid: {deltas}")
    return dict(alert_emitted_at=payload["alert_emitted_at"], gateway_received_at=payload["gateway_received_at"],
                websocket_received_at=websocket_received_at, flink_to_gateway_ms=round(deltas[0], 3),
                flink_to_websocket_ms=round(deltas[1], 3))


def read_pipeline(timeout=20):
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "icu-fast-verify-reader",
                         "enable.auto.commit": False, "fetch.wait.max.ms": 20})
    result = {topic: [] for topic in CORE_TOPICS}
    try:
        bounds = {topic: consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=5) for topic in CORE_TOPICS}
        targets = {topic: high for topic, (low, high) in bounds.items() if high > low}
        consumer.assign([TopicPartition(topic, 0, bounds[topic][0]) for topic in targets])
        deadline, complete = time.monotonic() + timeout, set()
        while set(targets) != complete:
            if time.monotonic() >= deadline:
                raise AssertionError("Timed out reading pipeline evidence")
            message = consumer.poll(0.1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            topic, event = message.topic(), json.loads(message.value())
            if topic in BUSINESS_TOPICS.values():
                validate(event, topic, message.key(), message.headers(), require_headers=True)
            elif topic == DLQ_TOPIC:
                actual = header_dict(message.headers())
                assert all(actual.get(k) == v.decode() for k, v in headers(event))
            elif message.key().decode() != stay_key(event):
                raise AssertionError("RAW Kafka key differs from stay_id")
            result[topic].append(event)
            if message.offset() + 1 >= targets[topic]:
                complete.add(topic)
        return result
    finally:
        consumer.close()


def validate_pipeline(records, start, ws_alerts, snapshot, message_counts):
    expected = list(demo_events(start=start))
    raw = [(topic, event) for topic in TOPICS.values() for event in records[topic]]
    assert Counter((t, e["event_id"]) for t, e in raw) == Counter((t, e["event_id"]) for t, e in expected), "RAW counts changed"
    wide = records[BUSINESS_TOPICS["telemetry"]]
    assert len(wide) == 4 * 360, f"Expected1440 wide samples, got{len(wide)}"
    assert len({(e["stay_id"], e["sample_time"]) for e in wide}) == len(wide), "Duplicate wide sample"
    assert all(all(e.get(f) is not None for f in FIELDS.values()) for e in wide), "Incomplete normal sample"
    rejects = records[DLQ_TOPIC]
    reasons = Counter(e["error_message"].split(":")[0] for e in rejects)
    assert reasons == {"DUPLICATE_EVENT": 1, "VALUE_ERROR": 1, "LATE_EVENT": 1}, reasons
    windows = records[BUSINESS_TOPICS["aggregates"]]
    assert windows, "No event-time window closed"
    assert len({(e["stay_id"], e["window_end"]) for e in windows}) == len(windows)
    for result in windows:
        end = timestamp(result["window_end"])
        selected = [e for e in wide if e["stay_id"] == result["stay_id"]
                    and end - timedelta(minutes=5) <= timestamp(e["sample_time"]) < end]
        assert selected
        for field in FIELDS.values():
            values = [e[field] for e in selected if e[field] is not None]
            assert math.isclose(result["avg_" + field], sum(values) / len(values), rel_tol=1e-10)
    alerts = [e for e in records[BUSINESS_TOPICS["alerts"]] if e["event_type"] == "SEPSIS_ALERT_CREATED"]
    counts = {p["demo_id"]: sum(e["stay_id"] == p["stay_id"] for e in alerts) for p in PATIENTS}
    assert counts == {"P01": 0, "P02": 1, "P03": 0, "P04": 0}, counts
    alert = alerts[0]
    assert alert["alert_type"] == "HYPOTENSION_SUSTAINED" and alert["severity"] == "CRITICAL" and alert["status"] == "ACTIVE"
    assert (timestamp(alert["alert_time"]) - start).total_seconds() == 42, alert
    assert len(ws_alerts) == 1 and ws_alerts[0][0]["alert_id"] == alert["alert_id"], "WebSocket alert mismatch"
    assert set(snapshot) == {p["demo_id"] for p in PATIENTS}
    for name, patient in snapshot.items():
        assert len(patient["alerts"]) == counts[name] and patient["vitals"] and patient["summaries"]
    assert all(message_counts[k] > 0 for k in ("TELEMETRY_SAMPLE", "VITALS_WINDOW_5M", "ALERT"))
    return dict(result="PASS", logical_duration_seconds=360, speed_factor=SPEED, alert_counts=counts,
                confirmation_seconds=30, recovery_observations=5, recovery_window_seconds=10,
                lateness_seconds=RuntimeConfig().max_out_of_orderness_seconds,
                map_gap_seconds=WindowAlertConfig().map_max_sample_gap_seconds,
                bucket_finality="UTC 1s buckets finalized at watermark; <=2s OOO tail plus bucket completion",
                topic_record_counts={t: len(v) for t, v in records.items()},
                websocket_message_counts=dict(message_counts), **transport_timings(*ws_alerts[0]))


async def verify_async():
    began = time.monotonic()
    ws_alerts, message_counts = [], Counter()
    async with ClientSession() as session:
        async with session.ws_connect("http://127.0.0.1:8083/ws") as ws:
            initial = await ws.receive_json(timeout=5)
            assert initial["message_type"] == "SNAPSHOT"
            assert all(not p["vitals"] and not p["summaries"] and not p["alerts"] for p in initial["payload"].values())
            async def receive():
                async for message in ws:
                    received_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                    if message.type == WSMsgType.TEXT:
                        update = json.loads(message.data)
                        message_counts[update["message_type"]] += 1
                        if update["message_type"] == "ALERT":
                            ws_alerts.append((update["payload"], received_at))
            collector = asyncio.create_task(receive())
            try:
                producer_began = time.monotonic()
                start = await asyncio.to_thread(run_demo, speed=SPEED, quiet=True, skip_missed_ticks=False)
                producer_seconds = time.monotonic() - producer_began
                deadline = time.monotonic() + 90
                while True:
                    records = await asyncio.to_thread(read_pipeline)
                    async with session.get("http://127.0.0.1:8083/api/snapshot") as response:
                        snapshot = await response.json()
                    try:
                        result = validate_pipeline(records, start, ws_alerts, snapshot, message_counts)
                        break
                    except AssertionError:
                        if time.monotonic() >= deadline:
                            raise
                        await asyncio.sleep(0.2)
                return dict(result, producer_seconds=round(producer_seconds, 3),
                            verification_seconds=round(time.monotonic() - began, 3), event_start=start.isoformat())
            finally:
                collector.cancel()
                await asyncio.gather(collector, return_exceptions=True)


def verify():
    return asyncio.run(verify_async())
