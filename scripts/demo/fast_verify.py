"""Real Kafka/Flink/Gateway/WebSocket verification; no alternate business path."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import time

from aiohttp import ClientSession, WSMsgType
from contracts import CORE_TOPICS, TOPICS, BUSINESS_TOPICS, DLQ_TOPIC, stay_key
from event_time import event_timestamp_ms
from run_icu_demo import run_demo, demo_events, PATIENTS
from validation import parse_timestamp

SPEED = 24


def transport_timings(payload, websocket_received_at):
    emitted = parse_timestamp(payload["alert_emitted_at"])
    gateway = parse_timestamp(payload["gateway_received_at"])
    websocket = parse_timestamp(websocket_received_at)
    gateway_ms = (gateway - emitted).total_seconds() * 1000
    websocket_ms = (websocket - emitted).total_seconds() * 1000
    # Host and Linux-container wall clocks can differ by microseconds. Preserve
    # both measured deltas rather than assuming their relative clock ordering.
    if not (0 <= gateway_ms < 2000 and 0 <= websocket_ms < 2000):
        raise AssertionError(f"Invalid clock order or transport >=2s: gateway={gateway_ms:.3f}ms, WS={websocket_ms:.3f}ms")
    return {"alert_emitted_at": payload["alert_emitted_at"],
            "gateway_received_at": payload["gateway_received_at"],
            "websocket_received_at": websocket_received_at,
            "flink_to_gateway_ms": round(gateway_ms, 3),
            "flink_to_websocket_ms": round(websocket_ms, 3)}


def read_pipeline(timeout=8):
    """Read current empty-reset topics directly without joining a consumer group."""
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "icu-fast-verify-reader",
                         "enable.auto.commit": False, "fetch.wait.max.ms": 20})
    result = {topic: [] for topic in CORE_TOPICS}
    try:
        bounds = {topic: consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=5)
                  for topic in CORE_TOPICS}
        targets = {topic: high for topic, (low, high) in bounds.items() if high > low}
        consumer.assign([TopicPartition(topic, 0, bounds[topic][0]) for topic in targets])
        deadline = time.monotonic() + timeout
        complete = set()
        while set(targets) != complete:
            if time.monotonic() >= deadline:
                raise AssertionError("Timed out reading pipeline evidence")
            message = consumer.poll(0.1)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(message.error())
            topic = message.topic()
            event = json.loads(message.value())
            if topic != DLQ_TOPIC and message.key().decode() != stay_key(event):
                raise AssertionError(f"Wrong Kafka key in {topic}")
            result[topic].append(event)
            if message.offset() + 1 >= targets[topic]:
                complete.add(topic)
        return result
    finally:
        consumer.close()


def validate_pipeline(records, start, ws_alerts, snapshot, message_counts):
    expected = list(demo_events(start=start))
    raw = [(topic, event) for topic in TOPICS.values() for event in records[topic]]
    if Counter((topic, e["event_id"]) for topic, e in raw) != Counter((topic, e["event_id"]) for topic, e in expected):
        raise AssertionError("FastVerify skipped or changed the six-minute logical scenario")
    charts = records[BUSINESS_TOPICS["chartevents"]]
    labs = records[BUSINESS_TOPICS["labevents"]]
    if len(charts) != 1441 or len(labs) != 8 or len(records[DLQ_TOPIC]) != 3:
        raise AssertionError("Unexpected CLEAN/DLQ counts")
    if not any(e["event_id"].endswith("-ooo") and e["ordering_status"] == "OUT_OF_ORDER" for e in charts):
        raise AssertionError("Tolerated out-of-order observation was not accepted")
    summaries = records[BUSINESS_TOPICS["aggregates"]]
    if not summaries or any(e["window_minutes"] != 15 for e in summaries):
        raise AssertionError("Expected only 15-minute statistics")
    kafka_alerts = records[BUSINESS_TOPICS["alerts"]]
    counts = {p["demo_id"]: sum(e["stay_id"] == p["stay_id"] for e in kafka_alerts) for p in PATIENTS}
    if counts != {"P01": 0, "P02": 1, "P03": 0, "P04": 0}:
        raise AssertionError(f"Unexpected Kafka alert counts: {counts}")
    if len(ws_alerts) != 1 or ws_alerts[0][0]["alert_id"] != kafka_alerts[0]["alert_id"]:
        raise AssertionError("Expected exactly one matching live WebSocket alert")
    if set(snapshot) != {p["demo_id"] for p in PATIENTS} or any(
            len(snapshot[p]["alerts"]) != counts[p] or not snapshot[p]["vitals"] or not snapshot[p]["summaries"]
            for p in counts):
        raise AssertionError("Gateway snapshot does not match the real pipeline")
    if any(message_counts[k] == 0 for k in ("VITAL_UPDATE", "VITAL_15M", "ALERT")):
        raise AssertionError("Missing live Gateway message type")
    alert = kafka_alerts[0]
    origin = event_timestamp_ms({"event_time": start.isoformat()})
    first = (event_timestamp_ms({"event_time": alert["episode_start"]}) - origin) / 1000
    qualifying = (event_timestamp_ms({"event_time": alert["trigger_time"]}) - origin) / 1000
    if (first, qualifying, qualifying - first) != (13, 313, 300):
        raise AssertionError("Clinical confirmation timestamps changed")
    return {"logical_duration_seconds": 360, "speed_factor": SPEED,
            "logical_first_p02_low": "00:13", "logical_qualifying_p02_map": "05:13",
            "clinical_confirmation_seconds": qualifying - first,
            **transport_timings(*ws_alerts[0]), "alert_counts": counts,
            "topic_record_counts": {topic: len(events) for topic, events in records.items()},
            "websocket_message_counts": dict(message_counts), "result": "PASS"}


async def verify_async():
    began = time.monotonic()
    ws_alerts, message_counts = [], Counter()
    async with ClientSession() as session:
        async with session.ws_connect("http://127.0.0.1:8083/ws") as ws:
            initial = await ws.receive_json(timeout=5)
            if initial.get("message_type") != "SNAPSHOT" or any(
                    p["vitals"] or p["summaries"] or p["alerts"] for p in initial["payload"].values()):
                raise AssertionError("WebSocket did not start with an empty reset snapshot")

            async def receive():
                async for message in ws:
                    received_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                    if message.type != WSMsgType.TEXT:
                        continue
                    update = json.loads(message.data)
                    message_counts[update["message_type"]] += 1
                    if update["message_type"] == "ALERT":
                        ws_alerts.append((update["payload"], received_at))

            collector = asyncio.create_task(receive())
            try:
                producer_began = time.monotonic()
                start = await asyncio.to_thread(run_demo, speed=SPEED, quiet=True, skip_missed_ticks=False)
                producer_seconds = time.monotonic() - producer_began
                deadline = time.monotonic() + 5
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
                        await asyncio.sleep(0.1)
                return dict(result, producer_seconds=round(producer_seconds, 3),
                            effective_speed_factor=round(360 / producer_seconds, 3),
                            verification_seconds=round(time.monotonic() - began, 3), event_start=start.isoformat())
            finally:
                collector.cancel()
                await asyncio.gather(collector, return_exceptions=True)


def verify():
    return asyncio.run(verify_async())
