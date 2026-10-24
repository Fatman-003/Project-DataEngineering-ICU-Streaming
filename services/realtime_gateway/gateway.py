"""One in-memory Kafka-to-WebSocket connection; no database dependency."""
import asyncio
import json
import logging
import math
import os
import sys
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from aiohttp import web, WSMsgType

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "jobs/flink"))
from registry import load_patients, IDENTITY_FIELDS
from contracts import BUSINESS_TOPICS, CHART_FIELDS, parse_event
from validation import validate_event, parse_timestamp
from business_contract import (validate, ContractError, headers, deadletter, source_record,
                               FIELDS, encode, DLQ_TOPIC)

GROUP_ID = "icu-ui-gateway-v2"
SUBSCRIPTIONS = tuple(BUSINESS_TOPICS[key] for key in ("telemetry", "aggregates", "alerts"))
LOG = logging.getLogger(__name__)


def utc_now():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class GatewayState:
    def __init__(self):
        self.patients = {p["demo_id"]: dict(p, vitals={}, summaries={}, alerts=[])
                         for p in load_patients()}
        self.recent_alert_ids = OrderedDict()

    def snapshot(self):
        return json.loads(json.dumps(self.patients))

    def ingest(self, topic, event, received_at=None):
        validate(event, topic)
        patient = next((p for p in self.patients.values() if event["stay_id"] == p["stay_id"]), None)
        if patient is None:
            raise ContractError("REFERENCE_NOT_FOUND", "Unknown fixed stay")
        common = {field: patient[field] for field in ("demo_id", "bed_id", "stay_id")}
        # Application identifiers come from the registry, not optional message extras.
        payload = dict(event, **common)
        if topic == BUSINESS_TOPICS["telemetry"]:
            clock, target = "sample_time", patient["vitals"]
            previous = target.get("HR")
            if previous and parse_timestamp(previous["event_time"]) >= parse_timestamp(event[clock]):
                return None
            for signal, field in FIELDS.items():
                target[signal] = dict(common, value=event.get(field), event_time=event[clock])
            message_type = "TELEMETRY_SAMPLE"
        elif topic == BUSINESS_TOPICS["aggregates"]:
            clock, target = "window_end", patient["summaries"]
            previous = target.get("HR")
            if previous and parse_timestamp(previous[clock]) >= parse_timestamp(event[clock]):
                return None
            for signal, field in FIELDS.items():
                target[signal] = dict(common, mean=event.get("avg_" + field), window_end=event[clock])
            message_type = "VITALS_WINDOW_5M"
        else:
            existing = next((a for a in patient["alerts"] if a["alert_id"] == event["alert_id"]
                             and parse_timestamp(a["alert_time"]) == parse_timestamp(event["alert_time"])), None)
            if event["event_type"] == "SEPSIS_ALERT_STATUS_CHANGED":
                if existing is None:
                    raise ContractError("ALERT_NOT_FOUND", "Status refers to unknown alert")
                if existing["status"] == event["status"]:
                    return None
                existing["status"] = event["status"]
                message_type = "ALERT_STATUS_CHANGED"
            else:
                if existing is not None:
                    return None
                payload.update(alert_emitted_at=event["produced_at"], gateway_received_at=received_at or utc_now())
                patient["alerts"].append(payload)
                patient["alerts"] = sorted(patient["alerts"], key=lambda a: parse_timestamp(a["alert_time"]))[-100:]
                message_type = "ALERT"
        return {"message_type": message_type, "payload": payload}


class KafkaFeed:
    def __init__(self):
        from confluent_kafka import Consumer, Producer
        self.connected = False
        self.received_at = None
        self.producer = Producer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                                  "acks": "all", "enable.idempotence": True})
        self.consumer = Consumer({"bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                                  "group.id": GROUP_ID, "enable.auto.commit": False,
                                  "auto.offset.reset": "earliest", "fetch.wait.max.ms": 50})

    def subscribe(self):
        from confluent_kafka import OFFSET_BEGINNING

        def rebuild(consumer, partitions):
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
            consumer.assign(partitions)
            self.connected = True

        self.consumer.subscribe(list(SUBSCRIPTIONS), on_assign=rebuild)

    def poll(self):
        message = self.consumer.poll(0.2)
        self.received_at = utc_now() if message is not None else None
        if message is not None and message.error():
            self.connected = False
            LOG.warning("Kafka: %s", message.error())
            return None
        if message is not None:
            self.connected = True
        return message

    def reject(self, message, code, stage):
        envelope = deadletter(source_record(message), code, "Rejected gateway business message", stage, GROUP_ID)
        failures = []
        def delivered(error, record):
            if error:
                failures.append(str(error))
        self.producer.produce(DLQ_TOPIC, key=message.key() or envelope["dlq_id"].encode(),
                              value=encode(envelope).encode(), headers=headers(envelope), on_delivery=delivered)
        if self.producer.flush(20) or failures:
            raise RuntimeError("Gateway DLQ delivery failed")

    def close(self):
        self.consumer.close()


def publish(app, topic, event, received_at=None):
    message = app[STATE].ingest(topic, event, received_at)
    if message:
        for queue in tuple(app[CLIENTS]):
            if queue.full():
                # A slow UI reconnects for a fresh snapshot; Kafka polling never waits on it.
                app[CLIENTS].discard(queue)
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(None)
            else:
                queue.put_nowait(message)
    return message


async def consume(app):
    feed = app[FEED]
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="icu-kafka")
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(executor, feed.subscribe)
    try:
        while True:
            message = await loop.run_in_executor(executor, feed.poll)
            if message is not None:
                try:
                    event = parse_event(message.value().decode("utf-8"))
                    validate(event, message.topic(), message.key(), message.headers(), require_headers=True)
                    publish(app, message.topic(), event, getattr(feed, "received_at", None))
                except (ValueError, KeyError, TypeError, UnicodeError, RecursionError) as error:
                    app[REJECTED] += 1
                    code = getattr(error, "code", "INVALID_UTF8" if isinstance(error, UnicodeError) else "JSON_PARSE_ERROR")
                    stage = "PROCESSING" if code == "ALERT_NOT_FOUND" else "VALIDATION" if isinstance(error, ContractError) else "DESERIALIZATION"
                    await loop.run_in_executor(executor, feed.reject, message, code, stage)
    finally:
        await loop.run_in_executor(executor, feed.close)
        executor.shutdown(wait=False)


async def health(request):
    feed = request.app.get(FEED)
    return web.json_response({"status": "ok", "consumer_group": GROUP_ID,
                              "kafka_connected": bool(feed and feed.connected),
                              "rejected_records": request.app[REJECTED]})


async def snapshot(request):
    return web.json_response(request.app[STATE].snapshot())


async def patient(request):
    value = request.app[STATE].snapshot().get(request.match_info["demo_id"])
    if value is None:
        raise web.HTTPNotFound(text="Unknown demo patient")
    return web.json_response(value)


async def websocket(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    queue = asyncio.Queue(maxsize=256)
    request.app[CLIENTS].add(queue)
    # Registration + snapshot happen without yielding, preserving their ordering.
    initial = {"message_type": "SNAPSHOT", "payload": request.app[STATE].snapshot()}

    async def send_updates():
        await ws.send_json(initial)
        while True:
            update = await queue.get()
            if update is None:
                await ws.close(code=1013, message=b"Reconnect for current snapshot")
                return
            await ws.send_json(update)

    sender = asyncio.create_task(send_updates())
    try:
        async for message in ws:
            if message.type == WSMsgType.ERROR:
                break
    finally:
        request.app[CLIENTS].discard(queue)
        sender.cancel()
        await asyncio.gather(sender, return_exceptions=True)
    return ws


STATE = web.AppKey("state", GatewayState)
CLIENTS = web.AppKey("clients", set)
FEED = web.AppKey("feed", KafkaFeed)
REJECTED = web.AppKey("rejected", int)


def create_app(feed_factory=KafkaFeed):
    app = web.Application()
    app[STATE], app[CLIENTS], app[REJECTED] = GatewayState(), set(), 0
    app.router.add_get("/api/health", health)
    app.router.add_get("/api/patients", snapshot)
    app.router.add_get("/api/patients/{demo_id}", patient)
    app.router.add_get("/api/snapshot", snapshot)
    app.router.add_get("/ws", websocket)

    async def lifecycle(app):
        task = None
        if feed_factory is not None:
            app[FEED] = feed_factory()
            task = asyncio.create_task(consume(app))
            def report_failure(completed):
                if not completed.cancelled() and completed.exception():
                    LOG.error("Kafka feed stopped", exc_info=completed.exception())
            task.add_done_callback(report_failure)
        yield
        for queue in tuple(app[CLIENTS]):
            while not queue.empty():
                queue.get_nowait()
            queue.put_nowait(None)
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    app.cleanup_ctx.append(lifecycle)
    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    web.run_app(create_app(), host="0.0.0.0", port=8083)
