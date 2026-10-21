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
from runtime_config import HYPOTENSION_MAP_THRESHOLD, HYPOTENSION_CONFIRMATION_SECONDS

GROUP_ID = "icu-ui-gateway-v2"
SUBSCRIPTIONS = tuple(BUSINESS_TOPICS[key] for key in ("chartevents", "aggregates", "alerts"))
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
        if topic not in SUBSCRIPTIONS or event.get("schema_version") != "2.0":
            raise ValueError("Unsupported gateway topic/schema")
        patient = next((p for p in self.patients.values()
                        if all(event.get(field) == p[field] for field in IDENTITY_FIELDS)), None)
        if patient is None:
            raise ValueError("Gateway identity must match P01-P04")
        common = {field: patient[field] for field in ("demo_id", "bed_id", "stay_id")}
        if topic == BUSINESS_TOPICS["chartevents"]:
            validate_event({key: event[key] for key in CHART_FIELDS}, "chartevents")
            if event.get("quality_status") != "VALID":
                raise ValueError("Only clean vitals can reach the UI")
            signal, clock, target = event["signal_name"], "event_time", patient["vitals"]
            payload = dict(common, signal=signal, value=event["valuenum"], unit=event["valueuom"],
                           event_time=event[clock])
            message_type = "VITAL_UPDATE"
        elif topic == BUSINESS_TOPICS["aggregates"]:
            if event.get("window_minutes") != 15:
                raise ValueError("Only 15-minute summaries can reach the UI")
            for field in ("mean", "min", "max"):
                if type(event.get(field)) not in (int, float) or not math.isfinite(event[field]):
                    raise ValueError("Summary values must be finite")
            signal, clock, target = event["signal_name"], "window_end", patient["summaries"]
            payload = dict(common, signal=signal, **{key: event[key] for key in ("mean", "min", "max", clock)})
            message_type = "VITAL_15M"
        else:
            parse_timestamp(event["trigger_time"])
            alert_id = event["alert_id"]
            if event.get("alert_type") != "SUSTAINED_HYPOTENSION" or event.get("signal_name") != "MAP":
                raise ValueError("Unsupported alert")
            if alert_id in self.recent_alert_ids:
                return None
            self.recent_alert_ids[alert_id] = True
            if len(self.recent_alert_ids) > 4096:
                self.recent_alert_ids.popitem(last=False)
            payload = dict(common, severity=event["severity"],
                           message=(f"MAP < {HYPOTENSION_MAP_THRESHOLD:g} sustained for "
                                    f"{HYPOTENSION_CONFIRMATION_SECONDS / 60:g} minutes"),
                           trigger_time=event["trigger_time"],
                           alert_id=alert_id, status=event["status"])
            if "alert_emitted_at" in event:
                parse_timestamp(event["alert_emitted_at"])
                payload.update(alert_emitted_at=event["alert_emitted_at"],
                               gateway_received_at=received_at or utc_now())
            patient["alerts"].append(payload)
            patient["alerts"] = sorted(patient["alerts"], key=lambda a: parse_timestamp(a["trigger_time"]))[-100:]
            return {"message_type": "ALERT", "payload": payload}
        timestamp = parse_timestamp(payload[clock])
        previous = target.get(signal)
        if previous and parse_timestamp(previous[clock]) >= timestamp:
            return None
        target[signal] = payload
        return {"message_type": message_type, "payload": payload}


class KafkaFeed:
    def __init__(self):
        from confluent_kafka import Consumer
        self.connected = False
        self.received_at = None
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
                    publish(app, message.topic(), parse_event(message.value().decode("utf-8")),
                            getattr(feed, "received_at", None))
                except (ValueError, KeyError, TypeError, UnicodeError):
                    app[REJECTED] += 1
                    LOG.exception("Discarding invalid business record")
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
