"""TICKET-004: paced canonical/WFDB replay into Kafka RAW topics only."""
import argparse
import itertools
import json
import math
import sys
import time
import uuid
from datetime import datetime
from collections import Counter

import pyarrow.parquet as pq
from confluent_kafka import Producer
from config import TOPICS, settings
from kafka_topics import ensure_topics
from serializers import encode, message_key, stamp
from waveforms import descriptors, decode_chunk


def delay_seconds(previous, current, speed):
    if not math.isfinite(speed) or speed <= 0:
        raise ValueError("Speed must be positive and finite")
    gap = (current - previous).total_seconds()
    if gap < 0:
        raise ValueError("Events must be ordered")
    return gap / speed


def prepare_events(rows, stream):
    events = []
    for row in rows:
        event = dict(row)
        if stream == "clinical":
            known = row["available_time"] is not None
            # This is pacing metadata, never a replacement for available_time.
            event["event_time"] = row["available_time"] if known else row["chart_time"]
            event["source_event_time"] = row["chart_time"]
            event["replay_time_basis"] = (
                "available_time" if known else "chart_time_unknown_availability")
        else:
            event["replay_time_basis"] = "canonical_event_time"
        if not isinstance(event["event_time"], datetime) or not event.get("event_id"):
            raise ValueError("Missing event_time/event_id")
        message_key(event)
        events.append(event)
    return sorted(events, key=lambda event: (event["event_time"], event["event_id"]))


class Pacer:
    """Absolute monotonic deadlines avoid cumulative serialization/delivery drift."""
    def __init__(self, speed, clock=time.monotonic):
        self.speed = speed
        self.clock = clock
        self.origin_event = None
        self.origin_runtime = None

    def wait(self, event_time, poll):
        if self.origin_event is None:
            self.origin_event = event_time
            self.origin_runtime = self.clock()
        deadline = self.origin_runtime + delay_seconds(self.origin_event, event_time, self.speed)
        while True:
            remaining = deadline - self.clock()
            if remaining <= 0:
                break
            poll(min(0.2, remaining))


class Delivery:
    def __init__(self, producer, session_id, clock=time.monotonic, now=None):
        self.producer = producer
        self.session_id = session_id
        self.clock = clock
        self.now = now
        self.queued = 0
        self.published = 0
        self.failed = 0
        self.samples = 0
        self.missing_samples = 0
        self.signals = Counter()
        self.errors = []
        self.first = None
        self.last = None

    def poll(self, seconds):
        self.producer.poll(seconds)
        if self.failed:
            raise RuntimeError("Kafka delivery failed: " + self.errors[-1])

    def send(self, topic, event):
        key = message_key(event)
        deadline = self.clock() + 30

        def delivered(error, message):
            if error is not None:
                self.failed += 1
                self.errors.append(str(error))
                print(f"DELIVERY ERROR {event['event_id']}: {error}", file=sys.stderr)
            else:
                self.published += 1
                self.samples += event.get("sample_count", 0)
                self.missing_samples += event.get("missing_sample_count", 0)
                label = event.get("signal_name", event.get("event_type", "unknown"))
                self.signals[label] += 1

        while True:
            payload = stamp(event, self.session_id, self.now() if self.now else None)
            try:
                self.producer.produce(topic, key=key, value=encode(payload), on_delivery=delivered)
                self.queued += 1
                self.first = self.first or event["event_time"]
                self.last = event["event_time"]
                break
            except BufferError:
                if self.clock() >= deadline:
                    raise TimeoutError("Kafka producer queue remained full for 30 seconds")
                self.poll(0.1)
        self.poll(0)

    def flush(self):
        remaining = self.producer.flush(15)
        if remaining:
            self.errors.append(f"{remaining} messages still pending after flush")
        return remaining


def cli():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream", choices=list(TOPICS), required=True)
    parser.add_argument("--speed", type=int, choices=(1, 10, 60), default=10)
    parser.add_argument("--stay-id", help="Filter to one Reference stay; recommended for normal runs")
    parser.add_argument("--max-events", type=int, help="Bound a smoke test by events/chunks")
    parser.add_argument("--start-time", type=datetime.fromisoformat,
                        help="Inclusive historical ISO timestamp (naive, like canonical data)")
    parser.add_argument("--waveform-seconds", type=float, default=10,
                        help="Global historical window from first eligible waveform start (default 10)")
    args = parser.parse_args()
    if args.max_events is not None and args.max_events <= 0:
        parser.error("max-events must be positive")
    if not math.isfinite(args.waveform_seconds) or args.waveform_seconds <= 0:
        parser.error("waveform-seconds must be positive and finite")
    if args.start_time is not None and args.start_time.tzinfo is not None:
        parser.error("start-time must be timezone-naive, matching MIMIC historical timestamps")
    return args


def run(args):
    started = time.monotonic()
    session = "REPLAY-" + uuid.uuid4().hex
    delivery = None
    pending = 0
    attempted = 0
    input_rows = 0
    selected_rows = 0
    error = None
    interrupted = False
    try:
        config = settings()
        filename = {"vitals": "telemetry_event.parquet", "clinical": "clinical_event.parquet",
                    "waveform": "waveform_manifest.parquet"}[args.stream]
        rows = pq.read_table(config["reference"] / filename).to_pylist()
        input_rows = len(rows)
        if args.stay_id:
            rows = [row for row in rows if row["stay_id"] == args.stay_id]
        selected_rows = len(rows)
        if not rows:
            raise ValueError("No input rows match the requested stay")
        if args.stream == "waveform":
            events = descriptors(rows, args.waveform_seconds, args.start_time)
        else:
            events = iter(prepare_events(rows, args.stream))
            if args.start_time:
                events = (event for event in events if event["event_time"] >= args.start_time)
        if args.max_events:
            events = itertools.islice(events, args.max_events)
        # Fail empty selections before contacting or mutating Kafka.
        first_event = next(events, None)
        if first_event is None:
            raise ValueError("No events in the requested historical interval")
        events = itertools.chain([first_event], events)
        if args.stream != "waveform" and not args.stay_id and len({r["stay_id"] for r in rows}) > 1:
            print("Historical timeline spans multiple stays/years; use --stay-id for a bounded run.",
                  file=sys.stderr)
        ensure_topics(config["bootstrap"])
        producer = Producer({
            "bootstrap.servers": config["bootstrap"], "client.id": session,
            "enable.idempotence": True, "acks": "all",
            "delivery.timeout.ms": 10000, "request.timeout.ms": 5000,
            "linger.ms": 5,
        })
        delivery = Delivery(producer, session)
        pacer = Pacer(args.speed)
        for event in events:
            pacer.wait(event["event_time"], delivery.poll)
            attempted += 1
            if args.stream == "waveform":
                event = decode_chunk(event, config["waveform"])
            delivery.send(TOPICS[args.stream], event)
    except KeyboardInterrupt:
        error = "Interrupted; queued messages flushed before exit"
        interrupted = True
    except Exception as exc:
        error = str(exc)
        print(f"Replay failed: {error}", file=sys.stderr)
    finally:
        if delivery is not None:
            try:
                pending = delivery.flush()
            except Exception as exc:
                error = error or f"Flush failed: {exc}"
        summary = {
            "stream": args.stream, "replay_session_id": session,
            "input_rows": input_rows, "selected_input_rows": selected_rows,
            "attempted": attempted, "queued": delivery.queued if delivery else 0,
            "published": delivery.published if delivery else 0,
            "failed": delivery.failed if delivery else 0,
            "unpublished": attempted - (delivery.published if delivery else 0),
            "pending_after_flush": pending, "speed": args.speed,
            "first_event_time": delivery.first if delivery else None,
            "last_event_time": delivery.last if delivery else None,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "error": error, "delivery_errors": delivery.errors if delivery else [],
        }
        if args.stream == "waveform":
            summary.update(chunks_published=summary["published"],
                           samples_published=delivery.samples if delivery else 0,
                           missing_samples=delivery.missing_samples if delivery else 0,
                           signals_replayed=dict(delivery.signals) if delivery else {},
                           waveform_seconds=args.waveform_seconds)
        ok = (error is None and delivery is not None and delivery.failed == 0
              and pending == 0 and delivery.queued == delivery.published
              and not delivery.errors)
        summary["status"] = "PASS" if ok else "INTERRUPTED" if interrupted else "FAIL"
        print(encode(summary).decode(), flush=True)
    return 0 if ok else 130 if interrupted else 1


if __name__ == "__main__":
    raise SystemExit(run(cli()))
