#!/usr/bin/env python3
"""High-throughput telemetry-only Kafka simulator for ICU demo and load tests."""
import argparse
import json
import math
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    from confluent_kafka import Producer
except ImportError as error:
    raise SystemExit("Missing dependency: pip install confluent-kafka") from error

DEFAULT_STAY_IDS = "37323533,35727289,39864867,32359580"
DEFAULT_TOPIC = "icu.telemetry"

HEADERS = {
    "content-type": "application/json; charset=utf-8",
    "schema-version": "1.0",
    "event-type": "TELEMETRY_SAMPLE",
    "producer": "icu-telemetry-simulator",
}

PROFILES = {
    "stable": {"hr": (72, 8), "sbp": (118, 7), "spo2": (97, 1.5)},
    "septic": {"hr": (102, 10), "sbp": (98, 9), "spo2": (94, 2.5)},
    "post_op": {"hr": (84, 7), "sbp": (126, 8), "spo2": (96, 1.8)},
    "respiratory": {"hr": (92, 9), "sbp": (108, 8), "spo2": (91, 3.0)},
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TELEMETRY_TOPIC", DEFAULT_TOPIC))
    parser.add_argument("--total", type=int, default=1_000_000)
    parser.add_argument("--stay-ids", default=DEFAULT_STAY_IDS, help="Comma-separated int64 stay IDs")
    parser.add_argument(
        "--rate",
        type=float,
        default=0,
        help="Events per second across all stays; 0 disables rate limiting",
    )
    parser.add_argument(
        "--interval-us",
        type=int,
        default=1000,
        help="Event-time spacing per stay when --span-minutes is 0",
    )
    parser.add_argument(
        "--span-minutes",
        type=float,
        default=60,
        help="Event-time window covered by the run, ending at start; "
        "0 keeps --interval-us spacing. Wall pacing still follows --rate.",
    )
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--null-rate", type=float, default=0.02)
    parser.add_argument("--hypotension-rate", type=float, default=0.0)
    parser.add_argument("--progress-interval", type=int, default=50_000)
    parser.add_argument(
        "--reset",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Truncate DB stream tables and recreate empty Kafka topics before "
        "the run so every simulation starts from a clean slate (default: on)",
    )
    parser.add_argument(
        "--reset-only",
        action="store_true",
        help="Run the reset (DB stream tables + Kafka topics) and exit without producing",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def validate_args(args):
    if args.total <= 0:
        raise ValueError("--total must be greater than 0")
    if args.rate < 0:
        raise ValueError("--rate must be 0 or greater")
    if args.interval_us <= 0:
        raise ValueError("--interval-us must be greater than 0")
    if args.span_minutes < 0:
        raise ValueError("--span-minutes must be 0 or greater")
    if not 0 <= args.null_rate < 1:
        raise ValueError("--null-rate must be in [0, 1)")
    if not 0 <= args.hypotension_rate < 1:
        raise ValueError("--hypotension-rate must be in [0, 1)")


def parse_stay_ids(value):
    stay_ids = [int(item.strip()) for item in value.split(",") if item.strip()]
    if not stay_ids:
        raise ValueError("At least one stay ID is required")
    if any(stay_id <= 0 or stay_id >= 2**63 for stay_id in stay_ids):
        raise ValueError("Stay IDs must be positive int64 values")
    return stay_ids


STREAM_TABLES = ("patient_telemetry_stream", "vitals_sliding_window_5m", "sepsis_alert_events")


def load_local_env():
    values = {}
    path = Path(__file__).resolve().parents[2] / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip("\"'")
    return values


def reset_stream_state(bootstrap):
    """Recreate empty app topics and truncate the stream hypertables.

    Reference/seed data and the DB volume are untouched; only run-derived data
    (Kafka topics, telemetry/vitals/alert rows) is cleared. Consumer groups are
    never deleted: their offsets die together with the deleted topics, so a
    running storage writer simply resumes from the head of the fresh topics.
    """
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import AdminClient
    # kafka_topics puts jobs/flink on sys.path first; importing `contracts`
    # before it would resolve to the unrelated PyPI package of the same name.
    from kafka_topics import ensure_topics, topic_offsets
    from contracts import CORE_TOPICS

    print("Resetting Kafka topics...", flush=True)
    admin = AdminClient({"bootstrap.servers": bootstrap, "broker.address.family": "v4"})
    existing = admin.list_topics(timeout=10).topics
    targets = [topic for topic in CORE_TOPICS if topic in existing]
    for future in (
        admin.delete_topics(targets, operation_timeout=15).values() if targets else ()
    ):
        try:
            future.result(20)
        except KafkaException as error:
            print(f"  note: topic delete reported {error}", flush=True)

    # Deletion, server-side auto-recreation and replica election are all
    # asynchronous: keep re-ensuring topics and verifying emptiness until the
    # six application topics exist and hold no messages.
    deadline = time.monotonic() + 45
    last_error = None
    while True:
        try:
            ensure_topics(bootstrap)
            bounds = topic_offsets(list(CORE_TOPICS), bootstrap)
            leftovers = {
                topic: partitions
                for topic, partitions in bounds.items()
                if any(high > 0 for _, high in partitions.values())
            }
            if not leftovers:
                print(f"Kafka topics empty: {', '.join(sorted(bounds))}", flush=True)
                break
            last_error = f"topics still hold messages: {leftovers}"
        except KafkaException as error:
            last_error = str(error)

        if time.monotonic() >= deadline:
            raise SystemExit(
                f"Kafka reset failed: {last_error}\n"
                "Check icu-kafka is running: docker start icu-kafka"
            )
        time.sleep(1)

    try:
        import psycopg

        env = load_local_env()
        connection = psycopg.connect(
            host="127.0.0.1",
            port=int(env.get("TIMESCALE_PORT", "5432")),
            dbname=env.get("POSTGRES_DB", "icu"),
            user=env.get("POSTGRES_USER", "icu_app"),
            password=env.get("POSTGRES_PASSWORD", ""),
            connect_timeout=5,
        )
        with connection.transaction():
            connection.execute("TRUNCATE " + ", ".join(STREAM_TABLES))
        connection.close()
        print(f"DB stream tables truncated: {', '.join(STREAM_TABLES)}", flush=True)
    except Exception as error:
        print(f"WARNING: DB stream truncate skipped: {error}", flush=True)


def timestamp(value):
    return value.isoformat(timespec="microseconds").replace("+00:00", "Z")


def build_headers(event):
    headers = dict(HEADERS)
    headers["message-id"] = event["message_id"]
    return [(key, value.encode("utf-8")) for key, value in headers.items()]


def make_event(stay_id, sample_time, rng, null_rate, hypotension_rate, profile_name):
    profile = PROFILES[profile_name]
    hypotension = rng.random() < hypotension_rate

    heart_rate = max(25, min(220, rng.gauss(*profile["hr"])))
    sbp = max(45, min(260, rng.gauss(*profile["sbp"])))
    if hypotension:
        sbp = max(45, min(90, rng.gauss(72, 7)))
    spo2 = max(60, min(100, rng.gauss(*profile["spo2"])))
    map_value = max(25, min(220, sbp * 0.62 + heart_rate * 0.06))

    values = {
        "heart_rate": round(heart_rate, 1),
        "map": round(map_value, 1),
        "sbp": round(sbp, 1),
        "spo2": round(spo2, 1),
    }

    nullable = [key for key in values if rng.random() < null_rate]
    if len(nullable) < len(values):
        for key in nullable:
            values[key] = None

    event_time = datetime.now(timezone.utc)
    message_id = str(uuid.uuid4())
    return {
        "schema_version": "1.0",
        "message_id": message_id,
        "event_type": "TELEMETRY_SAMPLE",
        "produced_at": timestamp(event_time),
        "producer": "icu-telemetry-simulator",
        "sample_time": timestamp(sample_time),
        "stay_id": stay_id,
        **values,
    }


def producer_config(bootstrap):
    return {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 10,
        "batch.num.messages": 1000,
        "queue.buffering.max.messages": 200_000,
        "queue.buffering.max.ms": 10,
        "message.timeout.ms": 300_000,
    }


def rate_deadline(rate):
    return time.monotonic() if rate <= 0 else time.monotonic()


def main():
    args = parse_args()
    validate_args(args)

    if args.reset_only:
        reset_stream_state(args.bootstrap)
        print("Reset complete; no simulation (--reset-only).")
        return 0

    if args.reset and not args.dry_run:
        reset_stream_state(args.bootstrap)

    stay_ids = parse_stay_ids(args.stay_ids)
    rng = random.Random(args.seed)
    per_stay = math.ceil(args.total / len(stay_ids))
    if args.span_minutes > 0:
        # Spread event time over the requested window; wall pacing stays tied
        # to --rate so charts fill the whole range as the run streams.
        interval_us = max(1, int(args.span_minutes * 60_000_000) // per_stay)
    else:
        interval_us = int(1_000_000 / args.rate) if args.rate > 0 else args.interval_us
    event_span_us = per_stay * interval_us
    now = datetime.now(timezone.utc)
    starts = {
        stay_id: now - timedelta(microseconds=event_span_us)
        for stay_id in stay_ids
    }
    sequences = {stay_id: 0 for stay_id in stay_ids}
    profile_names = list(PROFILES)

    producer = None
    delivered = 0
    failed = 0
    started = time.monotonic()
    stop = False

    def delivery_handler(error, _message):
        nonlocal delivered, failed
        if error is None:
            delivered += 1
        else:
            failed += 1
            print(f"Delivery failed: {error}", file=sys.stderr)

    if not args.dry_run:
        producer = Producer(producer_config(args.bootstrap))

    def request_stop(_signum, _frame):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)

    print(
        f"Simulating {args.total:,} telemetry events across {len(stay_ids)} stays; "
        f"event spacing per stay={interval_us}us"
    )

    sent = 0
    next_progress = args.progress_interval
    deadline = rate_deadline(args.rate)
    tick = 1 / args.rate if args.rate > 0 else 0

    try:
        while sent < args.total and not stop:
            stay_id = stay_ids[sent % len(stay_ids)]
            sequence = sequences[stay_id]
            sequences[stay_id] = sequence + 1
            sample_time = starts[stay_id] + timedelta(microseconds=sequence * interval_us)
            profile_name = profile_names[(sequence + stay_id) % len(profile_names)]

            event = make_event(
                stay_id,
                sample_time,
                rng,
                args.null_rate,
                args.hypotension_rate,
                profile_name,
            )
            payload = json.dumps(event, allow_nan=False, separators=(",", ":"))

            if args.dry_run:
                if sent == 0:
                    print(json.dumps(event, indent=2))
            else:
                producer.produce(
                    args.topic,
                    key=str(stay_id).encode("utf-8"),
                    value=payload.encode("utf-8"),
                    headers=build_headers(event),
                    on_delivery=delivery_handler,
                )
                producer.poll(0)

            sent += 1

            if args.rate > 0:
                deadline += tick
                delay = deadline - time.monotonic()
                if delay > 0:
                    time.sleep(delay)

            if sent >= next_progress:
                elapsed = time.monotonic() - started
                print(
                    f"Progress: {sent:,}/{args.total:,} sent, "
                    f"{delivered:,} acked, {failed:,} failed, "
                    f"{sent / elapsed:,.0f} events/s",
                    flush=True,
                )
                next_progress += args.progress_interval

    finally:
        if producer is not None:
            remaining = producer.flush(30)
            if remaining:
                print(f"WARNING: {remaining} messages were not flushed", file=sys.stderr)

    elapsed = time.monotonic() - started
    print(
        f"Done: sent={sent:,}, acked={delivered:,}, failed={failed:,}, "
        f"elapsed={elapsed:.2f}s, throughput={(sent / elapsed if elapsed else 0):,.0f} events/s"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
