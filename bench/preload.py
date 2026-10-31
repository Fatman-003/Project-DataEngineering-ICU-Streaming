"""E3 preload: write HF events to RAW as fast as Kafka accepts them (no pacing).

    python bench/preload.py --frequency 200 --minutes 10 --start-time 2026-10-16T20:00:00Z

Uses the deterministic high_frequency_events generator; prints one JSON summary line.
"""
import argparse
import json
import sys
import time
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
for folder in ("jobs/flink", "scripts/demo"):
    sys.path.insert(0, str(PROJECT / folder))
from contracts import stay_key                      # noqa: E402
from dlq import encode                              # noqa: E402
from high_frequency import high_frequency_events    # noqa: E402
from validation import parse_timestamp              # noqa: E402


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--frequency", type=int, default=200)
    parser.add_argument("--minutes", type=float, default=10)
    parser.add_argument("--start-time", required=True)
    parser.add_argument("--bootstrap", default="127.0.0.1:9092")
    args = parser.parse_args()
    from confluent_kafka import Producer
    producer = Producer({"bootstrap.servers": args.bootstrap, "enable.idempotence": True, "acks": "all",
                         "linger.ms": 20, "batch.size": 1048576, "compression.type": "none",
                         "queue.buffering.max.messages": 1000000, "queue.buffering.max.kbytes": 1048576})
    failures, counts, began = [], {}, time.perf_counter()
    for topic, event in high_frequency_events(args.frequency, args.minutes, parse_timestamp(args.start_time)):
        while True:
            try:
                producer.produce(topic, key=stay_key(event).encode(), value=encode(event).encode(),
                                 on_delivery=lambda error, _: error and failures.append(str(error)))
                break
            except BufferError:
                producer.poll(0.05)
        counts[topic] = counts.get(topic, 0) + 1
        if sum(counts.values()) % 50000 == 0:
            producer.poll(0)
    remaining = producer.flush(120)
    elapsed = time.perf_counter() - began
    print(json.dumps({"counts": counts, "total": sum(counts.values()), "seconds": elapsed,
                      "events_per_second": sum(counts.values()) / elapsed, "undelivered": remaining,
                      "failures": failures[:3]}))
    return 1 if remaining or failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
