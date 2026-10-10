"""Bounded manual verification consumer; never commits offsets."""
import argparse
import json
import time
import uuid
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition
from config import TOPICS, settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic", required=True, choices=list(TOPICS.values()))
    parser.add_argument("--max-messages", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=15,
                        help="Total runtime bound, including waits (seconds)")
    parser.add_argument("--offset", default="earliest",
                        help="earliest, latest, or explicit partition-0 offset")
    args = parser.parse_args()
    if args.max_messages <= 0 or args.timeout <= 0:
        parser.error("max-messages and timeout must be positive")
    if args.offset not in ("earliest", "latest"):
        try:
            if int(args.offset) < 0:
                raise ValueError()
        except ValueError:
            parser.error("offset must be earliest, latest, or a nonnegative integer")
    consumer = Consumer({
        "bootstrap.servers": settings()["bootstrap"],
        "group.id": "icu-raw-verify-" + uuid.uuid4().hex,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "socket.timeout.ms": 10000,
    })
    count = 0
    started = time.monotonic()
    error = None
    try:
        partition = TopicPartition(args.topic, 0)
        low, high = consumer.get_watermark_offsets(partition, timeout=min(args.timeout, 10))
        offset = low if args.offset == "earliest" else high if args.offset == "latest" else int(args.offset)
        consumer.assign([TopicPartition(args.topic, 0, offset)])
        while count < args.max_messages and time.monotonic() - started < args.timeout:
            msg = consumer.poll(max(0, min(0.5, args.timeout - (time.monotonic() - started))))
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            payload = json.loads(msg.value())
            key = msg.key().decode("utf-8") if msg.key() else None
            if key != str(payload.get("stay_id")):
                raise ValueError("Kafka key does not equal stay_id")
            fields = ("stay_id", "event_id", "event_time", "producer_time",
                      "signal_name", "event_type", "value", "available_time",
                      "availability_status", "validation_flags", "source_event_time",
                      "quality_flag", "replay_time_basis", "replay_session_id")
            result = {field: payload.get(field) for field in fields}
            result.update(topic=msg.topic(), partition=msg.partition(),
                          offset=msg.offset(), key=key)
            if "samples" in payload:
                result.update(sample_count=len(payload["samples"]),
                              missing_sample_count=payload.get("missing_sample_count"),
                              start_time=payload["start_time"], end_time=payload["end_time"],
                              sampling_rate_hz=payload["sampling_rate_hz"])
            print(json.dumps(result, ensure_ascii=False), flush=True)
            count += 1
    except KeyboardInterrupt:
        error = "Interrupted"
    except Exception as exc:
        error = str(exc)
    finally:
        consumer.close()
    print(json.dumps({"consumed": count, "requested": args.max_messages, "error": error,
                      "elapsed_seconds": round(time.monotonic() - started, 3)}))
    return 0 if count == args.max_messages and error is None else 1


if __name__ == "__main__":
    raise SystemExit(main())
