"""Q_wide business logic, engine-free: dedup -> event-time class -> LONG-to-WIDE.

Reproduces, per stay, the Flink chain Deduplicate -> ClassifyTime -> Assemble
(`dedup.py`, `event_time.py`, `business_outputs.flink_telemetry`) using the
shared helpers. Storage goes through small map/value interfaces so the same
code runs on Spark state and on in-memory fixtures in unit tests.

Watermarks: Spark's watermark is `max - 2000 ms`; Flink's is one millisecond
lower. `flink_watermark(spark_wm)` converts, so `classify_event` and timer
firing (`T <= flink_wm`, i.e. `T < spark_wm`) follow Flink's conditions exactly.
"""
import json
from business_contract import FIELDS, encode
from business_outputs import telemetry
from dedup import dedup_identity
from dlq import rejection
from event_time import classify_event, event_timestamp_ms
from validation import parse_timestamp

PRODUCER = "icu-spark"
NO_WATERMARK = -(2 ** 63)


def flink_watermark(spark_watermark_ms):
    """Spark reports 0 (or less) before its first watermark; Flink starts at Long.MIN_VALUE."""
    return NO_WATERMARK if spark_watermark_ms is None or spark_watermark_ms <= 0 else spark_watermark_ms - 1


def spark_telemetry(event, values):
    return dict(telemetry(event, values), producer=PRODUCER)


class MemoryMap:
    """Map-state interface (also the in-memory store for whole-object state)."""

    def __init__(self, items=None):
        self.items = dict(items or {})

    def get(self, key):
        return self.items.get(key)

    def put(self, key, value):
        self.items[key] = value

    def remove(self, key):
        self.items.pop(key, None)

    def contains(self, key):
        return key in self.items

    def keys(self):
        return list(self.items)


class MemoryValue:
    def __init__(self, value=None):
        self.value = value

    def get(self):
        return self.value

    def put(self, value):
        self.value = value


class WideLogic:
    """One stay's operator. Outputs are ("TELEMETRY" | "DLQ", dict) pairs."""

    def __init__(self, seen, pending, maximum):
        self.seen, self.pending, self.maximum = seen, pending, maximum

    def process(self, rows, spark_watermark_ms):
        """rows: dicts with status, offset, source, event (VALID) or dlq (REJECTED)."""
        watermark = flink_watermark(spark_watermark_ms)
        outputs = []
        # Flink handles one partition in arrival order; Spark gives no order guarantee (S6).
        for row in sorted(rows, key=lambda item: item["offset"]):
            if row["status"] == "REJECTED":
                outputs.append(("DLQ", row["dlq"]))
            else:
                outputs.extend(self.accept(row["source"], row["event"], watermark))
        outputs.extend(self.finalize(spark_watermark_ms))
        return outputs

    def accept(self, source, event, watermark):
        identity = dedup_identity(event, "chartevents")
        if self.seen.contains(identity):
            return [("DLQ", rejection(source, "chartevents", "DUPLICATE_EVENT",
                                      "Event identity already seen within processing-time state TTL", event))]
        timestamp = event_timestamp_ms(event)
        self.seen.put(identity, timestamp)
        maximum = self.maximum.get()
        if classify_event(timestamp, watermark, maximum) == "LATE_EVENT":
            # Unreachable on Spark (rows <= watermark are dropped first, S2); kept for parity.
            return [("DLQ", rejection(source, "chartevents", "LATE_EVENT",
                                      f"event_time_ms={timestamp} <= effective_watermark_ms={watermark}", event))]
        if maximum is None or timestamp > maximum:
            self.maximum.put(timestamp)
        return self.assemble(source, event, timestamp)

    def assemble(self, source, event, timestamp):
        field = FIELDS.get(event["signal_name"])
        if field is None:
            return []
        stored = self.pending.get(timestamp)
        samples = {} if stored is None else json.loads(stored)
        sample_time = parse_timestamp(event["event_time"]).isoformat(timespec="microseconds")
        bucket = samples.setdefault(sample_time, {"event": event, "values": {}, "emitted": False})
        values = bucket["values"]
        if bucket["emitted"]:
            if values.get(field) != event["valuenum"]:
                return [("DLQ", rejection(source, "chartevents", "IDEMPOTENCY_CONFLICT",
                                          "Finalized wide sample cannot be corrected", event))]
            return []
        outputs = []
        values[field] = min(values.get(field, event["valuenum"]), event["valuenum"])
        if len(values) == len(FIELDS):
            bucket["emitted"] = True
            outputs.append(("TELEMETRY", spark_telemetry(event, values)))
        self.pending.put(timestamp, encode(samples))
        return outputs

    def finalize(self, spark_watermark_ms):
        """Flink's on_timer for every pending timestamp T with T <= flink watermark."""
        watermark = flink_watermark(spark_watermark_ms)
        outputs = []
        for timestamp in sorted(key for key in self.pending.keys() if key <= watermark):
            stored = self.pending.get(timestamp)
            self.pending.remove(timestamp)
            for _, bucket in sorted(json.loads(stored).items()):
                if not bucket["emitted"] and bucket["values"]:
                    outputs.append(("TELEMETRY", spark_telemetry(bucket["event"], bucket["values"])))
        return outputs

    def next_timer(self):
        """Earliest pending timestamp, or None. Fires once the Spark watermark exceeds it."""
        keys = self.pending.keys()
        return min(keys) if keys else None

    def expire_seen(self, spark_watermark_ms, ttl_ms):
        expire_seen(self.seen, spark_watermark_ms, ttl_ms)


def expire_seen(seen, spark_watermark_ms, ttl_ms):
    """Event-time stand-in for Flink's 24 h processing-time TTL (DECISIONS D11)."""
    horizon = flink_watermark(spark_watermark_ms) - ttl_ms
    for identity in [key for key in seen.keys() if seen.get(key) < horizon]:
        seen.remove(identity)
