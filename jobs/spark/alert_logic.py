"""Q_alert business logic, engine-free: the Flink hypotension operator per stay.

Mirrors `hypotension.flink_hypotension` (process_element / on_timer) with the
shared helpers `bucket_add`, `median_map` and `advance_episode`, preceded by
the dedup and late checks that Flink runs upstream. Timers are emulated as in
wide_logic: a 1 s bucket's timer `T = start + 999` fires once Flink's
watermark reaches T, i.e. once Spark's watermark exceeds T (DECISIONS D8).
"""
import json
from business_contract import encode
from dedup import dedup_identity
from event_time import classify_event, event_timestamp_ms
from hypotension import CONTEXT_SIGNALS, advance_episode, bucket_add, median_map
from runtime_config import MAP_BUCKET_SECONDS, WindowAlertConfig
from window_aggregates import iso_timestamp
from wide_logic import flink_watermark

BUCKET_MS = MAP_BUCKET_SECONDS * 1000
SIGNALS = ("MAP", *CONTEXT_SIGNALS)


class AlertLogic:
    """One stay. `pending` maps bucket start -> JSON bucket; `context` maps "start:signal" -> JSON samples."""

    def __init__(self, seen, pending, context, episode, config=None):
        self.seen, self.pending, self.context, self.episode = seen, pending, context, episode
        self.config = config or WindowAlertConfig()

    def process(self, rows, spark_watermark_ms):
        watermark = flink_watermark(spark_watermark_ms)
        for row in sorted(rows, key=lambda item: item["offset"]):
            if row["status"] == "VALID":
                self.accept(row["event"], watermark)
        return self.finalize(spark_watermark_ms)

    def accept(self, event, watermark, deduplicate=True):
        """Returns ACCEPTED, IGNORED, DUPLICATE_EVENT or LATE_EVENT (Q_wide owns the DLQ).

        `deduplicate=False` feeds the operator directly, like Flink's process_element
        without the upstream Deduplicate operator (used by the reused unit tests).
        """
        signal = event["signal_name"]
        if signal not in SIGNALS:
            return "IGNORED"
        timestamp = event_timestamp_ms(event)
        if deduplicate:
            identity = dedup_identity(event, "chartevents")
            if self.seen.contains(identity):
                return "DUPLICATE_EVENT"
            self.seen.put(identity, timestamp)
        if classify_event(timestamp, watermark, None) == "LATE_EVENT":
            return "LATE_EVENT"
        start = timestamp // BUCKET_MS * BUCKET_MS
        if signal == "MAP":
            stored = self.pending.get(start)
            bucket = ({"samples": {}, "event": event, "first_ms": timestamp, "last_ms": timestamp}
                      if stored is None else json.loads(stored))
            bucket["first_ms"] = min(bucket["first_ms"], timestamp)
            bucket["last_ms"] = max(bucket["last_ms"], timestamp)
            bucket_add(bucket["samples"], event)
            self.pending.put(start, encode(bucket))
        else:
            key = f"{start}:{signal}"
            stored = self.context.get(key)
            self.context.put(key, encode(bucket_add({} if stored is None else json.loads(stored), event)))
        return "ACCEPTED"

    def starts(self):
        return set(self.pending.keys()) | {int(key.split(":", 1)[0]) for key in self.context.keys()}

    def finalize(self, spark_watermark_ms):
        watermark = flink_watermark(spark_watermark_ms)
        outputs = []
        for start in sorted(self.starts()):
            if start + BUCKET_MS - 1 > watermark:
                break
            outputs.extend(self.on_timer(start))
        return outputs

    def on_timer(self, start):
        context = {}
        for signal in CONTEXT_SIGNALS:
            key = f"{start}:{signal}"
            stored = self.context.get(key)
            if stored is not None:
                self.context.remove(key)
                context[signal] = median_map(json.loads(stored))
        stored = self.pending.get(start)
        # Secondary-only buckets never advance MAP evidence, continuity or recovery.
        if stored is None:
            return []
        self.pending.remove(start)
        bucket = json.loads(stored)
        event = dict(bucket["event"], event_time=iso_timestamp(start), valuenum=median_map(bucket["samples"]))
        state = self.episode.get()
        state, outputs = advance_episode(None if state is None else json.loads(state),
                                         {"event": event, "first_observation_ms": bucket["first_ms"],
                                          "last_observation_ms": bucket["last_ms"], "context": context}, self.config)
        self.episode.put(None if state is None else encode(state))
        return outputs

    def next_timer(self):
        starts = self.starts()
        return min(starts) + BUCKET_MS - 1 if starts else None
