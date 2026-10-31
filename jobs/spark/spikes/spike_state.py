"""Phase-2 spikes S7-S9: rejected rows vs watermark, transformWithStateInPandas, dedup state cost.

    spark-submit jobs/spark/spikes/spike_state.py s7   # null event_ts rows (applyInPandasWithState)
    spark-submit jobs/spark/spikes/spike_state.py s8   # transformWithStateInPandas semantics
    spark-submit jobs/spark/spikes/spike_state.py s9   # dedup cost as the seen-id set grows

Needs the spikes folder on PYTHONPATH (reuses helpers from spike.py). S8/S9 also need
protobuf>=6.33.0 (the spike image added protobuf==6.33.6; the main image does not).
"""
import json
import os
import statistics
import sys

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StatefulProcessor
from pyspark.sql.streaming.state import GroupStateTimeout

from business_contract import encode
from event_time import event_timestamp_ms
from spike import CHECKPOINTS, chart, evidence, kafka_stream, produce, reset

ROCKSDB = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
PARSED = "stay_key string, event_ts timestamp, event_id string, status string"


def session(rocksdb=False):
    builder = (SparkSession.builder.master("local[1]").appName("icu-spikes-state")
               .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false")
               .config("spark.sql.session.timeZone", "UTC"))
    if rocksdb:
        builder = builder.config("spark.sql.streaming.stateStore.providerClass", ROCKSDB)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse(batches):
    """Rejected rows (event_id ending in -bad) get a NULL event_ts, as Q_wide would."""
    for frame in batches:
        rows = []
        for value in frame["value"]:
            event = json.loads(bytes(value).decode())
            bad = event["event_id"].endswith("-bad")
            rows.append(("__invalid__" if bad else str(event["stay_id"]),
                         None if bad else pd.Timestamp(event_timestamp_ms(event), unit="ms"),
                         event["event_id"], "REJECTED" if bad else "VALID"))
        yield pd.DataFrame(rows, columns=["stay_key", "event_ts", "event_id", "status"])


def parsed_stream(spark, topic):
    return (kafka_stream(spark, topic).select("value").mapInPandas(parse, PARSED)
            .withWatermark("event_ts", "2 seconds"))


def collect(query, spark, name, label, origin):
    query.processAllAvailable()
    for progress in query.recentProgress:
        ops = progress.get("stateOperators", [{}])
        evidence(label + " progress", batchId=progress["batchId"], numInputRows=progress["numInputRows"],
                 watermark=progress.get("eventTime", {}).get("watermark"),
                 numRowsDroppedByWatermark=[op.get("numRowsDroppedByWatermark") for op in ops])
    for row in spark.sql(f"SELECT * FROM {name}").collect():
        detail = json.loads(row["detail"])
        wm = row["watermark_ms"]
        evidence(label + " rows", kind=row["kind"], watermark_s=None if not wm or wm < 0 else (wm - origin) / 1000,
                 **{k: [(t - origin) / 1000 if isinstance(t, int) else t for t in v] for k, v in detail.items()})


# ---------------------------------------------------------------- S7

def observe_s7(key, frames, state):
    seen = []
    for frame in frames:
        for ts, event_id in zip(frame["event_ts"], frame["event_id"]):
            seen.append(event_id.split("-")[-1] if pd.isna(ts) else int(ts.value // 1_000_000))
    yield pd.DataFrame([{"stay_key": key[0], "kind": "CALL", "watermark_ms": state.getCurrentWatermarkMs(),
                         "detail": encode({"seen": seen})}])


def spike_s7():
    spark = session()
    topic, name = "icu.alt.spike.s7", "s7"
    origin = event_timestamp_ms(chart(0))
    reset(name)
    query = (parsed_stream(spark, topic).groupBy("stay_key").applyInPandasWithState(
        observe_s7, "stay_key string, kind string, watermark_ms long, detail string", "unused string",
        "append", GroupStateTimeout.EventTimeTimeout)
        .writeStream.format("memory").queryName(name).option("checkpointLocation", f"{CHECKPOINTS}/{name}").start())
    produce(spark, topic, [chart(s) for s in range(10)])
    query.processAllAvailable()
    # After watermark 7s: a NULL-timestamp rejected row plus new valid rows.
    produce(spark, topic, [chart(10), chart(4, suffix="-bad"), chart(11)])
    collect(query, spark, name, "S7", origin)
    query.stop()
    spark.stop()


# ---------------------------------------------------------------- S8

class Observe(StatefulProcessor):
    def init(self, handle):
        self.handle = handle
        self.pending = handle.getMapState("pending", "ts long", "seen boolean")  # TTL rejected in EventTime mode

    def handleInputRows(self, key, rows, timerValues):
        watermark = timerValues.getCurrentWatermarkInMs()
        seen = []
        for frame in rows:
            for ts in frame["event_ts"]:
                if pd.isna(ts):
                    seen.append("null")
                    continue
                ms = int(ts.value // 1_000_000)
                seen.append(ms)
                self.pending.updateValue((ms,), (True,))
        future = sorted(k[0] for k in self.pending.keys() if k[0] > watermark)
        boundary = os.environ.get("SPIKE_TIMER_AT_NEXT_WATERMARK")
        for timer in self.handle.listTimers():
            self.handle.deleteTimer(timer)
        if future:
            self.handle.registerTimer(max(future) - 2000 if boundary else future[0])
        yield pd.DataFrame([{"stay_key": key[0], "kind": "CALL", "watermark_ms": watermark,
                             "detail": encode({"seen": seen})}])

    def handleExpiredTimer(self, key, timerValues, expiredTimerInfo):
        watermark = timerValues.getCurrentWatermarkInMs()
        fired = expiredTimerInfo.getExpiryTimeInMs()
        done = sorted(k[0] for k in self.pending.keys() if k[0] < watermark)
        for ms in done:
            self.pending.removeKey((ms,))
        rest = sorted(k[0] for k in self.pending.keys())
        if rest:
            self.handle.registerTimer(rest[0])
        yield pd.DataFrame([{"stay_key": key[0], "kind": "TIMER", "watermark_ms": watermark,
                             "detail": encode({"fired_at": [fired], "finalized": done})}])

    def close(self):
        pass


def spike_s8():
    spark = session(rocksdb=True)
    topic = "icu.alt.spike.s8"
    name = "s8b" if os.environ.get("SPIKE_TIMER_AT_NEXT_WATERMARK") else "s8"
    origin = event_timestamp_ms(chart(0))
    reset(name)
    query = (parsed_stream(spark, topic).groupBy("stay_key").transformWithStateInPandas(
        Observe(), "stay_key string, kind string, watermark_ms long, detail string", "Append", "EventTime")
        .writeStream.format("memory").queryName(name).option("checkpointLocation", f"{CHECKPOINTS}/{name}").start())
    produce(spark, topic, [chart(s) for s in (3, 0, 7, 1, 9, 2, 8, 4, 6, 5)])
    query.processAllAvailable()
    produce(spark, topic, [chart(s) for s in (10, 11, 12)]
            + [chart(3, suffix="-late"), chart(7, suffix="-edge"), chart(7.5, suffix="-after"),
               chart(5, suffix="-bad")])
    query.processAllAvailable()
    collect(query, spark, name, "S8", origin)
    query.stop()
    spark.stop()


# ---------------------------------------------------------------- S9

def dedup_json(key, frames, state):
    seen = set(json.loads(state.get[0])) if state.exists else set()
    duplicates = 0
    for frame in frames:
        for event_id in frame["event_id"]:
            if event_id in seen:
                duplicates += 1
            seen.add(event_id)
    state.update((encode(sorted(seen)),))
    yield pd.DataFrame([{"stay_key": key[0], "duplicates": duplicates, "state_size": len(seen)}])


class DedupMap(StatefulProcessor):
    def init(self, handle):
        self.seen = handle.getMapState("seen", "event_id string", "ts long", 24 * 3600 * 1000)

    def handleInputRows(self, key, rows, timerValues):
        duplicates = 0
        for frame in rows:
            for event_id, ts in zip(frame["event_id"], frame["event_ts"]):
                if self.seen.containsKey((event_id,)):
                    duplicates += 1
                else:
                    self.seen.updateValue((event_id,), (int(ts.value // 1_000_000),))
        yield pd.DataFrame([{"stay_key": key[0], "duplicates": duplicates, "state_size": -1}])

    def close(self):
        pass


def spike_s9():
    batch_size = int(os.environ.get("SPIKE_S9_BATCH", "2000"))
    batches = int(os.environ.get("SPIKE_S9_BATCHES", "20"))
    for variant in os.environ.get("SPIKE_S9_VARIANTS", "apply-json,tws-mapstate").split(","):
        spark = session(rocksdb=variant.startswith("tws"))
        topic, name = f"icu.alt.spike.s9{variant.replace('-', '')}", f"s9{variant.replace('-', '')}"
        reset(name)
        grouped = parsed_stream(spark, topic).groupBy("stay_key")
        schema = "stay_key string, duplicates long, state_size long"
        if variant == "apply-json":
            out = grouped.applyInPandasWithState(dedup_json, schema, "seen string", "append",
                                                 GroupStateTimeout.NoTimeout)
        else:
            out = grouped.transformWithStateInPandas(DedupMap(), schema, "Append", "ProcessingTime")
        query = (out.writeStream.format("memory").queryName(name)
                 .option("checkpointLocation", f"{CHECKPOINTS}/{name}").start())
        for batch in range(batches):
            # One patient, distinct ids (10 ms apart) plus one repeated id per batch.
            events = [chart((batch * batch_size + i) / 100, signal="HR") for i in range(batch_size)]
            produce(spark, topic, events + [events[0]])
            query.processAllAvailable()
            last = [p for p in query.recentProgress if p["numInputRows"]][-1]
            evidence("S9 batch", variant=variant, batch=batch, ids_in_state=(batch + 1) * batch_size,
                     rows=last["numInputRows"], trigger_ms=last["durationMs"]["triggerExecution"])
        query.stop()
        durations = [p["durationMs"]["triggerExecution"] for p in query.recentProgress if p["numInputRows"]]
        duplicates = sum(row["duplicates"] for row in spark.sql(f"SELECT * FROM {name}").collect())
        evidence("S9 dedup cost", variant=variant, batch_size=batch_size, batches=len(durations),
                 duplicates_found=duplicates, first5_median_ms=statistics.median(durations[1:6]),
                 last5_median_ms=statistics.median(durations[-5:]), all_ms=durations)
        spark.stop()


if __name__ == "__main__":
    {"s7": spike_s7, "s8": spike_s8, "s9": spike_s9}[sys.argv[1]]()
