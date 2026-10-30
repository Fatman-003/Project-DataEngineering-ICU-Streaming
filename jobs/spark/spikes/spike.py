"""Phase-0 spikes S1-S6 for the Spark alternative (run inside the icu-spark image).

Every spike writes its input with a Spark batch Kafka write, drives the
streaming query with processAllAvailable(), and prints JSON evidence lines
prefixed with "EVIDENCE" so the findings document can quote them.

    spark-submit jobs/spark/spikes/spike.py s1   # also covers S2, S3, S6
    spark-submit jobs/spark/spikes/spike.py s4
    spark-submit jobs/spark/spikes/spike.py s5
"""
import json
import os
import shutil
import statistics
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.streaming.state import GroupStateTimeout

from business_contract import encode, headers, validate
from business_outputs import telemetry
from contracts import parse_event
from event_time import event_timestamp_ms
from registry import load_patients
from validation import ValidationError, validate_event

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "icu-kafka:9094")
CHECKPOINTS = "/tmp/icu-spikes"
START = datetime(2026, 10, 16, 20, tzinfo=timezone.utc)
PATIENT = load_patients()[1]


def evidence(name, **values):
    print("EVIDENCE " + json.dumps({"spike": name, **values}, default=str), flush=True)


def session(shuffle_partitions=1):
    return (SparkSession.builder.master("local[1]").appName("icu-spikes")
            .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def chart(second, signal="MAP", value=62.0, suffix=""):
    event_time = (START + timedelta(seconds=second)).isoformat().replace("+00:00", "Z")
    itemid, unit = {"MAP": (220052, "mmHg"), "HR": (220045, "bpm"),
                    "SBP": (220050, "mmHg"), "SpO2": (220277, "%")}[signal]
    return {"schema_version": "3.0", "event_id": f"spike-{event_time}-{signal}{suffix}",
            **{field: PATIENT[field] for field in ("subject_id", "hadm_id", "stay_id", "bed_id")},
            "itemid": itemid, "signal_name": signal, "event_time": event_time,
            "valuenum": value, "valueuom": unit}


def produce(spark, topic, events):
    rows = [(str(event["stay_id"]), encode(event)) for event in events]
    spark.createDataFrame(rows, "key string, value string").write.format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP).option("topic", topic).save()


def kafka_stream(spark, topic):
    reader = spark.readStream.format("kafka")
    if os.environ.get("SPIKE_FETCH_MAX_WAIT_MS"):
        reader = reader.option("kafka.fetch.max.wait.ms", os.environ["SPIKE_FETCH_MAX_WAIT_MS"])
    return (reader.option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("subscribe", topic).option("startingOffsets", "earliest")
            .option("includeHeaders", "true").option("failOnDataLoss", "true").load())


def parse_validate(batches):
    """S1 chain step 1: the shared Flink parse/validate logic inside mapInPandas."""
    for frame in batches:
        rows = []
        for offset, value in zip(frame["offset"], frame["value"]):
            try:
                event = parse_event(bytes(value).decode("utf-8"))
                validate_event(event, "chartevents")
                status = "VALID"
                ts = pd.Timestamp(event_timestamp_ms(event), unit="ms")
            except (ValueError, ValidationError):
                status, event, ts = "REJECTED", {}, pd.Timestamp(0, unit="ms")
            rows.append((str(event.get("stay_id", "__invalid__")), ts, int(offset), status, encode(event)))
        yield pd.DataFrame(rows, columns=["stay_key", "event_ts", "offset", "status", "event_json"])


def observe(key, frames, state):
    """S2/S3/S6 probe: report which rows the stateful function actually sees."""
    watermark = state.getCurrentWatermarkMs()
    if state.hasTimedOut:
        pending = json.loads(state.get[0])
        state.remove()
        yield pd.DataFrame([{"stay_key": key[0], "kind": "TIMEOUT", "watermark_ms": watermark,
                             "detail": encode(pending)}])
        return
    seen = []
    for frame in frames:
        seen.extend(int(ts.value // 1_000_000) for ts in frame["event_ts"])
    pending = sorted(set(json.loads(state.get[0]) if state.exists else []) | set(seen))
    state.update((encode(pending),))
    if os.environ.get("SPIKE_TIMEOUT_AT_NEXT_WATERMARK"):
        # S3 boundary: exactly the watermark the next batch will compute (max - 2s).
        state.setTimeoutTimestamp(max(pending) - 2000)
    else:
        # Timeout at the oldest pending timestamp still ahead of the watermark (S3).
        state.setTimeoutTimestamp(min([t for t in pending if t > watermark] or [watermark + 1]))
    yield pd.DataFrame([{"stay_key": key[0], "kind": "CALL", "watermark_ms": watermark,
                         "detail": encode({"arrival_order_ms": seen})}])


def reset(name):
    shutil.rmtree(f"{CHECKPOINTS}/{name}", ignore_errors=True)


def stateful_query(spark, topic, name):
    parsed = kafka_stream(spark, topic).select("offset", "value").mapInPandas(
        parse_validate, "stay_key string, event_ts timestamp, offset long, status string, event_json string")
    out = (parsed.withWatermark("event_ts", "2 seconds").groupBy("stay_key").applyInPandasWithState(
        observe, "stay_key string, kind string, watermark_ms long, detail string", "pending string",
        "append", GroupStateTimeout.EventTimeTimeout))
    reset(name)
    return (out.writeStream.format("memory").queryName(name).outputMode("append")
            .option("checkpointLocation", f"{CHECKPOINTS}/{name}").start())


def drain(query, spark, name, label):
    query.processAllAvailable()
    rows = [row.asDict() for row in spark.sql(f"SELECT * FROM {name}").collect()]
    spark.sql(f"SELECT * FROM {name}").count()
    for progress in query.recentProgress:
        ops = progress.get("stateOperators", [{}])
        evidence(label, batchId=progress["batchId"], numInputRows=progress["numInputRows"],
                 watermark=progress.get("eventTime", {}).get("watermark"),
                 numRowsDroppedByWatermark=[op.get("numRowsDroppedByWatermark") for op in ops])
    return rows


def spike_s1(spark):
    topic = "icu.alt.spike.s1"
    origin = event_timestamp_ms(chart(0))
    query = stateful_query(spark, topic, "s1")
    # Batch A: seconds 0..9 deliberately shuffled (S6 evidence).
    produce(spark, topic, [chart(s) for s in (3, 0, 7, 1, 9, 2, 8, 4, 6, 5)])
    rows_a = drain(query, spark, "s1", "S1/S6 batch A")
    # Batch B: seconds 10..12 plus stale rows (S2 evidence) against watermark 7s:
    # 3s is clearly late, 7s sits exactly on the watermark, 7.5s is just after it.
    produce(spark, topic, [chart(s) for s in (10, 11, 12)]
            + [chart(3, suffix="-late"), chart(7, suffix="-edge"), chart(7.5, suffix="-after")])
    rows_b = drain(query, spark, "s1", "S2 batch B")
    # No more input: does a no-data batch fire the event-time timeout? (S3)
    time.sleep(3)
    rows_c = drain(query, spark, "s1", "S3 after idle")
    query.stop()
    for label, rows in (("A", rows_a), ("B", rows_b), ("C", rows_c)):
        for row in rows:
            detail = json.loads(row["detail"])
            if "arrival_order_ms" in detail:
                detail = {"arrival_order_s": [(t - origin) / 1000 for t in detail["arrival_order_ms"]]}
            else:
                detail = {"timed_out_pending_s": [(t - origin) / 1000 for t in detail]}
            wm = row["watermark_ms"]
            evidence(f"S1 rows after {label}", kind=row["kind"],
                     watermark_s=None if wm in (None, 0) else (wm - origin) / 1000, **detail)
    stats = [p for p in query.recentProgress]
    evidence("S3 batches", batches=[{"batchId": p["batchId"], "numInputRows": p["numInputRows"],
                                     "watermark": p.get("eventTime", {}).get("watermark")} for p in stats])


def to_business(batches):
    """S4: format one telemetry message per RAW MAP event and attach contract headers."""
    for frame in batches:
        keys, values, header_rows = [], [], []
        for value in frame["value"]:
            event = parse_event(bytes(value).decode("utf-8"))
            message = dict(telemetry(event, {"map": event["valuenum"]}), producer="icu-spark")
            keys.append(str(event["stay_id"]))
            values.append(encode(message))
            header_rows.append([{"key": name, "value": data} for name, data in headers(message)])
        yield pd.DataFrame({"key": keys, "value": values, "headers": header_rows})


def spike_s4(spark):
    source, sink = "icu.alt.spike.s4in", "icu.alt.spike.s4out"
    produce(spark, source, [chart(s, value=70.0) for s in range(5)])
    reset("s4")
    query = (kafka_stream(spark, source).select("value").mapInPandas(
        to_business, "key string, value string, headers array<struct<key:string,value:binary>>")
        .writeStream.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP).option("topic", sink)
        .option("checkpointLocation", f"{CHECKPOINTS}/s4").start())
    query.processAllAvailable()
    query.stop()
    read = (spark.read.format("kafka").option("kafka.bootstrap.servers", BOOTSTRAP).option("subscribe", sink)
            .option("startingOffsets", "earliest").option("includeHeaders", "true").load().collect())
    results = []
    for row in read:
        message = json.loads(row["value"].decode())
        kafka_headers = [(h["key"], bytes(h["value"])) for h in row["headers"] or []]
        try:
            validate(message, "icu.telemetry", bytes(row["key"]), kafka_headers, require_headers=True)
            results.append("OK")
        except ValueError as error:
            results.append(f"FAIL {error}")
    evidence("S4 validate", messages=len(read), results=results,
             header_names=[h[0] for h in [(x["key"], x["value"]) for x in read[0]["headers"]]] if read else [])


def spike_s5(spark_factory):
    for partitions in (1, 2, 200):
        spark = spark_factory(partitions)
        topic, name = f"icu.alt.spike.s5p{partitions}", f"s5p{partitions}"
        query = stateful_query(spark, topic, name)
        for batch in range(25):
            produce(spark, topic, [chart(batch * 4 + s) for s in range(4)])
            query.processAllAvailable()
        query.stop()
        # Skip the first five data batches (JIT, Python worker start-up).
        data = [p["durationMs"]["triggerExecution"] for p in query.recentProgress if p["numInputRows"]][5:]
        evidence("S5 batch duration", shuffle_partitions=partitions, batches_measured=len(data),
                 median_ms=statistics.median(data), p90_ms=sorted(data)[int(0.9 * (len(data) - 1))],
                 max_ms=max(data))
        spark.stop()


ROCKSDB = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"


def spike_s5b():
    """Where does a tiny batch spend its time? Stateless vs stateful, HDFS vs RocksDB state."""
    variants = os.environ.get("SPIKE_S5B_VARIANTS", "stateless,stateful-hdfs,stateful-rocksdb").split(",")
    suffix = os.environ.get("SPIKE_FETCH_MAX_WAIT_MS", "")
    for variant in variants:
        builder = (SparkSession.builder.master("local[1]").appName("icu-spikes")
                   .config("spark.sql.shuffle.partitions", "1").config("spark.ui.enabled", "false")
                   .config("spark.sql.session.timeZone", "UTC"))
        if variant == "stateful-rocksdb":
            builder = builder.config("spark.sql.streaming.stateStore.providerClass", ROCKSDB)
        spark = builder.getOrCreate()
        topic, name = f"icu.alt.spike.s5b{variant.replace('-', '')}", f"s5b{variant.replace('-', '')}{suffix}"
        if variant == "jvm-only":
            reset(name)
            query = (kafka_stream(spark, topic).selectExpr("offset", "CAST(value AS STRING) AS value")
                     .writeStream.format("memory").queryName(name)
                     .option("checkpointLocation", f"{CHECKPOINTS}/{name}").start())
        elif variant == "stateless":
            reset(name)
            query = (kafka_stream(spark, topic).select("offset", "value").mapInPandas(
                parse_validate, "stay_key string, event_ts timestamp, offset long, status string, event_json string")
                .writeStream.format("memory").queryName(name).option("checkpointLocation", f"{CHECKPOINTS}/{name}")
                .start())
        else:
            query = stateful_query(spark, topic, name)
        for batch in range(25):
            produce(spark, topic, [chart(batch * 4 + s) for s in range(4)])
            query.processAllAvailable()
        query.stop()
        data = [p["durationMs"] for p in query.recentProgress if p["numInputRows"]][5:]
        evidence("S5b breakdown", variant=variant, fetch_max_wait_ms=suffix or "default",
                 batches_measured=len(data),
                 median_ms={key: statistics.median(d.get(key, 0) for d in data) for key in sorted(data[0])})
        spark.stop()


def main():
    which = sys.argv[1]
    if which == "s5":
        spike_s5(session)
        return
    if which == "s5b":
        spike_s5b()
        return
    spark = session()
    spark.sparkContext.setLogLevel("WARN")
    {"s1": spike_s1, "s4": spike_s4}[which](spark)
    spark.stop()


if __name__ == "__main__":
    main()
