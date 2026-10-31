"""Q_window: icu.alt.spark.telemetry -> 5-minute windows sliding 1 minute -> icu.alt.spark.vitals.5m.

Reads Spark's own wide telemetry, as Flink windows its wide stream (DECISIONS D20).
"""
import pandas as pd
from pyspark.sql import functions as F

from business_contract import FIELDS
from records import OUTPUT_SCHEMA, kafka_record
from spark_config import TOPICS, WATERMARK_DELAY, kafka_source, start
from window_logic import window_message

TELEMETRY_SCHEMA = "sample_time string, stay_id long, " + ", ".join(f"{f} double" for f in FIELDS.values())


def millis(value):
    # Session time zone is UTC, so Arrow hands over naive UTC timestamps.
    return int(pd.Timestamp(value).value // 1_000_000)


def format_windows(batches):
    for frame in batches:
        rows = []
        for item in frame.to_dict("records"):
            stats = {field: (item[f"n_{field}"], item[f"mean_{field}"], item[f"var_{field}"])
                     for field in FIELDS.values()}
            message = window_message(item["stay_id"], millis(item["window_start"]), millis(item["window_end"]), stats)
            if message is not None:
                rows.append(kafka_record(TOPICS["aggregates"], message))
        yield pd.DataFrame(rows, columns=["topic", "key", "value", "headers"])


def build(spark, config):
    telemetry = (kafka_source(spark, config, TOPICS["telemetry"])
                 .select(F.from_json(F.col("value").cast("string"), TELEMETRY_SCHEMA).alias("m"))
                 .select("m.*")
                 .withColumn("sample_ts", F.col("sample_time").cast("timestamp"))
                 .withWatermark("sample_ts", WATERMARK_DELAY))
    aggregates = [agg for field in FIELDS.values() for agg in (
        F.count(field).alias(f"n_{field}"), F.avg(field).alias(f"mean_{field}"),
        F.var_pop(field).alias(f"var_{field}"))]
    windows = (telemetry.groupBy(F.window("sample_ts", "5 minutes", "1 minute"), "stay_id").agg(*aggregates)
               .select(F.col("window.start").alias("window_start"), F.col("window.end").alias("window_end"),
                       "stay_id", *[F.col(f"{p}_{f}") for f in FIELDS.values() for p in ("n", "mean", "var")])
               .mapInPandas(format_windows, OUTPUT_SCHEMA))
    writer = (windows.writeStream.format("kafka").outputMode("append")
              .option("kafka.bootstrap.servers", config.bootstrap_servers)
              .option("kafka.acks", "all").option("kafka.linger.ms", "0"))
    return start(writer, config, "window")
