"""Environment-driven settings and the SparkSession shared by every ICU query."""
import os
from dataclasses import dataclass

TOPICS = {"telemetry": "icu.alt.spark.telemetry", "aggregates": "icu.alt.spark.vitals.5m",
          "alerts": "icu.alt.spark.alerts", "deadletter": "icu.alt.spark.deadletter"}
WATERMARK_DELAY = "2 seconds"
READY_FILE = "/tmp/icu-spark-ready"


@dataclass(frozen=True)
class SparkConfig:
    bootstrap_servers: str = "icu-kafka:9094"
    master: str = "local[1]"
    shuffle_partitions: int = 1
    trigger: str = ""
    fetch_max_wait_ms: int = 10
    checkpoint_root: str = "/opt/icu/checkpoints"
    metrics_root: str = "/opt/icu/metrics"
    queries: tuple = ("wide", "alert", "labs", "window")

    @classmethod
    def from_env(cls):
        env = os.environ.get
        config = cls(bootstrap_servers=env("KAFKA_BOOTSTRAP_SERVERS", cls.bootstrap_servers),
                     master=env("SPARK_MASTER", cls.master),
                     shuffle_partitions=int(env("SPARK_SHUFFLE_PARTITIONS", cls.shuffle_partitions)),
                     trigger=env("SPARK_TRIGGER", cls.trigger).strip(),
                     fetch_max_wait_ms=int(env("SPARK_KAFKA_FETCH_MAX_WAIT_MS", cls.fetch_max_wait_ms)),
                     checkpoint_root=env("SPARK_CHECKPOINT_ROOT", cls.checkpoint_root),
                     metrics_root=env("SPARK_METRICS_ROOT", cls.metrics_root),
                     queries=tuple(name.strip() for name in env("SPARK_QUERIES", ",".join(cls.queries)).split(",")
                                   if name.strip()))
        config.validate()
        return config

    def validate(self):
        if not self.bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be nonempty")
        if self.shuffle_partitions <= 0 or self.fetch_max_wait_ms < 0:
            raise ValueError("Shuffle partitions must be positive and fetch wait nonnegative")
        unknown = set(self.queries) - set(SparkConfig.queries)
        if unknown:
            raise ValueError(f"Unknown SPARK_QUERIES: {sorted(unknown)}")

    def checkpoint(self, query):
        return f"{self.checkpoint_root}/{query}"


def build_session(config):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master(config.master).appName("icu-spark-alternative")
             # Fixed at checkpoint creation; changing it requires deleting checkpoints.
             .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.streaming.noDataMicroBatches.enabled", "true")
             .config("spark.ui.port", "4040")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def kafka_source(spark, config, topic):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("includeHeaders", "true")
            .option("kafka.fetch.max.wait.ms", str(config.fetch_max_wait_ms))
            .load())


def start(writer, config, name):
    writer = writer.queryName(name).option("checkpointLocation", config.checkpoint(name))
    if config.trigger:
        writer = writer.trigger(processingTime=config.trigger)
    return writer.start()
