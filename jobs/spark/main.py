"""Driver: ensure output topics, start the enabled queries, signal readiness, wait."""
import importlib
import os

from spark_config import READY_FILE, SparkConfig, build_session
from metrics_listener import JsonlProgressListener
from spark_topics import ensure_topics

# Query name -> module exposing build(spark, config). Later phases add the rest.
QUERY_MODULES = {"labs": "q_labs"}


def main():
    config = SparkConfig.from_env()
    if os.path.exists(READY_FILE):
        os.remove(READY_FILE)
    ensure_topics(config.bootstrap_servers)
    spark = build_session(config)
    spark.streams.addListener(JsonlProgressListener(config.metrics_root))
    enabled = [name for name in config.queries if name in QUERY_MODULES]
    skipped = [name for name in config.queries if name not in QUERY_MODULES]
    queries = [importlib.import_module(QUERY_MODULES[name]).build(spark, config) for name in enabled]
    print(f"[icu-spark] started={enabled} not_implemented={skipped} "
          f"master={config.master} shuffle_partitions={config.shuffle_partitions} "
          f"trigger={config.trigger or 'default'} fetch_max_wait_ms={config.fetch_max_wait_ms}", flush=True)
    with open(READY_FILE, "w", encoding="utf-8") as ready:
        ready.write(",".join(query.name for query in queries))
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
