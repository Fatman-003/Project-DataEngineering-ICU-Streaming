"""One-shot bootstrap: create the application topics, then seed reference data.

Runs inside the icu-init container after Kafka and TimescaleDB are healthy and
the Spring server has started (Flyway owns the schema; this only waits for the
tables and fills the project reference seed).
"""
import json
import os
import sys

sys.path.insert(0, "/app/scripts/demo")

from kafka_topics import ensure_topics  # noqa: E402  (also loads jobs/flink)
import seed_sources  # noqa: E402


def main():
    print("Ensuring Kafka topics...", flush=True)
    topics = ensure_topics("icu-kafka:9094", timeout=180)
    print(f"Kafka topics ready: {', '.join(topics)}", flush=True)

    class Args:
        host = "icu-db"
        port = 5432
        dbname = os.environ["POSTGRES_DB"]
        user = os.environ["POSTGRES_USER"]
        password = os.environ["POSTGRES_PASSWORD"]
        seed_file = seed_sources.DEFAULT_SEED
        wait = True
        wait_timeout = 300

    seed = json.loads(Args.seed_file.read_text(encoding="utf-8"))
    seed_sources.wait_for_tables(Args, seed_sources.missing_tables(seed_sources.connect(Args)))

    with seed_sources.connect(Args) as connection:
        counts = {
            "patients": seed_sources.seed_patients(connection, seed["patients"]),
            "admissions": seed_sources.seed_admissions(connection, seed["admissions"]),
            "icustays": seed_sources.seed_icustays(connection, seed["icustays"]),
            "d_items": seed_sources.seed_d_items(connection, seed["d_items"]),
            "d_labitems": seed_sources.seed_d_labitems(connection, seed["d_labitems"]),
            "labevents": seed_sources.seed_labevents(connection, seed),
        }

    print("Seed complete:", ", ".join(f"{t}={n}" for t, n in counts.items()), flush=True)


if __name__ == "__main__":
    main()
