"""The two storage modules behind one interface, for the telemetry table.

TimescaleDB = the repo as it is: repo schema (server Flyway migrations) and the repo's INSERT (db.insert_sql).
Cassandra = a query-first port of the same table.

A record is a dict in the repo's column order: sample_time, stay_id, heart_rate, map, sbp, spo2.
"""
import os
from datetime import timedelta, timezone

from dbbench import REPO  # noqa: F401  (sets sys.path to the repo modules)
import psycopg
from db import insert_sql

TELEMETRY = "patient_telemetry_stream"
UTC = timezone.utc


def days_between(start, end):
    day, last = start.date(), (end - timedelta(microseconds=1)).date()
    out = []
    while day <= last:
        out.append(day)
        day += timedelta(days=1)
    return out


# --------------------------------------------------------------------------------------------- TimescaleDB
class TimescaleStore:
    def __init__(self):
        self.conn = psycopg.connect(host=os.environ.get("TS_HOST", "timescaledb"),
                                    port=int(os.environ.get("TS_PORT", "5432")), dbname="icu", user="icu_app",
                                    password="icu-dbbench-local", autocommit=True, connect_timeout=5,
                                    # Plan every query with its real values: a server-side prepared (generic) plan
                                    # cannot exclude chunks at plan time and locks all 720 one-hour chunks, which
                                    # exhausts the lock table with 8 concurrent clients.
                                    prepare_threshold=None)

    def close(self):
        self.conn.close()

    def setup(self, patients):
        """Reference rows the telemetry foreign key needs (stay_id -> icustays -> admissions, patients)."""
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.executemany("INSERT INTO patients (subject_id) VALUES (%s) ON CONFLICT DO NOTHING",
                            [(p["subject_id"],) for p in patients])
            cur.executemany("INSERT INTO admissions (hadm_id, subject_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                            [(p["hadm_id"], p["subject_id"]) for p in patients])
            cur.executemany("INSERT INTO icustays (stay_id, subject_id, hadm_id, bed_id) VALUES (%s, %s, %s, %s) "
                            "ON CONFLICT DO NOTHING",
                            [(p["stay_id"], p["subject_id"], p["hadm_id"], p["bed_id"]) for p in patients])

    def reset(self):
        self.conn.execute(f"TRUNCATE {TELEMETRY}")

    def write_one(self, record):
        self.conn.execute(insert_sql(TELEMETRY), list(record.values()))

    def write_batch(self, records):
        """One transaction per batch: executemany (libpq pipeline) of the repo INSERT ... ON CONFLICT DO NOTHING."""
        with self.conn.transaction(), self.conn.cursor() as cur:
            cur.executemany(insert_sql(TELEMETRY), [list(r.values()) for r in records])

    # -- reads
    def q_point_1h(self, stay, ctx):
        return self.conn.execute(f"SELECT sample_time, heart_rate, map, sbp, spo2 FROM {TELEMETRY} WHERE stay_id=%s "
                                 f"AND sample_time >= %s AND sample_time < %s ORDER BY sample_time DESC",
                                 (stay, ctx["end"] - timedelta(hours=1), ctx["end"])).fetchall()

    def q_avg_hr_30d(self, stay, ctx):
        return self.conn.execute(f"SELECT avg(heart_rate) FROM {TELEMETRY} WHERE stay_id=%s AND sample_time >= %s "
                                 f"AND sample_time < %s", (stay, ctx["end"] - timedelta(days=30), ctx["end"])).fetchall()


# ----------------------------------------------------------------------------------------------- Cassandra
CQL_SCHEMA = [
    "CREATE KEYSPACE IF NOT EXISTS icu WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
    # One partition per patient per UTC day: bounded partitions, time-ordered inside.
    # sample_us keeps the microsecond part (CQL timestamp is milliseconds; the contract allows microseconds).
    """CREATE TABLE IF NOT EXISTS icu.patient_telemetry_stream (
        stay_id bigint, day date, sample_time timestamp, sample_us smallint,
        heart_rate double, map double, sbp double, spo2 double,
        PRIMARY KEY ((stay_id, day), sample_time, sample_us)
    ) WITH CLUSTERING ORDER BY (sample_time DESC, sample_us DESC)
      AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'HOURS',
                        'compaction_window_size': 6}""",
]


def cassandra_session(retries=60):
    import time
    from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
    from cassandra.policies import DCAwareRoundRobinPolicy
    from cassandra import ConsistencyLevel
    try:
        from cassandra.io.libevreactor import LibevConnection as connection_class
    except ImportError:  # pragma: no cover
        from cassandra.io.asyncioreactor import AsyncioConnection as connection_class
    profile = ExecutionProfile(load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="dc1"),
                               consistency_level=ConsistencyLevel.LOCAL_QUORUM, request_timeout=30)
    last = None
    for _ in range(retries):
        try:
            cluster = Cluster([os.environ.get("CASS_HOST", "cassandra")], port=int(os.environ.get("CASS_PORT", "9042")),
                              execution_profiles={EXEC_PROFILE_DEFAULT: profile}, protocol_version=5,
                              connection_class=connection_class)
            return cluster, cluster.connect()
        except Exception as error:  # noqa: BLE001  (node still booting)
            last = error
            time.sleep(2)
    raise RuntimeError(f"Cassandra not reachable: {last}")


class CassandraStore:
    def __init__(self, concurrency=128):
        self.concurrency = concurrency
        self.cluster, self.session = cassandra_session()
        self.prepared = False

    def close(self):
        self.cluster.shutdown()

    def setup(self):
        for statement in CQL_SCHEMA:
            self.session.execute(statement, timeout=120)

    def prepare(self):
        if self.prepared:
            return
        p = self.session.prepare
        self.insert = p("INSERT INTO icu.patient_telemetry_stream (stay_id, day, sample_time, sample_us, heart_rate, "
                        "map, sbp, spo2) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        self.range = p("SELECT sample_time, sample_us, heart_rate, map, sbp, spo2 FROM icu.patient_telemetry_stream "
                       "WHERE stay_id=? AND day=? AND sample_time >= ? AND sample_time < ?")
        # avg() is per partition only, so the 30-day average = sum/count over the 30-31 day partitions.
        self.hr_sum = p("SELECT sum(heart_rate), count(heart_rate) FROM icu.patient_telemetry_stream "
                        "WHERE stay_id=? AND day=? AND sample_time >= ? AND sample_time < ?")
        self.prepared = True

    def reset(self):
        self.session.execute(f"TRUNCATE icu.{TELEMETRY}", timeout=120)

    @staticmethod
    def params(r):
        st = r["sample_time"].astimezone(UTC)
        return (r["stay_id"], st.date(), st.replace(microsecond=st.microsecond // 1000 * 1000), st.microsecond % 1000,
                r["heart_rate"], r["map"], r["sbp"], r["spo2"])

    def write_one(self, record):
        self.session.execute(self.insert, self.params(record))

    def write_batch(self, records):
        """Independent single-partition writes in flight at once (Cassandra batches across partitions are an
        anti-pattern); all acknowledged before the call returns."""
        from cassandra.concurrent import execute_concurrent
        execute_concurrent(self.session, [(self.insert, self.params(r)) for r in records],
                           concurrency=self.concurrency, raise_on_first_error=True, results_generator=False)

    # -- reads
    def q_point_1h(self, stay, ctx):
        start, end = ctx["end"] - timedelta(hours=1), ctx["end"]
        rows = []
        for day in reversed(days_between(start, end)):
            rows.extend(self.session.execute(self.range, (stay, day, start, end)))
        return rows

    def q_avg_hr_30d(self, stay, ctx):
        """One aggregate per day partition (no cross-partition GROUP BY), fanned out in parallel, combined here."""
        from cassandra.concurrent import execute_concurrent_with_args
        start, end = ctx["end"] - timedelta(days=30), ctx["end"]
        args = [(stay, day, start, end) for day in days_between(start, end)]
        total, count = 0.0, 0
        for result in execute_concurrent_with_args(self.session, self.hr_sum, args, concurrency=32,
                                                   raise_on_first_error=True):
            row = result.result_or_exc[0]
            total += row[0] or 0.0
            count += row[1]
        return [(total / count if count else None,)]


def make_store(label):
    """Store for one benchmark target. Cassandra statements are prepared here (keyspace made by `setup`)."""
    if label == "timescaledb":
        return TimescaleStore()
    if label == "cassandra":
        store = CassandraStore()
        store.prepare()
        return store
    raise ValueError(label)
