"""Storage contract and failure-ordering checks without a running database."""
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

PROJECT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(PROJECT / "services/storage_writer"), str(PROJECT / "scripts/demo")]

from config import GROUP_ID, TOPIC_TABLE, REFERENCE_TABLES, RUNTIME_TABLES
from db import REGISTRY, mapped_record, persist_one
from db_admin import reset_runtime
from writer import store_and_commit
import orchestrate


class FakeTransaction:
    def __init__(self, calls):
        self.calls = calls

    def __enter__(self):
        self.calls.append("begin")

    def __exit__(self, kind, error, trace):
        self.calls.append("rollback" if kind else "db_commit")


class FakeConnection:
    def __init__(self, fail=False):
        self.calls = []
        self.fail = fail

    def transaction(self):
        return FakeTransaction(self.calls)

    def execute(self, query, params=None):
        self.calls.append("insert")
        if self.fail:
            raise RuntimeError("database unavailable")
        if isinstance(query, str) and query.startswith("TRUNCATE"):
            return None
        return self


class FakeConsumer:
    def __init__(self, calls):
        self.calls = calls

    def commit(self, **kwargs):
        self.calls.append("offset_commit")


class FakeMessage:
    def topic(self):
        return "icu.v2.chartevents.cleaned"

    def key(self):
        return b"35727289"


class StorageTests(unittest.TestCase):
    def test_compose_and_schema_cover_exact_persistent_boundary(self):
        compose = (PROJECT / "docker/flink/compose.yaml").read_text()
        schema = (PROJECT / "docker/timescaledb/init/001_schema.sql").read_text()
        hypertables = (PROJECT / "docker/timescaledb/init/002_hypertables.sql").read_text()
        self.assertIn("timescale/timescaledb:2.30.2-pg17", compose)
        self.assertIn('127.0.0.1:5432:5432', compose)
        self.assertIn("icu-timescale-data", compose)
        self.assertIn("${POSTGRES_PASSWORD:?", compose)
        self.assertEqual(schema.count("CREATE TABLE IF NOT EXISTS"), 9)
        self.assertEqual(hypertables.count("SELECT create_hypertable"), 4)
        self.assertEqual(len(REFERENCE_TABLES), 5)
        self.assertEqual(len(RUNTIME_TABLES), 4)
        self.assertIn("REFERENCES icustays(stay_id)", schema)
        self.assertEqual(orchestrate.PORT_OWNERS[5432], "icu-timescaledb")

    def test_seed_and_subscription_are_fixed_to_current_four_patients(self):
        seed = json.loads((PROJECT / "config/db_reference_seed.json").read_text())
        self.assertEqual({r["stay_id"] for r in seed["icustays"]}, set(REGISTRY))
        self.assertEqual({int(r["itemid"]) for r in seed["d_items"]},
                         {220045, 220052, 220050, 220051, 220277, 220210})
        self.assertEqual({int(r["itemid"]) for r in seed["d_labitems"]}, {50813})
        self.assertEqual(GROUP_ID, "icu-timescale-writer-v2")
        self.assertEqual(set(TOPIC_TABLE.values()), set(RUNTIME_TABLES))
        self.assertEqual(len(TOPIC_TABLE), 4)

    def test_db_transaction_commits_before_kafka_offset(self):
        connection = FakeConnection()
        consumer = FakeConsumer(connection.calls)
        event = {"event_id": "x"}
        with patch("writer.persist_one", side_effect=lambda *args: persist_one(
                connection, "icu.v2.chartevents.cleaned", self._valid_event(), b"35727289")):
            store_and_commit(consumer, connection, FakeMessage(), event)
        self.assertEqual(connection.calls, ["begin", "insert", "db_commit", "offset_commit"])

    def test_failed_db_transaction_never_commits_kafka_offset(self):
        connection = FakeConnection(fail=True)
        consumer = FakeConsumer(connection.calls)
        with self.assertRaises(RuntimeError):
            store_and_commit(consumer, connection, FakeMessage(), self._valid_event())
        self.assertEqual(connection.calls, ["begin", "insert", "rollback"])

    def test_scoped_reset_preserves_references_and_keep_skips_truncate(self):
        connection = FakeConnection()
        with patch("db_admin.runtime_counts", return_value={name: 0 for name in RUNTIME_TABLES}):
            reset_runtime(connection)
            self.assertEqual(connection.calls, ["begin", "insert", "db_commit"])
            connection.calls.clear()
            reset_runtime(connection, keep=True)
            self.assertEqual(connection.calls, [])

    @staticmethod
    def _valid_event():
        patient = REGISTRY["35727289"]
        return {"event_time": "2026-10-17T00:00:00Z", "event_id": "x", "schema_version": "2.0",
                "subject_id": patient["subject_id"], "hadm_id": patient["hadm_id"],
                "stay_id": patient["stay_id"], "bed_id": patient["bed_id"],
                "itemid": 220045, "signal_name": "HR", "value": "80", "valuenum": 80,
                "valueuom": "bpm", "quality_status": "VALID", "ordering_status": "IN_ORDER",
                "processed_at": "2026-10-17T00:00:01Z"}


if __name__ == "__main__":
    unittest.main()
