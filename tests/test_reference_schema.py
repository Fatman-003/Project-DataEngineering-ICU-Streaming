"""Integer ICU beds across RAW, API and transactional reference migrations.

Real PostgreSQL tests opt in with ICU_RUN_DATABASE_TESTS=1. They create and
remove only a uniquely named disposable DB, never the project's ICU database.
"""
import base64
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / p) for p in ("jobs/flink", "scripts/demo", "services/realtime_gateway",
                                       "services/storage_writer", "scripts/reference")]
import db_admin
from registry import load_patients, source_root
from contracts import RAW_SCHEMA_VERSION, TOPICS
from validation import validate_event, ValidationError
from runtime_pipeline import parse_and_validate
from run_icu_demo import events_at, DEFAULT_START
from gateway import GatewayState
from business_outputs import telemetry
from db import persist_one
from build_db_seed import build


class ReferenceIdentityTests(unittest.TestCase):
    def test_seed_generator_excludes_demo_id_and_keeps_frozen_integer_beds(self):
        seed = json.loads((ROOT / "config/db_reference_seed.json").read_text(encoding="utf-8"))
        self.assertTrue(all("demo_id" not in row for row in seed["patients"]))
        self.assertNotIn("demo_id", db_admin.SEED_COLUMNS["patients"])
        self.assertEqual(build(source_root()), seed)
        mapping = {int(row["stay_id"]): row["bed_id"] for row in seed["icustays"]}
        self.assertEqual(mapping, {37323533: 1, 35727289: 2, 39864867: 3, 32359580: 4})
        self.assertTrue(all(type(bed) is int for bed in mapping.values()))

    def test_chart_and_lab_raw_have_integer_beds_and_new_major_version(self):
        for topic, event in events_at(0, DEFAULT_START):
            wire = json.loads(json.dumps(event))
            self.assertIs(type(wire["bed_id"]), int)
            self.assertEqual(wire["schema_version"], "3.0")
            self.assertEqual(RAW_SCHEMA_VERSION, "3.0")
            validate_event(wire, "chartevents" if topic == TOPICS["chartevents"] else "labevents")

    def test_strict_bed_types_and_stay_binding_for_both_raw_streams(self):
        examples = {topic: event for topic, event in events_at(0, DEFAULT_START)}
        for topic, example in examples.items():
            stream = "chartevents" if topic == TOPICS["chartevents"] else "labevents"
            for invalid in ("BED-04", "4", 4.0, 1.5, None, True, False, -1, 0, 1):
                with self.subTest(topic=topic, bed=invalid), self.assertRaises(ValidationError):
                    validate_event(dict(example, bed_id=invalid), stream)
            for version in ("2.0", "1.0", "3.1"):
                source = dict(topic=topic, partition=0, offset=1, key_base64=None,
                              payload_base64=base64.b64encode(json.dumps(
                                  dict(example, schema_version=version)).encode()).decode())
                value, rejected = parse_and_validate(source, stream)
                self.assertIsNone(value)
                self.assertEqual(rejected["error_code"], "SCHEMA_VERSION_UNSUPPORTED")

    def test_registry_rejects_noncanonical_beds(self):
        for bed in ("BED-01", "1", True, 1.0, None, -1, 2):
            rows = load_patients()
            rows[0]["bed_id"] = bed
            with tempfile.TemporaryDirectory() as folder:
                path = Path(folder) / "patients.yaml"
                path.write_text(json.dumps(dict(patients=rows)), encoding="utf-8")
                with self.subTest(bed=bed), self.assertRaises(ValueError):
                    load_patients(path)

    def test_mapping_preflight_never_guesses_from_labels_or_row_order(self):
        rows = [(p["stay_id"], p["subject_id"], p["hadm_id"], f"BED-{p['bed_id']:02}")
                for p in reversed(load_patients())]
        db_admin.check_stay_beds(rows, legacy=True)
        for bed in ("BED-05", "BED-1", "BED-01 ", "1", 1, None):
            with self.subTest(bed=bed), self.assertRaises(RuntimeError):
                db_admin.check_stay_beds([(*rows[0][:3], bed)], legacy=True)

    def test_gateway_uses_application_lookup_and_overrides_untrusted_bed_extras(self):
        state = GatewayState()
        for patient in load_patients():
            raw = next(e for t, e in events_at(0) if e.get("stay_id") == patient["stay_id"])
            wide = telemetry(raw, dict(heart_rate=88, map=62, sbp=110, spo2=97))
            result = state.ingest("icu.telemetry", dict(wide, bed_id="BED-99", demo_id="wrong"))
            self.assertEqual(result["payload"]["bed_id"], patient["bed_id"])
            self.assertIs(type(result["payload"]["bed_id"]), int)
            self.assertEqual(result["payload"]["demo_id"], patient["demo_id"])
            snapshot = state.snapshot()[patient["demo_id"]]
            self.assertEqual(snapshot["bed_id"], patient["bed_id"])
            self.assertTrue(all(type(v["bed_id"]) is int for v in snapshot["vitals"].values()))


@unittest.skipUnless(os.environ.get("ICU_RUN_DATABASE_TESTS") == "1", "Opt-in disposable PostgreSQL DB")
class ReferenceDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg import sql
        cls.database = "icu_t014_test_" + uuid.uuid4().hex
        cls.admin = db_admin.connect()
        cls.created = False
        cls.folder = tempfile.TemporaryDirectory(prefix="icu_reference_test_")
        cls.admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        cls.created = True
        params = db_admin.db_params(host="127.0.0.1", values=db_admin.local_env())
        params["dbname"] = cls.database
        cls.connection = psycopg.connect(**params, autocommit=True)
        cls.connection.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    @classmethod
    def tearDownClass(cls):
        from psycopg import sql
        cls.connection.close()
        assert cls.created and cls.database.startswith("icu_t014_test_")
        assert cls.admin.execute("SELECT current_database()").fetchone()[0] != cls.database
        cls.admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(cls.database)))
        cls.admin.close()
        cls.folder.cleanup()

    def setUp(self):
        self.transaction = self.connection.transaction(force_rollback=True)
        self.transaction.__enter__()
        # Backup evidence was verified for the actual project DB before live migration.
        # This temporary file models that external backup gate in rollback-only fixtures.
        self.backup = Path(self.folder.name) / "backup-gate"
        self.backup.write_bytes(b"rollback-only fixture; live pg_dump verified separately")

    def tearDown(self):
        self.transaction.__exit__(None, None, None)

    def schema(self, legacy=False):
        for name in ("001_schema.sql", "002_hypertables.sql", "003_indexes.sql"):
            text = (db_admin.SQL_ROOT / name).read_text(encoding="utf-8")
            if legacy and name == "001_schema.sql":
                text = text.replace("subject_id BIGINT PRIMARY KEY,", "subject_id BIGINT PRIMARY KEY,\n    demo_id TEXT NOT NULL UNIQUE,", 1)
                text = text.replace("bed_id INTEGER NOT NULL UNIQUE", "bed_id TEXT NOT NULL UNIQUE")
            for statement in db_admin.sql_statements(text):
                self.connection.execute(statement)
        if legacy:
            seed = json.loads(db_admin.SEED.read_text(encoding="utf-8"))
            by_subject = {p["subject_id"]: p for p in load_patients()}
            for row in seed["patients"]:
                row["demo_id"] = by_subject[int(row["subject_id"])]["demo_id"]
            for row in seed["icustays"]:
                row["bed_id"] = f"BED-{row['bed_id']:02}"
            for table in db_admin.REFERENCE_TABLES:
                self.connection.execute('INSERT INTO public."' + table + '" SELECT * FROM '
                                        'jsonb_populate_recordset(NULL::public."' + table + '", %s::jsonb)',
                                        (json.dumps(seed[table]),))
        else:
            db_admin.seed_references(self.connection)

    def test_fresh_schema_and_repeated_seed_have_target_columns_and_counts(self):
        self.assertFalse(db_admin.migrate_reference_identity(self.connection))
        self.schema()
        self.assertTrue(db_admin.verify_reference_identity(self.connection)["patients_demo_id_absent"])
        self.assertFalse(db_admin.reference_migration_needed(self.connection))
        self.assertFalse(db_admin.migrate_reference_identity(self.connection))
        db_admin.apply_schema(self.connection)
        db_admin.seed_references(self.connection)
        self.assertEqual(db_admin.reference_counts(self.connection),
                         dict(patients=4, admissions=4, icustays=4, d_items=6, d_labitems=1))
        self.assertEqual(db_admin.source_counts(self.connection), dict(chartevents=0, labevents=0))

    def test_legacy_migration_preserves_business_rows_and_constraints_then_reseeds(self):
        self.schema(legacy=True)
        raw = next(e for t, e in events_at(0) if e.get("stay_id") == 37323533)
        wide = telemetry(raw, dict(heart_rate=88, map=62, sbp=110, spo2=97))
        persist_one(self.connection, "icu.telemetry", wide, b"37323533")
        before = self.connection.execute("SELECT * FROM patient_telemetry_stream").fetchall()
        self.assertTrue(db_admin.migrate_reference_identity(self.connection, self.backup, quiesced=True))
        self.assertTrue(db_admin.verify_reference_identity(self.connection)["bed_unique"])
        self.assertEqual(self.connection.execute("SELECT * FROM patient_telemetry_stream").fetchall(), before)
        self.assertFalse(db_admin.migrate_reference_identity(self.connection))
        db_admin.seed_references(self.connection)
        self.assertEqual(self.connection.execute("SELECT * FROM patient_telemetry_stream").fetchall(), before)
        import psycopg
        with self.assertRaises(psycopg.errors.UniqueViolation), self.connection.transaction():
            self.connection.execute("UPDATE icustays SET bed_id=1 WHERE bed_id=2")
        self.assertEqual(db_admin.verify_reference_identity(self.connection)["bed_type"], "integer")

    def test_legacy_requires_backup_and_quiescence(self):
        self.schema(legacy=True)
        for backup, quiet in ((None, True), (self.backup, False)):
            with self.assertRaisesRegex(RuntimeError, "backup.*quiesced"):
                db_admin.migrate_reference_identity(self.connection, backup, quiet)
        self.assertEqual(db_admin.reference_identity_state(self.connection)[("icustays", "bed_id")][0], "text")

    def test_unexpected_bed_fails_without_dropping_demo_column(self):
        self.schema(legacy=True)
        self.connection.execute("UPDATE icustays SET bed_id='unexpected' WHERE stay_id=37323533")
        with self.assertRaisesRegex(RuntimeError, "Unexpected ICU stay/bed mapping"):
            db_admin.migrate_reference_identity(self.connection, self.backup, quiesced=True)
        self.assertIn(("patients", "demo_id"), db_admin.reference_identity_state(self.connection))
        self.assertEqual(self.connection.execute("SELECT bed_id FROM icustays WHERE stay_id=37323533").fetchone()[0], "unexpected")

    def test_dependent_view_causes_full_migration_rollback_without_cascade(self):
        import psycopg
        self.schema(legacy=True)
        self.connection.execute("CREATE VIEW patient_demo_dependency AS SELECT demo_id FROM patients")
        with self.assertRaises(psycopg.errors.DependentObjectsStillExist):
            db_admin.migrate_reference_identity(self.connection, self.backup, quiesced=True)
        self.assertEqual(db_admin.reference_identity_state(self.connection)[("icustays", "bed_id")][0], "text")
        self.assertIn(("patients", "demo_id"), db_admin.reference_identity_state(self.connection))


if __name__ == "__main__":
    unittest.main()
