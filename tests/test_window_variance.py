"""Five-minute population variance, additive wire contract and DB persistence.

Real DB tests require ICU_VARIANCE_TEST_DSN naming an icu_t017_test* database.
The real Flink test opts in with ICU_RUN_FLINK_WINDOW_TESTS=1.
"""
import copy
from datetime import datetime
import json
import math
import os
from pathlib import Path
import random
import statistics
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "jobs/flink"), str(ROOT / "services/storage_writer")]
from business_contract import ContractError, FIELDS, encode, headers, metadata, validate
from window_aggregates import (add_record, aggregate_result, attach_windows,
                               create_accumulator, iso_timestamp, merge_accumulators)

SIGNALS = tuple(FIELDS.values())
STAY = 35727289
MIGRATIONS = ROOT / "server/src/main/resources/db/migration"
MIGRATION = MIGRATIONS / "V3__create_hypertables.sql"


def accumulate(records):
    accumulator = create_accumulator()
    for record in records:
        add_record(accumulator, record)
    return accumulator


def window_event():
    return aggregate_result(accumulate([
        dict(heart_rate=80, map=70, sbp=110, spo2=96),
        dict(heart_rate=100, map=74, sbp=120, spo2=98),
    ]), STAY, 0, 300000, 302000)


class WindowVarianceTests(unittest.TestCase):
    def test_population_variance_and_all_existing_means(self):
        event = window_event()
        for signal, mean, variance in (("heart_rate", 90, 100), ("map", 72, 4),
                                       ("sbp", 115, 25), ("spo2", 97, 1)):
            self.assertEqual(event["avg_" + signal], mean)
            self.assertEqual(event["var_" + signal], variance)

    def test_null_singleton_constant_and_independent_signal_counts(self):
        event = aggregate_result(accumulate([
            dict(heart_rate=None, map=62, sbp=120, spo2=96),
            dict(heart_rate=None, map=None, sbp=120, spo2=98),
            dict(sbp=120),
        ]), STAY, 0, 300000)
        self.assertIsNone(event["avg_heart_rate"])
        self.assertIsNone(event["var_heart_rate"])
        self.assertEqual(event["avg_map"], 62)
        self.assertIsNone(event["var_map"])
        self.assertEqual((event["avg_sbp"], event["var_sbp"]), (120, 0))
        self.assertEqual((event["avg_spo2"], event["var_spo2"]), (97, 1))
        for records in ([], [{s: None for s in SIGNALS}]):
            self.assertIsNone(aggregate_result(accumulate(records), STAY, 0, 300000))

    def test_welford_avoids_cancellation_for_small_variations(self):
        values = [300 + (i % 5) * 1e-7 for i in range(1000)]
        event = aggregate_result(accumulate([dict(heart_rate=v) for v in values]), STAY, 0, 300000)
        self.assertAlmostEqual(event["avg_heart_rate"], statistics.mean(values), places=10)
        self.assertTrue(math.isclose(event["var_heart_rate"], statistics.pvariance(values),
                                     rel_tol=1e-6, abs_tol=1e-20))

    def test_merge_matches_population_reference_and_does_not_alias_inputs(self):
        rng = random.Random(17)
        records = [{s: rng.uniform(0, 100) if rng.random() > .3 else None for s in SIGNALS}
                   for _ in range(101)]
        for split in (0, 1, 37, 100, 101):
            with self.subTest(split=split):
                left, right = accumulate(records[:split]), accumulate(records[split:])
                before = copy.deepcopy((left, right))
                merged = merge_accumulators(left, right)
                event = aggregate_result(merged, STAY, 0, 300000)
                for signal in SIGNALS:
                    valid = [r[signal] for r in records if r[signal] is not None]
                    # Preserve the old sum/count mean semantics, allowing floating-point roundoff.
                    self.assertAlmostEqual(event["avg_" + signal], sum(valid) / len(valid), places=12)
                    self.assertAlmostEqual(event["var_" + signal], statistics.pvariance(valid), places=10)
                add_record(merged, {s: 100 for s in SIGNALS})
                self.assertEqual((left, right), before)
        self.assertIsNone(aggregate_result(merge_accumulators(create_accumulator(), create_accumulator()),
                                          STAY, 0, 300000))

    def test_window_metadata_identity_and_empty_window_behavior_are_preserved(self):
        event = window_event()
        legacy_metadata = metadata("VITALS_WINDOW_5M", [STAY, 300000], produced_at=iso_timestamp(302000))
        for name, value in legacy_metadata.items():
            self.assertEqual(event[name], value)
        self.assertEqual(event["window_end"], "1970-01-01T00:05:00.000Z")
        self.assertNotIn("window_start", event)
        self.assertEqual(set(event), set(legacy_metadata) | {"window_end", "stay_id"}
                         | {prefix + s for prefix in ("avg_", "var_") for s in SIGNALS})
        with self.assertRaises(ValueError):
            aggregate_result(create_accumulator(), STAY, 0, 60000)

    def test_wire_roundtrip_headers_and_additive_legacy_compatibility(self):
        for version in ("1.0", "1.1"):
            event = dict(window_event(), schema_version=version)
            decoded = json.loads(encode(event))
            validate(decoded, "icu.vitals.5m", str(STAY).encode(), headers(decoded), True)
            legacy = {k: v for k, v in decoded.items() if not k.startswith("var_")}
            validate(legacy, "icu.vitals.5m", str(STAY).encode(), headers(legacy), True)
            self.assertEqual(legacy["message_id"], decoded["message_id"])
        validate(dict(window_event(), var_map=None), "icu.vitals.5m")
        with self.assertRaises(ContractError):
            validate(dict(window_event(), schema_version="2.0"), "icu.vitals.5m")

    def test_variance_rejects_negative_nonfinite_boolean_string_and_overflow(self):
        for field in ("var_" + s for s in SIGNALS):
            for value in (-1, math.nan, math.inf, -math.inf, True, False, "1", [], {}, 10**400):
                with self.subTest(field=field, value=type(value).__name__), self.assertRaises(ContractError):
                    validate(dict(window_event(), **{field: value}), "icu.vitals.5m")
            for value in (None, 0, 0.5, 100000):
                validate(dict(window_event(), **{field: value}), "icu.vitals.5m")

    def test_writer_maps_new_and_legacy_columns_and_keeps_conflict_sql(self):
        from db import TABLE_COLUMNS, insert_sql, mapped_record
        event = window_event()
        table, fields = mapped_record("icu.vitals.5m", event, str(STAY).encode())
        self.assertEqual(tuple(fields), TABLE_COLUMNS[table])
        for signal in SIGNALS:
            self.assertEqual(fields["var_" + signal], event["var_" + signal])
        legacy = {k: v for k, v in event.items() if not k.startswith("var_")}
        _, old_fields = mapped_record("icu.vitals.5m", legacy, str(STAY).encode())
        self.assertTrue(all(old_fields["var_" + s] is None for s in SIGNALS))
        query = insert_sql(table).as_string()
        self.assertIn('ON CONFLICT ("window_end", "stay_id") DO NOTHING', query)
        self.assertEqual(query.count("%s"), len(TABLE_COLUMNS[table]))


@unittest.skipUnless(os.environ.get("ICU_VARIANCE_TEST_DSN"), "Opt-in isolated TimescaleDB variance test")
class WindowVarianceDatabaseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        dsn = os.environ["ICU_VARIANCE_TEST_DSN"]
        if not conninfo_to_dict(dsn).get("dbname", "").startswith("icu_t017_test"):
            raise ValueError("Variance DB tests require a dedicated icu_t017_test* database")
        cls.connection = psycopg.connect(dsn, autocommit=True)
        cls.addClassCleanup(cls.connection.close)
        assert cls.connection.execute("SELECT current_database()").fetchone()[0].startswith("icu_t017_test")

    def setUp(self):
        self.transaction = self.connection.transaction(force_rollback=True)
        self.transaction.__enter__()
        self.addCleanup(self.transaction.__exit__, None, None, None)

    def schema(self, legacy=False):
        # Actual Flyway SQL, applied only inside this disposable DB's rollback fixture.
        for path in sorted(MIGRATIONS.glob("V[0-5]__*.sql")):
            source = path.read_text(encoding="utf-8")
            if legacy and path == MIGRATION:
                start = source.index("ALTER TABLE vitals_sliding_window_5m")
                source = source[:start] + source[source.index(";", start) + 1:]
            self.connection.execute(source)
        from registry import load_patients
        patient = next(p for p in load_patients() if p["stay_id"] == STAY)
        self.connection.execute("INSERT INTO patients(subject_id) VALUES (%s)", (patient["subject_id"],))
        self.connection.execute("INSERT INTO admissions(hadm_id, subject_id) VALUES (%s,%s)",
                                (patient["hadm_id"], patient["subject_id"]))
        self.connection.execute("INSERT INTO icustays(stay_id, subject_id, hadm_id, bed_id) "
                                "VALUES (%s,%s,%s,%s)",
                                (STAY, patient["subject_id"], patient["hadm_id"], patient["bed_id"]))

    def migrate(self):
        self.connection.execute(MIGRATION.read_text(encoding="utf-8"))

    def test_fresh_schema_contains_variance_without_a_separate_migration(self):
        self.schema()
        columns = self.connection.execute("SELECT column_name FROM information_schema.columns "
                                          "WHERE table_name='vitals_sliding_window_5m' AND column_name LIKE 'var_%'").fetchall()
        self.assertEqual({name for name, in columns}, {"var_" + signal for signal in SIGNALS})
        self.assertFalse((MIGRATIONS / "V6__add_vitals_window_variance.sql").exists())

    def test_migration_preserves_rows_keys_hypertable_indexes_and_adds_nullable_columns(self):
        self.schema(legacy=True)
        connection = self.connection
        for date in ("2026-10-01T00:05:00Z", "2026-10-03T00:05:00Z"):
            connection.execute("INSERT INTO vitals_sliding_window_5m VALUES (%s,%s,90,72,115,97)", (date, STAY))
        before = connection.execute("SELECT * FROM vitals_sliding_window_5m ORDER BY window_end").fetchall()
        index_query = "SELECT indexname,indexdef FROM pg_indexes WHERE tablename='vitals_sliding_window_5m' ORDER BY indexname"
        hyper_query = ("SELECT hypertable_name,num_dimensions,num_chunks FROM timescaledb_information.hypertables "
                       "WHERE hypertable_name='vitals_sliding_window_5m'")
        indexes, hyper = connection.execute(index_query).fetchall(), connection.execute(hyper_query).fetchall()
        self.migrate()
        self.migrate()  # ADD COLUMN IF NOT EXISTS remains safe if columns already exist.
        after = connection.execute("SELECT * FROM vitals_sliding_window_5m ORDER BY window_end").fetchall()
        self.assertEqual([row[:6] for row in after], before)
        self.assertTrue(all(row[6:] == (None,) * 4 for row in after))
        self.assertEqual(connection.execute(index_query).fetchall(), indexes)
        self.assertEqual(connection.execute(hyper_query).fetchall(), hyper)
        columns = connection.execute("SELECT column_name,data_type,is_nullable FROM information_schema.columns "
                                     "WHERE table_name='vitals_sliding_window_5m' AND column_name LIKE 'var_%'").fetchall()
        self.assertEqual(set(columns), {(f"var_{s}", "double precision", "YES") for s in SIGNALS})

    def test_writer_persists_all_variances_and_duplicate_delivery_does_not_overwrite(self):
        from db import persist_one
        self.schema(legacy=True)
        self.migrate()
        event = window_event()
        persist_one(self.connection, "icu.vitals.5m", event, str(STAY).encode())
        changed = dict(event, avg_map=60, var_map=999)
        persist_one(self.connection, "icu.vitals.5m", changed, str(STAY).encode())
        result = self.connection.execute("SELECT avg_heart_rate,avg_map,avg_sbp,avg_spo2,"
                                         "var_heart_rate,var_map,var_sbp,var_spo2 FROM vitals_sliding_window_5m").fetchall()
        self.assertEqual(result, [(90, 72, 115, 97, 100, 4, 25, 1)])

    def test_legacy_messages_store_null_and_existing_rows_are_not_backfilled(self):
        from db import persist_one
        self.schema(legacy=True)
        self.connection.execute("INSERT INTO vitals_sliding_window_5m VALUES ('1970-01-01T00:05:00Z',%s,90,72,115,97)", (STAY,))
        self.migrate()
        event = window_event()
        persist_one(self.connection, "icu.vitals.5m", event, str(STAY).encode())
        legacy = {k: v for k, v in event.items() if not k.startswith("var_")}
        legacy["window_end"] = "1970-01-01T00:06:00Z"
        persist_one(self.connection, "icu.vitals.5m", legacy, str(STAY).encode())
        rows = self.connection.execute("SELECT var_heart_rate,var_map,var_sbp,var_spo2 FROM vitals_sliding_window_5m").fetchall()
        self.assertEqual(rows, [(None,) * 4, (None,) * 4])


@unittest.skipUnless(os.environ.get("ICU_RUN_FLINK_WINDOW_TESTS") == "1", "Opt-in real PyFlink window runtime")
class WindowVarianceFlinkTests(unittest.TestCase):
    def test_real_sliding_event_time_assignment_boundaries_and_results(self):
        from pyflink.common import Configuration, Duration, Types, WatermarkStrategy
        from pyflink.common.watermark_strategy import TimestampAssigner
        from pyflink.datastream import StreamExecutionEnvironment
        from runtime_config import WindowAlertConfig

        class EventTimestamp(TimestampAssigner):
            def extract_timestamp(self, value, record_timestamp):
                return json.loads(value)["timestamp_ms"]

        configuration = Configuration()
        configuration.set_string("taskmanager.memory.process.size", "1024m")
        configuration.set_string("jobmanager.memory.process.size", "512m")
        environment = StreamExecutionEnvironment.get_execution_environment(configuration)
        environment.set_parallelism(1)
        for path in (ROOT / "jobs/flink").glob("*.py"):
            environment.add_python_file(str(path))
        environment.add_python_file(str(Path(__file__)))
        samples = [(0, 10, None, None), (59999, 20, 70, None), (60000, 30, None, 120),
                   (299999, 40, 74, None), (300000, 50, None, None), (360000, 60, None, None)]
        records = [dict(stay_id=stay, timestamp_ms=t, heart_rate=hr, map=map_value, sbp=sbp, spo2=97)
                   for t, hr, map_value, sbp in samples for stay in (STAY, 37323533)]
        # An accepted 1ms reorder around the minute boundary; no sampling changes.
        records[2:6] = records[4:6] + records[2:4]
        source = environment.from_collection([encode(r) for r in records], type_info=Types.STRING())
        timed = source.assign_timestamps_and_watermarks(
            WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(2))
            .with_timestamp_assigner(EventTimestamp()))
        output = attach_windows(timed, WindowAlertConfig())
        with output.execute_and_collect() as results:
            events = [json.loads(value) for value in results]
        self.assertEqual(len(events), 22)  # Eleven overlapping windows per stay.
        self.assertEqual(len({(e["stay_id"], e["window_end"]) for e in events}), 22)
        for event in events:
            end = int(datetime.fromisoformat(event["window_end"].replace("Z", "+00:00")).timestamp() * 1000)
            expected = [r for r in records if r["stay_id"] == event["stay_id"] and end - 300000 <= r["timestamp_ms"] < end]
            self.assertTrue(expected)
            validate(event, "icu.vitals.5m")
            for signal in SIGNALS:
                values = [r[signal] for r in expected if r[signal] is not None]
                if values:
                    self.assertAlmostEqual(event["avg_" + signal], statistics.mean(values), places=10)
                else:
                    self.assertIsNone(event["avg_" + signal])
                if len(values) >= 2:
                    self.assertAlmostEqual(event["var_" + signal], statistics.pvariance(values), places=10)
                else:
                    self.assertIsNone(event["var_" + signal])


if __name__ == "__main__":
    unittest.main()
