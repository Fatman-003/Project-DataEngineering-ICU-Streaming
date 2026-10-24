"""Idempotent schema/seed initialization and scoped runtime data reset."""
import json
import os
import subprocess
from pathlib import Path
import sys
from datetime import date, datetime

import psycopg
from psycopg import sql

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "services/storage_writer"))
sys.path.insert(0, str(PROJECT / "jobs/flink"))
from config import CORE_TABLES, REFERENCE_TABLES, SOURCE_TABLES, RUNTIME_TABLES, TIME_COLUMNS, db_params
from registry import load_patients

SQL_ROOT = PROJECT / "docker/timescaledb/init"
SEED = PROJECT / "config/db_reference_seed.json"
SEED_COLUMNS = {
    "patients": ("subject_id", "gender", "anchor_age", "anchor_year", "anchor_year_group", "dod"),
    "admissions": ("hadm_id", "subject_id", "admittime", "dischtime", "admission_type",
                   "admission_location", "discharge_location"),
    "icustays": ("stay_id", "subject_id", "hadm_id", "bed_id", "first_careunit",
                 "last_careunit", "intime", "outtime", "los"),
    "d_items": ("itemid", "label", "abbreviation", "category", "unitname", "param_type"),
    "d_labitems": ("itemid", "label", "fluid", "category"),
}
SEED_IDS = {"patients": "subject_id", "admissions": "hadm_id", "icustays": "stay_id",
            "d_items": "itemid", "d_labitems": "itemid"}
INTEGER_COLUMNS = {"subject_id", "hadm_id", "stay_id", "bed_id", "itemid", "anchor_age", "anchor_year"}
TIMESTAMP_COLUMNS = {"admittime", "dischtime", "intime", "outtime"}


def local_env():
    """Load ignored .env without logging secrets; process environment wins."""
    values = {}
    path = PROJECT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            if line.strip() and not line.lstrip().startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key.strip()] = value.strip().strip("\"'")
    values.update({key: value for key, value in os.environ.items() if key in (
        "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD", "TIMESCALE_PORT")})
    return values


def connect():
    return psycopg.connect(**db_params(host="127.0.0.1", values=local_env()), autocommit=True)


def sql_statements(source):
    # These fixed project files contain no embedded semicolon in a string.
    body = "\n".join(line.split("--", 1)[0] for line in source.splitlines())
    return [part.strip() for part in body.split(";") if part.strip()]


def apply_schema(connection):
    with connection.transaction():
        for name in ("001_schema.sql", "002_hypertables.sql", "003_indexes.sql"):
            for statement in sql_statements((SQL_ROOT / name).read_text(encoding="utf-8")):
                connection.execute(statement)


def seed_values(row, columns):
    values = []
    for column in columns:
        value = row[column]
        if value is not None and column in INTEGER_COLUMNS:
            value = int(value)
        elif value is not None and column == "los":
            value = float(value)
        elif value is not None and column in TIMESTAMP_COLUMNS:
            value = datetime.fromisoformat(value)
        elif value is not None and column == "dod":
            value = date.fromisoformat(value)
        values.append(value)
    return values


def seed_references(connection):
    seed = json.loads(SEED.read_text(encoding="utf-8"))
    if len(seed["patients"]) != 4 or len(seed["admissions"]) != 4 or len(seed["icustays"]) != 4:
        raise ValueError("Reference seed must have exactly four fixed identities")
    with connection.transaction():
        for table in REFERENCE_TABLES:
            columns = SEED_COLUMNS[table]
            query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET {}").format(
                sql.Identifier(table), sql.SQL(", ").join(map(sql.Identifier, columns)),
                sql.SQL(", ").join(sql.Placeholder() for _ in columns), sql.Identifier(SEED_IDS[table]),
                sql.SQL(", ").join(sql.SQL("{} = EXCLUDED.{}").format(
                    sql.Identifier(column), sql.Identifier(column)) for column in columns if column != SEED_IDS[table]))
            for row in seed[table]:
                connection.execute(query, seed_values(row, columns))
    return {table: len(seed[table]) for table in REFERENCE_TABLES}


def verify_structure(connection, allow_legacy=False):
    extension = connection.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").fetchone()
    if not extension:
        raise RuntimeError("TimescaleDB extension is not installed")
    tables = {row[0] for row in connection.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")}
    legacy = tables & {"vitals_15m", "alerts"}
    if tables - (legacy if allow_legacy else set()) != set(CORE_TABLES):
        raise RuntimeError(f"Expected eleven core tables; found {sorted(tables)}")
    hypertables = {row[0] for row in connection.execute(
        "SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_schema = 'public'")}
    if hypertables - (legacy if allow_legacy else set()) != set(RUNTIME_TABLES):
        raise RuntimeError(f"Expected four business hypertables; found {sorted(hypertables)}")
    return {"extension_version": extension[0], "tables": sorted(tables),
            "hypertables": sorted(hypertables), "legacy_tables": sorted(legacy)}


def reference_counts(connection):
    return {table: connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
        sql.Identifier(table))).fetchone()[0] for table in REFERENCE_TABLES}


def runtime_counts(connection):
    return {table: connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
        sql.Identifier(table))).fetchone()[0] for table in RUNTIME_TABLES}


def reset_runtime(connection, keep=False):
    if not keep:
        with connection.transaction():
            connection.execute("TRUNCATE TABLE patient_telemetry_stream, vitals_sliding_window_5m, sepsis_alert_events, waveform_segments")
    return runtime_counts(connection)


def legacy_sources(connection):
    return bool(connection.execute("SELECT 1 FROM information_schema.columns WHERE table_schema='public' AND table_name='chartevents' AND column_name='event_time'").fetchone())


def backup_database(docker):
    """Full custom archive before changing an old schema; no DB mutation."""
    from datetime import timezone
    directory = PROJECT / ".local/db_backups"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / (datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ") + "_before_contract.dump")
    env = local_env()
    with target.open("wb") as output:
        subprocess.run([str(docker), "exec", "icu-timescaledb", "pg_dump", "-U", env["POSTGRES_USER"],
                        "-d", env["POSTGRES_DB"], "-Fc"], stdout=output, stderr=subprocess.PIPE, check=True)
    with target.open("rb") as stream:
        subprocess.run([str(docker), "exec", "-i", "icu-timescaledb", "pg_restore", "--list"],
                       stdin=stream, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    return str(target)


def database_snapshot(connection):
    tables = {row[0] for row in connection.execute(
        "SELECT tablename FROM pg_tables WHERE schemaname='public'")}
    owned = tables & (set(CORE_TABLES) | {"vitals_15m", "alerts"})
    return {"rows": {table: connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
        sql.Identifier(table))).fetchone()[0] for table in sorted(owned)},
            "hypertables": sorted(row[0] for row in connection.execute(
                "SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_schema='public'"))}


def migrate_sources(connection, backup_path, quiesced=False):
    if not legacy_sources(connection):
        return False
    if not quiesced or not backup_path or not Path(backup_path).is_file() or Path(backup_path).stat().st_size == 0:
        raise RuntimeError("Breaking migration requires a verified backup and quiesced ICU consumers")
    # Fixed owned source tables only, never CASCADE or reference tables.
    with connection.transaction():
        connection.execute("DROP TABLE chartevents, labevents")
    return True


def retire_legacy(connection, verified=False):
    if not verified:
        raise RuntimeError("Legacy tables may retire only after new consumers pass verification")
    with connection.transaction():
        connection.execute("DROP TABLE IF EXISTS vitals_15m, alerts")
    return verify_structure(connection)


def source_counts(connection):
    return {table: connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
        sql.Identifier(table))).fetchone()[0] for table in SOURCE_TABLES}


def reference_identity_state(connection):
    """Inspect before CREATE/seed so fresh and legacy databases share one path."""
    return {(table, column): (kind, nullable) for table, column, kind, nullable in connection.execute(
        "SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns "
        "WHERE table_schema='public' AND ((table_name='patients' AND column_name='demo_id') "
        "OR (table_name='icustays' AND column_name='bed_id'))")}


def reference_migration_needed(connection):
    state = reference_identity_state(connection)
    bed = state.get(("icustays", "bed_id"))
    return ("patients", "demo_id") in state or bed is not None and bed[0] != "integer"


def check_stay_beds(rows, legacy=False):
    """Validate explicit registry bindings before any cast; never guess a bed."""
    expected = {p["stay_id"]: p for p in load_patients()}
    for stay, subject, admission, bed in rows:
        patient = expected.get(stay)
        wanted = None if patient is None else (f"BED-{patient['bed_id']:02}" if legacy else patient["bed_id"])
        if (patient is None or (subject, admission) != (patient["subject_id"], patient["hadm_id"])
                or bed != wanted or type(bed) is not (str if legacy else int)):
            raise RuntimeError(f"Unexpected ICU stay/bed mapping: stay_id={stay}, bed_id={bed!r}; "
                               "expected the frozen P01-P04 identities with BED-01..BED-04 (legacy) "
                               "or integer beds 1..4. No reference data was changed.")


def migrate_reference_identity(connection, backup_path=None, quiesced=False):
    """Transactional, scoped TEXT-to-INTEGER conversion and obsolete column removal."""
    state = reference_identity_state(connection)
    bed = state.get(("icustays", "bed_id"))
    if bed is not None and bed[0] not in ("text", "integer"):
        raise RuntimeError(f"Unsupported icustays.bed_id type: {bed[0]}; refusing migration")
    changed = ("patients", "demo_id") in state or bed is not None and bed[0] == "text"
    if changed and (not quiesced or not backup_path or not Path(backup_path).is_file()
                    or Path(backup_path).stat().st_size == 0):
        raise RuntimeError("Reference identity migration requires a verified backup and quiesced ICU consumers")
    with connection.transaction():
        if changed:
            # Lock only the two owned reference tables; avoid a check/cast race.
            connection.execute("LOCK TABLE public.patients, public.icustays IN ACCESS EXCLUSIVE MODE")
        if bed is not None:
            rows = connection.execute(
                "SELECT stay_id,subject_id,hadm_id,bed_id FROM public.icustays ORDER BY stay_id").fetchall()
            check_stay_beds(rows, legacy=bed[0] == "text")
        if bed is not None and bed[0] == "text":
            connection.execute("ALTER TABLE public.icustays ALTER COLUMN bed_id TYPE INTEGER USING "
                               "CASE bed_id WHEN 'BED-01' THEN 1 WHEN 'BED-02' THEN 2 "
                               "WHEN 'BED-03' THEN 3 WHEN 'BED-04' THEN 4 END")
        if ("patients", "demo_id") in state:
            # PostgreSQL removes this column's own UNIQUE constraint/index.
            # External dependencies cause an error/rollback; never use CASCADE.
            connection.execute("ALTER TABLE public.patients DROP COLUMN demo_id")
    return changed


def verify_reference_identity(connection):
    state = reference_identity_state(connection)
    if ("patients", "demo_id") in state or state.get(("icustays", "bed_id")) != ("integer", "NO"):
        raise RuntimeError("Expected no patients.demo_id and icustays.bed_id INTEGER NOT NULL")
    unique = connection.execute(
        "SELECT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='public.icustays'::regclass "
        "AND contype='u' AND conkey=ARRAY[(SELECT attnum FROM pg_attribute "
        "WHERE attrelid='public.icustays'::regclass AND attname='bed_id')]::smallint[])").fetchone()[0]
    if not unique:
        raise RuntimeError("icustays.bed_id must retain its UNIQUE constraint")
    rows = connection.execute(
        "SELECT stay_id,subject_id,hadm_id,bed_id FROM public.icustays ORDER BY bed_id").fetchall()
    check_stay_beds(rows)
    if len(rows) != 4 or len({row[3] for row in rows}) != 4:
        raise RuntimeError("Expected four distinct frozen integer ICU beds")
    return {"patients_demo_id_absent": True, "bed_type": "integer", "bed_not_null": True,
            "bed_unique": True, "stay_beds": [list(row) for row in rows]}


def initialize(keep=False, backup_path=None, quiesced=False):
    with connect() as connection, connection.transaction():
        migrated = migrate_sources(connection, backup_path, quiesced)
        reference_migrated = migrate_reference_identity(connection, backup_path, quiesced)
        apply_schema(connection)
        seed_references(connection)
        reference_schema = verify_reference_identity(connection)
        structure = verify_structure(connection, allow_legacy=True)
        refs = reference_counts(connection)
        if refs["patients"] != 4 or refs["admissions"] != 4 or refs["icustays"] != 4:
            raise RuntimeError(f"Reference count mismatch: {refs}")
        before = runtime_counts(connection)
        after = reset_runtime(connection, keep=keep)
        if not keep and any(after.values()):
            raise RuntimeError("Runtime tables did not reset")
        if any(source_counts(connection).values()):
            raise RuntimeError("MIMIC source placeholders must remain empty")
        return {"structure": structure, "reference_counts": refs, "migrated": migrated or reference_migrated,
                "reference_migrated": reference_migrated, "reference_schema": reference_schema,
                "backup_path": None if backup_path is None else str(backup_path),
                "source_counts": source_counts(connection),
                "runtime_before": before, "runtime_after": after, "kept_runtime_data": keep}
