"""Idempotent schema/seed initialization and scoped runtime data reset."""
import json
import os
from pathlib import Path
import sys
from datetime import date, datetime

import psycopg
from psycopg import sql

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "services/storage_writer"))
from config import CORE_TABLES, REFERENCE_TABLES, RUNTIME_TABLES, TIME_COLUMNS, db_params

SQL_ROOT = PROJECT / "docker/timescaledb/init"
SEED = PROJECT / "config/db_reference_seed.json"
SEED_COLUMNS = {
    "patients": ("subject_id", "demo_id", "gender", "anchor_age", "anchor_year", "anchor_year_group", "dod"),
    "admissions": ("hadm_id", "subject_id", "admittime", "dischtime", "admission_type",
                   "admission_location", "discharge_location"),
    "icustays": ("stay_id", "subject_id", "hadm_id", "bed_id", "first_careunit",
                 "last_careunit", "intime", "outtime", "los"),
    "d_items": ("itemid", "label", "abbreviation", "category", "unitname", "param_type"),
    "d_labitems": ("itemid", "label", "fluid", "category"),
}
SEED_IDS = {"patients": "subject_id", "admissions": "hadm_id", "icustays": "stay_id",
            "d_items": "itemid", "d_labitems": "itemid"}
INTEGER_COLUMNS = {"subject_id", "hadm_id", "stay_id", "itemid", "anchor_age", "anchor_year"}
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


def verify_structure(connection):
    extension = connection.execute("SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'").fetchone()
    if not extension:
        raise RuntimeError("TimescaleDB extension is not installed")
    tables = {row[0] for row in connection.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")}
    if tables != set(CORE_TABLES):
        raise RuntimeError(f"Expected exactly nine public core tables; found {sorted(tables)}")
    hypertables = {row[0] for row in connection.execute(
        "SELECT hypertable_name FROM timescaledb_information.hypertables WHERE hypertable_schema = 'public'")}
    if hypertables != set(RUNTIME_TABLES):
        raise RuntimeError(f"Expected four business hypertables; found {sorted(hypertables)}")
    return {"extension_version": extension[0], "tables": sorted(tables),
            "hypertables": sorted(hypertables)}


def reference_counts(connection):
    return {table: connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
        sql.Identifier(table))).fetchone()[0] for table in REFERENCE_TABLES}


def runtime_counts(connection):
    return {table: connection.execute(sql.SQL("SELECT count(*) FROM {}").format(
        sql.Identifier(table))).fetchone()[0] for table in RUNTIME_TABLES}


def reset_runtime(connection, keep=False):
    if not keep:
        with connection.transaction():
            connection.execute("TRUNCATE TABLE chartevents, labevents, vitals_15m, alerts")
    return runtime_counts(connection)


def initialize(keep=False):
    with connect() as connection:
        apply_schema(connection)
        seed_references(connection)
        structure = verify_structure(connection)
        refs = reference_counts(connection)
        if refs["patients"] != 4 or refs["admissions"] != 4 or refs["icustays"] != 4:
            raise RuntimeError(f"Reference count mismatch: {refs}")
        before = runtime_counts(connection)
        after = reset_runtime(connection, keep=keep)
        if not keep and any(after.values()):
            raise RuntimeError("Runtime tables did not reset")
        return {"structure": structure, "reference_counts": refs,
                "runtime_before": before, "runtime_after": after, "kept_runtime_data": keep}
