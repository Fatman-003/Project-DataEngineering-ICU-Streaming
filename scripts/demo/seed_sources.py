#!/usr/bin/env python3
"""Idempotently seed the reference/source data used by the Spring Boot server."""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg

PROJECT = Path(__file__).resolve().parents[2]
DEFAULT_SEED = PROJECT / "config" / "db_reference_seed.json"

REQUIRED_TABLES = ("patients", "admissions", "icustays", "d_items", "d_labitems", "labevents")


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


def parse_args():
    env = local_env()
    parser = argparse.ArgumentParser(description=__doc__)
    # The host side always reaches the published port; container DNS names do not
    # resolve from Windows, and 127.0.0.1 skips the ::1 connection attempt.
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=int(env.get("TIMESCALE_PORT", "5432")))
    parser.add_argument("--dbname", default=env.get("POSTGRES_DB", "icu"))
    parser.add_argument("--user", default=env.get("POSTGRES_USER", "icu_app"))
    parser.add_argument("--password", default=env.get("POSTGRES_PASSWORD", ""))
    parser.add_argument("--seed-file", type=Path, default=DEFAULT_SEED)
    parser.add_argument("--wait", action="store_true", help="Wait for the server migrations to create the tables")
    parser.add_argument("--wait-timeout", type=int, default=120)
    return parser.parse_args()


def connect(args):
    return psycopg.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        connect_timeout=5,
    )


def missing_tables(connection):
    rows = connection.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    ).fetchall()
    present = {row[0] for row in rows}
    return [table for table in REQUIRED_TABLES if table not in present]


def wait_for_tables(args, missing):
    deadline = time.monotonic() + args.wait_timeout
    while time.monotonic() < deadline:
        with connect(args) as connection:
            missing = missing_tables(connection)
        if not missing:
            return
        print(f"Waiting for tables: {', '.join(missing)}", flush=True)
        time.sleep(2)
    raise RuntimeError(f"Timed out waiting for tables: {', '.join(missing)}")


def bed_number(value, fallback):
    if value is None:
        return fallback
    digits = "".join(char for char in str(value) if char.isdigit())
    return int(digits) if digits else fallback


def parse_timestamp(value):
    if value is None:
        return None
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def parse_date(value):
    if value is None:
        return None
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def seed_patients(connection, patients):
    query = """
        INSERT INTO patients (subject_id, gender, anchor_age, anchor_year, anchor_year_group, dod)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (subject_id) DO UPDATE SET
            gender = EXCLUDED.gender,
            anchor_age = EXCLUDED.anchor_age,
            anchor_year = EXCLUDED.anchor_year,
            anchor_year_group = EXCLUDED.anchor_year_group,
            dod = EXCLUDED.dod
    """
    for patient in patients:
        connection.execute(
            query,
            (
                int(patient["subject_id"]),
                patient.get("gender"),
                int(patient["anchor_age"]) if patient.get("anchor_age") not in (None, "") else None,
                int(patient["anchor_year"]) if patient.get("anchor_year") not in (None, "") else None,
                patient.get("anchor_year_group"),
                parse_date(patient.get("dod")),
            ),
        )
    return len(patients)


def seed_admissions(connection, admissions):
    query = """
        INSERT INTO admissions (
            hadm_id, subject_id, admittime, dischtime, admission_type,
            admission_location, discharge_location
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (hadm_id) DO UPDATE SET
            subject_id = EXCLUDED.subject_id,
            admittime = EXCLUDED.admittime,
            dischtime = EXCLUDED.dischtime,
            admission_type = EXCLUDED.admission_type,
            admission_location = EXCLUDED.admission_location,
            discharge_location = EXCLUDED.discharge_location
    """
    for admission in admissions:
        connection.execute(
            query,
            (
                int(admission["hadm_id"]),
                int(admission["subject_id"]),
                parse_timestamp(admission.get("admittime")),
                parse_timestamp(admission.get("dischtime")),
                admission.get("admission_type"),
                admission.get("admission_location"),
                admission.get("discharge_location"),
            ),
        )
    return len(admissions)


def seed_icustays(connection, icustays):
    query = """
        INSERT INTO icustays (
            stay_id, subject_id, hadm_id, bed_id, first_careunit,
            last_careunit, intime, outtime, los
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (stay_id) DO UPDATE SET
            subject_id = EXCLUDED.subject_id,
            hadm_id = EXCLUDED.hadm_id,
            bed_id = EXCLUDED.bed_id,
            first_careunit = EXCLUDED.first_careunit,
            last_careunit = EXCLUDED.last_careunit,
            intime = EXCLUDED.intime,
            outtime = EXCLUDED.outtime,
            los = EXCLUDED.los
    """
    for index, stay in enumerate(icustays, start=1):
        connection.execute(
            query,
            (
                int(stay["stay_id"]),
                int(stay["subject_id"]),
                int(stay["hadm_id"]),
                bed_number(stay.get("bed_id"), index),
                stay.get("first_careunit"),
                stay.get("last_careunit"),
                parse_timestamp(stay.get("intime")),
                parse_timestamp(stay.get("outtime")),
                float(stay["los"]) if stay.get("los") not in (None, "") else None,
            ),
        )
    return len(icustays)


def seed_d_items(connection, items):
    query = """
        INSERT INTO d_items (
            itemid, label, abbreviation, linksto, category,
            unitname, param_type, lownormalvalue, highnormalvalue
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (itemid) DO UPDATE SET
            label = EXCLUDED.label,
            abbreviation = EXCLUDED.abbreviation,
            linksto = EXCLUDED.linksto,
            category = EXCLUDED.category,
            unitname = EXCLUDED.unitname,
            param_type = EXCLUDED.param_type,
            lownormalvalue = EXCLUDED.lownormalvalue,
            highnormalvalue = EXCLUDED.highnormalvalue
    """
    for item in items:
        connection.execute(
            query,
            (
                int(item["itemid"]),
                item.get("label"),
                item.get("abbreviation"),
                item.get("linksto", "chartevents"),
                item.get("category"),
                item.get("unitname"),
                item.get("param_type", "Numeric"),
                item.get("lownormalvalue"),
                item.get("highnormalvalue"),
            ),
        )
    return len(items)


def seed_d_labitems(connection, items):
    query = """
        INSERT INTO d_labitems (itemid, label, fluid, category)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (itemid) DO UPDATE SET
            label = EXCLUDED.label,
            fluid = EXCLUDED.fluid,
            category = EXCLUDED.category
    """
    for item in items:
        connection.execute(
            query,
            (int(item["itemid"]), item.get("label"), item.get("fluid"), item.get("category")),
        )
    return len(items)


def seed_labevents(connection, seed):
    """Seed a small deterministic legacy lab set for the Labs panel."""
    stays = {int(stay["stay_id"]): stay for stay in seed["icustays"]}
    admissions = {int(admission["subject_id"]): admission for admission in seed["admissions"]}
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    samples = [
        ("Lactate", 50813, 2.0, "mmol/L", None, 6),
        ("Lactate", 50813, 3.4, "mmol/L", "HIGH", 18),
    ]
    count = 0

    for index, (stay_id, stay) in enumerate(sorted(stays.items()), start=1):
        subject_id = int(stay["subject_id"])
        admission = admissions[subject_id]
        hadm_id = int(admission["hadm_id"])
        stay_time = parse_timestamp(stay.get("intime")) or now

        for sample_index, (lab_name, itemid, value, unit, flags, hours_after) in enumerate(samples, start=1):
            charttime = stay_time + timedelta(hours=hours_after)
            connection.execute(
                """
                INSERT INTO labevents (
                    subject_id, hadm_id, charttime, itemid,
                    value, valuenum, valueuom, flags
                )
                SELECT %s, %s, %s, %s, %s, %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM labevents
                    WHERE subject_id = %s
                      AND hadm_id IS NOT DISTINCT FROM %s
                      AND charttime = %s
                      AND itemid = %s
                      AND valuenum IS NOT DISTINCT FROM %s
                      AND flags IS NOT DISTINCT FROM %s
                )
                """,
                (
                    subject_id,
                    hadm_id,
                    charttime,
                    itemid,
                    str(value),
                    value,
                    unit,
                    flags,
                    subject_id,
                    hadm_id,
                    charttime,
                    itemid,
                    value,
                    flags,
                ),
            )
            count += 1
    return count


def main():
    args = parse_args()
    if not args.seed_file.is_file():
        raise FileNotFoundError(f"Seed file not found: {args.seed_file}")

    seed = json.loads(args.seed_file.read_text(encoding="utf-8"))

    if args.wait:
        wait_for_tables(args, missing_tables(connect(args)))
    else:
        with connect(args) as connection:
            missing = missing_tables(connection)
        if missing:
            raise RuntimeError(
                "Missing tables: " + ", ".join(missing) + ". Start the server once or pass --wait."
            )

    with connect(args) as connection:
        counts = {
            "patients": seed_patients(connection, seed["patients"]),
            "admissions": seed_admissions(connection, seed["admissions"]),
            "icustays": seed_icustays(connection, seed["icustays"]),
            "d_items": seed_d_items(connection, seed["d_items"]),
            "d_labitems": seed_d_labitems(connection, seed["d_labitems"]),
            "labevents": seed_labevents(connection, seed),
        }

    print("Seed complete:")
    for table, count in counts.items():
        print(f"  {table}: {count}")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(f"Seed failed: {error}", file=sys.stderr)
        raise
