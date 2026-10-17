"""Freeze only the active, source-verified MIMIC reference rows for local DB startup.

Run explicitly with --source-root pointing at the clinical demo 2.2 directory.
Normal Docker startup reads the committed JSON and never scans source files.
"""
import argparse
import csv
import gzip
import json
from pathlib import Path
import sys

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "jobs/flink"))
from contracts import SIGNALS, LABS
from registry import load_patients

SOURCES = {
    "patients": ("hosp/patients.csv.gz", "subject_id",
                 ("subject_id", "gender", "anchor_age", "anchor_year", "anchor_year_group", "dod")),
    "admissions": ("hosp/admissions.csv.gz", "hadm_id",
                   ("hadm_id", "subject_id", "admittime", "dischtime", "admission_type",
                    "admission_location", "discharge_location")),
    "icustays": ("icu/icustays.csv.gz", "stay_id",
                 ("stay_id", "subject_id", "hadm_id", "first_careunit", "last_careunit",
                  "intime", "outtime", "los")),
    "d_items": ("icu/d_items.csv.gz", "itemid",
                ("itemid", "label", "abbreviation", "category", "unitname", "param_type")),
    "d_labitems": ("hosp/d_labitems.csv.gz", "itemid",
                   ("itemid", "label", "fluid", "category")),
}


def build(source_root):
    source_root = Path(source_root).resolve()
    patients = load_patients()
    expected = {
        "patients": {str(p["subject_id"]) for p in patients},
        "admissions": {str(p["hadm_id"]) for p in patients},
        "icustays": {str(p["stay_id"]) for p in patients},
        "d_items": {str(spec[0]) for spec in SIGNALS.values()},
        "d_labitems": {str(spec[0]) for spec in LABS.values()},
    }
    result = {"source": "MIMIC-IV Clinical Database Demo 2.2",
              "source_clock": "deidentified local timestamps; timezone unspecified",
              "bed_id_source": "project simulation"}
    for table, (name, key, fields) in SOURCES.items():
        path = source_root / name
        if not path.is_file():
            raise FileNotFoundError(path)
        selected = {}
        with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                identity = row[key]
                if identity in expected[table]:
                    if identity in selected:
                        raise ValueError(f"Duplicate {table} source ID {identity}")
                    selected[identity] = {field: row[field] or None for field in fields}
        if set(selected) != expected[table]:
            raise ValueError(f"{table}: source IDs disagree with fixed runtime registry")
        result[table] = [selected[key] for key in sorted(selected, key=int)]
    identity = {p["subject_id"]: p for p in patients}
    admissions = {int(a["hadm_id"]): a for a in result["admissions"]}
    for stay in result["icustays"]:
        patient = identity[int(stay["subject_id"])]
        if (int(stay["hadm_id"]) != patient["hadm_id"]
                or admissions[patient["hadm_id"]]["subject_id"] != str(patient["subject_id"])):
            raise ValueError("ICU stay/admission/source identity conflict")
        stay["bed_id"] = patient["bed_id"]
    for patient in result["patients"]:
        patient["demo_id"] = identity[int(patient["subject_id"])]["demo_id"]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--check", action="store_true", help="Compare source with committed seed without changing it")
    args = parser.parse_args()
    target = PROJECT / "config/db_reference_seed.json"
    output = json.dumps(build(args.source_root), indent=2, ensure_ascii=False) + "\n"
    if args.check:
        if target.read_text(encoding="utf-8") != output:
            raise SystemExit("Committed reference seed differs from validated source")
        print("Reference seed matches source")
    else:
        target.write_text(output, encoding="utf-8")
        print(f"Wrote {target}")


if __name__ == "__main__":
    main()
