"""The sole four-patient registry. JSON syntax is a dependency-free YAML subset."""
import csv
import gzip
import json
import os
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
IDENTITY_FIELDS = ("subject_id", "hadm_id", "stay_id", "bed_id")
BED_IDS = {"P01": 1, "P02": 2, "P03": 3, "P04": 4}


def load_patients(path=None):
    path = path or os.environ.get("ICU_PATIENT_CONFIG", PROJECT / "config/demo_patients.yaml")
    patients = json.loads(Path(path).read_text(encoding="utf-8"))["patients"]
    if len(patients) != 4:
        raise ValueError("Exactly four patients are required")
    for index, patient in enumerate(patients, 1):
        if set(patient) != {*IDENTITY_FIELDS, "demo_id"}:
            raise ValueError("Unexpected patient registry fields")
        if patient["demo_id"] != f"P{index:02}":
            raise ValueError("Registry must contain P01-P04 in order")
        if type(patient["bed_id"]) is not int or patient["bed_id"] != BED_IDS[patient["demo_id"]]:
            raise ValueError("Registry beds must be integers: P01=1, P02=2, P03=3, P04=4")
        if any(type(patient[field]) is not int or patient[field] <= 0
               for field in IDENTITY_FIELDS[:3]):
            raise ValueError("MIMIC identities must be positive integers")
    for field in (*IDENTITY_FIELDS, "demo_id"):
        if len({patient[field] for patient in patients}) != 4:
            raise ValueError(f"Duplicate registry {field}")
    return patients


def source_root():
    configured = os.environ.get("MIMIC_CLINICAL_ROOT")
    if configured:
        return Path(configured)
    return PROJECT.parents[1] / "MIMIC_DATA/clinical/mimic-iv-clinical-database-demo-2.2/mimic-iv-clinical-database-demo-2.2"


def source_rows(relative, root=None):
    with gzip.open((root or source_root()) / relative, "rt", encoding="utf-8", newline="") as source:
        yield from csv.DictReader(source)


def validate_source_identities(patients=None, root=None):
    patients = patients or load_patients()
    subjects = {int(row["subject_id"]) for row in source_rows("hosp/patients.csv.gz", root)}
    admissions = {(int(row["subject_id"]), int(row["hadm_id"]))
                  for row in source_rows("hosp/admissions.csv.gz", root)}
    stays = {(int(row["subject_id"]), int(row["hadm_id"]), int(row["stay_id"]))
             for row in source_rows("icu/icustays.csv.gz", root)}
    for patient in patients:
        triple = tuple(patient[field] for field in IDENTITY_FIELDS[:3])
        if triple[0] not in subjects or triple[:2] not in admissions or triple not in stays:
            raise ValueError(f"Identity not present in local MIMIC: {patient['demo_id']}")
    return patients
