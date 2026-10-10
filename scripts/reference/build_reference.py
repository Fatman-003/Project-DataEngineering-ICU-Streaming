"""TICKET-003: build only the three reviewed canonical Reference stays.

Run: .venv/Scripts/python.exe -B scripts/reference/build_reference.py
No waveform samples are decoded. Requires existing pyarrow and wfdb.
"""
import csv
import gzip
import hashlib
import json
import math
import re
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import wfdb

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "scripts/inspect"))
from run_inspection import load_paths, raw_snapshot

VERSION = "1.0.0"
SELECTED = (
    ("37323533", "10039708", "24928679", "83411188"),
    ("35727289", "10019003", "21457723", "87033314"),
    ("39864867", "10014354", "29600294", "81739927"),
)
VITAL_LABELS = {
    "HR": r"^Heart Rate$", "MAP": r"blood pressure.*mean",
    "SBP": r"blood pressure.*systolic", "DBP": r"blood pressure.*diastolic",
    "SPO2": r"^(?:O2|Oxygen) saturation pulseoxymetry$", "RR": r"^Respiratory Rate$",
}
LAB_LABELS = {
    "Lactate": r"^(?:Lactate|Lactic Acid)$",
    "Creatinine": r"^Creatinine(?:, Whole Blood| \(serum\)| \(whole blood\))?$",
    "Platelet": r"^Z?Platelet Count$",
    "Bilirubin": r"^(?:Total |Direct )?Bilirubin(?:, (?:Total|Direct|Indirect))?$",
}
VITAL_UNITS = {
    "HR": ({"bpm": "bpm"}, "bpm"),
    "MAP": ({"mmHg": "mmHg"}, "mmHg"), "SBP": ({"mmHg": "mmHg"}, "mmHg"),
    "DBP": ({"mmHg": "mmHg"}, "mmHg"), "SPO2": ({"%": "%"}, "%"),
    "RR": ({"insp/min": "breaths/min", "breaths/min": "breaths/min"}, "breaths/min"),
}
NULLS = {"", "null", "nan", "na", "n/a"}
PARQUETS = ("patient_stay.parquet", "telemetry_event.parquet",
            "clinical_event.parquet", "waveform_manifest.parquet")


def source_rows(path):
    with (gzip.open(path, "rt", encoding="utf-8-sig", newline="") if path.suffix == ".gz"
          else path.open(encoding="utf-8-sig", newline="")) as stream:
        for number, row in enumerate(csv.DictReader(stream), 1):
            if None in row or any(v is None for v in row.values()):
                raise ValueError(f"Malformed CSV record {number} in {path}")
            yield number, row


def read_csv(path):
    return [row for _, row in source_rows(path)]


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def stable_hash(parts):
    return hashlib.sha256(json.dumps(parts, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def parse_time(value):
    try:
        return datetime.fromisoformat(value) if value else None
    except (ValueError, TypeError):
        return None


def numeric(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else None
    except (TypeError, ValueError):
        return None


def available_time(chart_time, storetime):
    stored = parse_time(storetime)
    if stored is None:
        return None, "UNKNOWN_STORETIME"
    if stored < chart_time:
        return None, "STORETIME_BEFORE_CHARTTIME"
    return stored, "VERIFIED_STORETIME"


def source_id(file, number):
    return f"{file}#record={number}"


def event_id(table, stay, record, time, item):
    return "evt-" + stable_hash([VERSION, table, stay, record, time.isoformat(), item])


def frame_window(start, frame_count, fs, intime, outtime):
    """Return [first, stop) segment frames entirely in the ICU interval."""
    first = max(0, math.ceil((intime - start).total_seconds() * fs - 1e-9))
    stop = min(frame_count, math.floor((outtime - start).total_seconds() * fs + 1e-9))
    return first, max(first, stop)


def dictionary_rules(items, labs):
    vitals, clinical = {}, {}
    for item, definition in items.items():
        label, table = definition["label"], definition["linksto"]
        if table == "chartevents":
            for signal, pattern in VITAL_LABELS.items():
                if re.search(pattern, label, re.I):
                    vitals[item] = signal
            if re.match(r"^GCS -", label, re.I):
                clinical[("chartevents", item)] = ("GCS", label)
            for name, pattern in LAB_LABELS.items():
                if re.search(pattern, label, re.I):
                    clinical[("chartevents", item)] = ("LAB", name)
        elif table == "inputevents" and re.match(
                r"^(Norepinephrine|Epinephrine|Vasopressin|Phenylephrine|Dopamine)(?:$|[ .(])", label, re.I):
            clinical[("inputevents", item)] = ("VASOPRESSOR", label)
        elif table == "outputevents" and (
                label in {"Foley", "Void", "Condom Cath", "Straight Cath", "Suprapubic",
                          "Left Nephrostomy", "Right Nephrostomy"}
                or re.fullmatch(r"(?:OR |PACU )?Urine", label, re.I)):
            clinical[("outputevents", item)] = ("URINE", label)
    for item, definition in labs.items():
        if definition["fluid"] != "Blood":
            continue
        for name, pattern in LAB_LABELS.items():
            if re.search(pattern, definition["label"], re.I):
                clinical[("labevents", item)] = ("LAB", name)
    return vitals, clinical


def schemas(run_id):
    S, I, F, B, T, L = pa.string(), pa.int64(), pa.float64(), pa.bool_(), pa.timestamp("us"), pa.list_(pa.field("element", pa.string()))
    def fields(names, kind, nullable=False):
        return [pa.field(n, kind, nullable=nullable) for n in names.split()]
    identity = fields("patient_key subject_id hadm_id stay_id", S)
    event = fields("schema_version event_id run_id", S) + identity
    provenance = fields("source_table source_record_id source_file", S) + fields("source_row_number", I)
    reference = fields("dataset_type test_mode", S) + fields("scenario_type", S, True) + fields("is_virtual", B)
    definitions = {
        "patient_stay.parquet": identity + fields("gender", S, True) + fields("age anchor_age anchor_year", I, True)
            + fields("age_is_approximate bed_id_is_simulated", B) + fields("care_unit last_care_unit", S, True)
            + fields("icu_intime icu_outtime", T) + fields("bed_id dataset_type mapping_status mapping_reason", S)
            + fields("is_virtual", B) + fields("source_stay_record_id source_patient_record_id", S),
        "telemetry_event.parquet": event + fields("source_event_time event_time", T)
            + fields("producer_time", T, True) + fields("signal_name unit quality_flag source_type", S)
            + fields("value", F) + fields("sequence_number", I) + provenance
            + fields("source_item_id source_value source_unit", S) + reference + fields("virtual_patient_id", S, True),
        "clinical_event.parquet": event + fields("event_type item_id item_name measurement_name", S)
            + fields("chart_time", T) + fields("available_time", T, True) + fields("availability_status", S)
            + fields("value", F, True) + fields("value_text unit source_native_id source_data_flag", S, True)
            + fields("source_storetime", T, True) + fields("source_storetime_text", S, True)
            + fields("source_end_time", T, True) + fields("source_rate", F, True)
            + fields("source_rate_unit source_status", S, True) + fields("validation_flags", L)
            + provenance + reference,
        "waveform_manifest.parquet": identity + fields("waveform_record_id waveform_segment_id signal_name", S)
            + fields("source_start_time source_end_time raw_segment_start_time raw_segment_end_time", T)
            + fields("sampling_rate_hz frame_frequency_hz", F) + fields("unit", S, True)
            + fields("source_header_path source_parent_header_path source_data_reference", S)
            + fields("channel_index samples_per_frame source_frame_start source_frame_end source_sample_start source_sample_end", I)
            + fields("dataset_type", S) + fields("is_virtual", B),
    }
    metadata = {b"schema_version": VERSION.encode(), b"run_id": run_id.encode(),
                b"timestamp_convention": b"Original deidentified MIMIC wall times; timezone unspecified; no shift",
                b"dataset_type": b"REFERENCE"}
    return {name: pa.schema(spec, metadata=metadata) for name, spec in definitions.items()}


def load_context():
    paths, inventory = load_paths()
    config = {}
    for line in (PROJECT / ".env").read_text(encoding="utf-8-sig").splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            key, value = line.split("=", 1)
            config[key.strip()] = value.strip().strip("'\"")
    if not config.get("REFERENCE_DATA_ROOT"):
        raise ValueError("REFERENCE_DATA_ROOT is missing from .env")
    output = (PROJECT / config["REFERENCE_DATA_ROOT"]).resolve()
    if output != (PROJECT / "data/reference").resolve():
        raise ValueError("REFERENCE_DATA_ROOT must resolve to project data/reference")
    for key in ("MIMIC_CLINICAL_ROOT", "MIMIC_WAVEFORM_ROOT"):
        if output.is_relative_to(paths[key]) or paths[key].is_relative_to(output):
            raise ValueError("Reference output overlaps raw data")
    root = paths["MIMIC_CLINICAL_ROOT"]
    if not (root / "icu/icustays.csv.gz").is_file():
        choices = [p for p in root.iterdir() if (p / "icu/icustays.csv.gz").is_file() and (p / "hosp").is_dir()]
        if len(choices) != 1:
            raise ValueError("Cannot resolve unique Clinical dataset")
        root = choices[0]
    mapping_dir = paths["WORKING_DATA_ROOT"] / "mapping"
    mappings = read_csv(mapping_dir / "waveform_clinical_mapping.csv")
    candidates = read_csv(mapping_dir / "reference_candidates.csv")
    if {r["stay_id"] for r in candidates} != {s[0] for s in SELECTED}:
        raise ValueError("Reviewed reference candidates disagree with this ticket")
    selected = []
    for index, (stay, subject, admission, record) in enumerate(SELECTED, 1):
        candidate = next(c for c in candidates if c["stay_id"] == stay)
        if (candidate["subject_id"], candidate["hadm_id"], candidate["waveform_record_id"]) != (subject, admission, record):
            raise ValueError("Reference candidate identity differs from approved ticket")
        matches = [r for r in mappings if r["stay_id"] == stay and r["waveform_record_id"] == record
                   and r["mapping_status"] == "MATCHED"]
        if len(matches) != 1:
            raise ValueError("Approved representative is not uniquely MATCHED")
        row = dict(matches[0], patient_key=f"P{index:03d}", bed_id=f"BED-{index:03d}")
        integrity = json.loads(row["waveform_integrity"])
        if integrity["status"] != "COMPLETE_METADATA" or integrity["issues"]:
            raise ValueError("INCOMPLETE waveform cannot enter the reference dataset")
        selected.append(row)
    return paths, root, output, selected


def build_patients(root, selected):
    stays = {r["stay_id"]: (n, r) for n, r in source_rows(root / "icu/icustays.csv.gz")}
    patients = {r["subject_id"]: (n, r) for n, r in source_rows(root / "hosp/patients.csv.gz")}
    result = []
    for match in selected:
        n, stay = stays[match["stay_id"]]
        pn, patient = patients[match["subject_id"]]
        if (stay["subject_id"], stay["hadm_id"]) != (match["subject_id"], match["hadm_id"]):
            raise ValueError("Raw ICU identity differs from reviewed match")
        intime, outtime = parse_time(stay["intime"]), parse_time(stay["outtime"])
        if (intime, outtime) != (parse_time(match["icu_intime"]), parse_time(match["icu_outtime"])):
            raise ValueError("Raw ICU interval differs from reviewed match")
        anchor_age = int(patient["anchor_age"]) if patient["anchor_age"] else None
        anchor_year = int(patient["anchor_year"]) if patient["anchor_year"] else None
        age = anchor_age + intime.year - anchor_year if anchor_age is not None and anchor_year is not None else None
        result.append(dict(
            patient_key=match["patient_key"], subject_id=match["subject_id"], hadm_id=match["hadm_id"], stay_id=match["stay_id"],
            gender=patient["gender"] or None, age=age, anchor_age=anchor_age, anchor_year=anchor_year,
            age_is_approximate=True, bed_id_is_simulated=True, care_unit=stay["first_careunit"] or None,
            last_care_unit=stay["last_careunit"] or None, icu_intime=intime, icu_outtime=outtime,
            bed_id=match["bed_id"], dataset_type="REFERENCE", is_virtual=False,
            mapping_status="MATCHED", mapping_reason=match["mapping_reason"],
            source_stay_record_id=source_id("icu/icustays.csv.gz", n),
            source_patient_record_id=source_id("hosp/patients.csv.gz", pn),
        ))
    return result


def build_events(root, patients, run_id):
    items = {r["itemid"]: r for _, r in source_rows(root / "icu/d_items.csv.gz")}
    labs = {r["itemid"]: r for _, r in source_rows(root / "hosp/d_labitems.csv.gz")}
    vitals, clinical_rules = dictionary_rules(items, labs)
    by_stay = {p["stay_id"]: p for p in patients}
    by_subject = {p["subject_id"]: p for p in patients}
    telemetry, clinical, audit = [], [], []
    for file in ("icu/chartevents.csv.gz", "hosp/labevents.csv.gz", "icu/inputevents.csv.gz", "icu/outputevents.csv.gz"):
        table = Path(file).name.split(".")[0]
        for number, row in source_rows(root / file):
            if table == "labevents":
                patient = by_subject.get(row["subject_id"])
            else:
                patient = by_stay.get(row["stay_id"])
            if patient is None:
                continue
            raw_time = row.get("charttime", row.get("starttime", ""))
            record_id = source_id(file, number)
            entry = dict(source_table=table, source_record_id=record_id, source_row_number=number,
                         subject_id=row["subject_id"], hadm_id=row["hadm_id"],
                         stay_id=row.get("stay_id", ""), item_id=row["itemid"], source_time=raw_time,
                         outcome="", reason="", canonical_file="", event_id="")
            audit.append(entry)

            def exclude(reason, rejected=False):
                entry.update(outcome="REJECTED" if rejected else "EXCLUDED", reason=reason)

            if (row["subject_id"], row["hadm_id"]) != (patient["subject_id"], patient["hadm_id"]):
                exclude("Missing/conflicting admission or patient identity")
                continue
            time = parse_time(raw_time)
            if not time:
                exclude("Invalid/missing source observation timestamp", True)
                continue
            intime, outtime = patient["icu_intime"], patient["icu_outtime"]
            end = parse_time(row.get("endtime", ""))
            if table == "inputevents":
                inside = (intime <= time < outtime) if end == time else bool(
                    end and end > time and max(time, intime) < min(end, outtime))
            else:
                inside = intime <= time < outtime
            if not inside:
                exclude("Outside selected ICU interval")
                continue
            entry["stay_id"] = patient["stay_id"]
            signal = vitals.get(row["itemid"]) if table == "chartevents" else None
            category = clinical_rules.get((table, row["itemid"]))
            if signal is None and category is None:
                exclude("Outside documented target item set")
                continue
            provenance = dict(source_table=table, source_record_id=record_id, source_file=file, source_row_number=number)
            common = dict(schema_version=VERSION, event_id=event_id(table, patient["stay_id"], record_id, time, row["itemid"]),
                          run_id=run_id, **{k: patient[k] for k in ("patient_key", "subject_id", "hadm_id", "stay_id")},
                          dataset_type="REFERENCE", test_mode="REFERENCE", scenario_type=None, is_virtual=False)
            if signal:
                value = numeric(row["valuenum"])
                aliases, _ = VITAL_UNITS[signal]
                unit = aliases.get(row["valueuom"].strip())
                if value is None or unit is None:
                    exclude("Vital has missing/nonfinite numeric value or unsupported unit", True)
                    continue
                telemetry.append(dict(
                    **common, **provenance, source_event_time=time, event_time=time, producer_time=None,
                    signal_name=signal, value=value, unit=unit, sequence_number=0, quality_flag="GOOD",
                    source_type="MIMIC_IV_CLINICAL", virtual_patient_id=None,
                    source_item_id=row["itemid"], source_value=row["value"], source_unit=row["valueuom"],
                ))
                entry.update(outcome="EMITTED", reason="Verified numeric vital", canonical_file="telemetry_event.parquet",
                             event_id=common["event_id"])
                continue
            event_type, measurement = category
            if table == "inputevents" and row["statusdescription"] == "Rewritten":
                exclude("Rewritten input event; not accepted as administration", True)
                continue
            definition = labs[row["itemid"]] if table == "labevents" else items[row["itemid"]]
            raw_value = row.get("value", row.get("amount", ""))
            value = numeric(row.get("valuenum", raw_value))
            text = raw_value if raw_value.strip().lower() not in NULLS else None
            unit = row.get("valueuom", row.get("amountuom", "")) or None
            if event_type == "GCS" and unit is None:
                unit = "score"
            if unit == "ml":
                unit = "mL"
            flags = []
            if value is None:
                flags.append("NO_FINITE_NUMERIC_VALUE")
            if value is None and text is None:
                flags.append("MISSING_VALUE")
            available, status = available_time(time, row.get("storetime", ""))
            if available is None:
                flags.append(status)
            clinical.append(dict(
                **common, **provenance, event_type=event_type, item_id=row["itemid"],
                item_name=definition["label"], measurement_name=measurement, chart_time=time,
                available_time=available, availability_status=status, value=value, value_text=text,
                unit=unit, source_native_id=row.get("labevent_id", row.get("orderid")) or None,
                source_data_flag=row.get("flag") or None, source_storetime=parse_time(row.get("storetime", "")),
                source_storetime_text=row.get("storetime") or None, source_end_time=end,
                source_rate=numeric(row.get("rate")), source_rate_unit=row.get("rateuom") or None,
                source_status=row.get("statusdescription") or None, validation_flags=flags,
            ))
            entry.update(outcome="EMITTED", reason="Verified target clinical item", canonical_file="clinical_event.parquet",
                         event_id=common["event_id"])
    telemetry.sort(key=lambda r: (r["patient_key"], r["event_time"], r["signal_name"], r["source_row_number"]))
    sequences = Counter()
    for row in telemetry:
        sequences[row["stay_id"]] += 1
        row["sequence_number"] = sequences[row["stay_id"]]
    clinical.sort(key=lambda r: (r["patient_key"], r["chart_time"], r["source_table"], r["source_row_number"]))
    if any(not r["outcome"] for r in audit):
        raise AssertionError("Source row escaped accounting")
    observed_rules = [
        dict(source_table=r["source_table"], item_id=r.get("source_item_id", r.get("item_id")),
             canonical_name=r.get("signal_name", r.get("measurement_name")), unit=r.get("unit"))
        for r in telemetry + clinical
    ]
    return telemetry, clinical, audit, observed_rules


def build_waveforms(paths, selected, patients, tracked_hashes):
    root = paths["MIMIC_WAVEFORM_ROOT"]
    by_stay = {p["stay_id"]: p for p in patients}
    rows, accounting = [], Counter()
    for match in selected:
        patient = by_stay[match["stay_id"]]
        directory = Path(match["waveform_directory"]).resolve()
        if not directory.is_relative_to(root):
            raise ValueError("Reviewed waveform path escapes configured raw root")
        parent_path = directory / (match["waveform_record_id"] + ".hea")
        tracked_hashes[str(parent_path)] = digest(parent_path)
        parent = wfdb.rdheader(str(parent_path.with_suffix("")), rd_segments=False)
        start = datetime.combine(parent.base_date, parent.base_time)
        if start != parse_time(match["waveform_start_time"]):
            raise ValueError("Parent start time differs from reviewed mapping")
        if abs((start + timedelta(seconds=parent.sig_len / parent.fs) - parse_time(match["waveform_end_time"])).total_seconds()) > 1e-6:
            raise ValueError("Parent duration differs from reviewed mapping")
        for field, expected in (("subject_id", patient["subject_id"]), ("hadm_id", patient["hadm_id"])):
            values = {m.group(1) for c in parent.comments or []
                      if (m := re.fullmatch(field + r"\s+(\d+)", c.strip()))}
            if values != {expected}:
                raise ValueError("Parent WFDB identity is missing/conflicting")
        if not getattr(parent, "n_seg", 0) or sum(parent.seg_len) != parent.sig_len:
            raise ValueError("Expected consistent multi-segment WFDB parent")
        offset = 0
        for name, length in zip(parent.seg_name, parent.seg_len):
            if name == "~":
                accounting["gap_frames"] += length
                offset += length
                continue
            header = (directory / (name + ".hea")).resolve()
            if not header.is_relative_to(root):
                raise ValueError("Segment header escapes raw root")
            tracked_hashes[str(header)] = digest(header)
            segment = wfdb.rdheader(str(header.with_suffix("")), rd_segments=False)
            if segment.record_name != name or segment.sig_len != length or segment.fs != parent.fs:
                raise ValueError("Segment metadata conflicts with parent")
            if length == 0:
                accounting["layout_headers_excluded"] += 1
                continue
            segment_start = start + timedelta(seconds=offset / parent.fs)
            segment_end = start + timedelta(seconds=(offset + length) / parent.fs)
            first, stop = frame_window(segment_start, length, parent.fs, patient["icu_intime"], patient["icu_outtime"])
            for index, (signal, filename, unit, factor) in enumerate(zip(
                    segment.sig_name, segment.file_name, segment.units, segment.samps_per_frame)):
                accounting["source_signal_segments"] += 1
                if filename == "~":
                    accounting["null_signal_segments_excluded"] += 1
                    continue
                data = (directory / filename).resolve()
                if not data.is_relative_to(root) or not data.is_file() or data.stat().st_size == 0:
                    raise ValueError("Required data reference missing/empty; no partial-record salvage")
                if stop <= first:
                    accounting["outside_icu_signal_segments_excluded"] += 1
                    continue
                if factor <= 0:
                    raise ValueError("Invalid samples per frame")
                rows.append(dict(
                    **{k: patient[k] for k in ("patient_key", "subject_id", "hadm_id", "stay_id")},
                    waveform_record_id=match["waveform_record_id"], waveform_segment_id=name,
                    signal_name=signal, source_start_time=segment_start + timedelta(seconds=first / parent.fs),
                    source_end_time=segment_end if stop == length else segment_start + timedelta(seconds=stop / parent.fs),
                    raw_segment_start_time=segment_start, raw_segment_end_time=segment_end,
                    sampling_rate_hz=parent.fs * factor, frame_frequency_hz=parent.fs,
                    unit=unit or None, source_header_path=str(header), source_parent_header_path=str(parent_path),
                    source_data_reference=str(data), channel_index=index, samples_per_frame=factor,
                    source_frame_start=first, source_frame_end=stop,
                    source_sample_start=first * factor, source_sample_end=stop * factor,
                    dataset_type="REFERENCE", is_virtual=False,
                ))
                accounting["manifest_rows"] += 1
            offset += length
        if offset != parent.sig_len:
            raise ValueError("Parent frame accounting failed")
    if accounting["source_signal_segments"] != sum(accounting[key] for key in (
            "manifest_rows", "null_signal_segments_excluded", "outside_icu_signal_segments_excluded")):
        raise AssertionError("Waveform segment disappeared without accounting")
    rows.sort(key=lambda r: (r["patient_key"], r["source_start_time"], r["waveform_segment_id"], r["channel_index"]))
    return rows, dict(accounting)


def validate(tables, audit, patients, waveform_counts):
    expected = {p["stay_id"]: p for p in patients}
    reports = {}
    emitted = Counter(r["canonical_file"] for r in audit if r["outcome"] == "EMITTED")
    for name, table in tables.items():
        rows = table.to_pylist()
        for field in table.schema:
            if not field.nullable and table.column(field.name).null_count:
                raise AssertionError(f"Required null field: {name}/{field.name}")
        for row in rows:
            patient = expected[row["stay_id"]]
            if any(row[key] != patient[key] for key in ("patient_key", "subject_id", "hadm_id")):
                raise AssertionError("Canonical foreign key mismatch")
            if row["dataset_type"] != "REFERENCE" or row["is_virtual"]:
                raise AssertionError("Non-reference row in output")
        ids = [r["event_id"] for r in rows] if "event_id" in table.column_names else None
        duplicate_ids = len(ids) - len(set(ids)) if ids is not None else None
        if duplicate_ids:
            raise AssertionError("Duplicate canonical event_id")
        time_ranges = {}
        for field in table.schema:
            if pa.types.is_timestamp(field.type):
                values = [r[field.name] for r in rows if r[field.name] is not None]
                time_ranges[field.name] = dict(min=min(values).isoformat() if values else None,
                                              max=max(values).isoformat() if values else None)
        reports[name] = dict(
            row_count=len(rows), unique_patients=len({r["subject_id"] for r in rows}),
            unique_stay_ids=len({r["stay_id"] for r in rows}),
            required_null_counts={f.name: table.column(f.name).null_count for f in table.schema if not f.nullable},
            all_field_null_counts={f.name: table.column(f.name).null_count for f in table.schema},
            timestamp_ranges=time_ranges, duplicate_event_id_count=duplicate_ids,
            signals=sorted({r["signal_name"] for r in rows}) if "signal_name" in table.column_names else [],
            event_types=sorted({r["event_type"] for r in rows}) if "event_type" in table.column_names else [],
        )
        if name in emitted and emitted[name] != len(rows):
            raise AssertionError("Emitted source count differs from output count")
    if len(tables["patient_stay.parquet"]) != len(SELECTED):
        raise AssertionError("Patient/stay count changed")
    telemetry = tables["telemetry_event.parquet"].to_pylist()
    for row in telemetry:
        if row["source_event_time"] != row["event_time"] or row["producer_time"] is not None:
            raise AssertionError("Offline timestamp rules violated")
        if row["quality_flag"] != "GOOD" or row["signal_name"] not in VITAL_UNITS or not math.isfinite(row["value"]):
            raise AssertionError("Invalid numeric telemetry")
        patient = expected[row["stay_id"]]
        if not patient["icu_intime"] <= row["event_time"] < patient["icu_outtime"]:
            raise AssertionError("Telemetry outside ICU interval")
        if row["test_mode"] != "REFERENCE" or row["scenario_type"] is not None or row["virtual_patient_id"] is not None:
            raise AssertionError("Reference telemetry flags invalid")
    for stay in expected:
        numbers = [r["sequence_number"] for r in telemetry if r["stay_id"] == stay]
        if numbers != list(range(1, len(numbers) + 1)):
            raise AssertionError("Sequence numbers are not deterministic contiguous stay sequences")
    for row in tables["clinical_event.parquet"].to_pylist():
        if row["available_time"] is not None and row["available_time"] < row["chart_time"]:
            raise AssertionError("Premature clinical availability")
        if row["test_mode"] != "REFERENCE" or row["scenario_type"] is not None:
            raise AssertionError("Reference Clinical flags invalid")
        if row["event_type"] not in {"LAB", "GCS", "VASOPRESSOR", "URINE"}:
            raise AssertionError("Unexpected clinical type")
    wave = tables["waveform_manifest.parquet"].to_pylist()
    keys = {(r["source_header_path"], r["channel_index"], r["source_frame_start"], r["source_frame_end"]) for r in wave}
    if len(keys) != len(wave) or len(wave) != waveform_counts["manifest_rows"]:
        raise AssertionError("Duplicate or lost waveform manifest entry")
    for row in wave:
        patient = expected[row["stay_id"]]
        if not patient["icu_intime"] <= row["source_start_time"] < row["source_end_time"] <= patient["icu_outtime"]:
            raise AssertionError("Manifest interval outside ICU")
        if row["source_sample_end"] - row["source_sample_start"] != (
                row["source_frame_end"] - row["source_frame_start"]) * row["samples_per_frame"]:
            raise AssertionError("Manifest frame/sample conversion invalid")
    reports["source_accounting"] = {
        table: dict(scoped_rows=sum(r["source_table"] == table for r in audit),
                    outcomes=dict(Counter(r["outcome"] + ": " + r["reason"] for r in audit if r["source_table"] == table)))
        for table in sorted({r["source_table"] for r in audit})
    }
    reports["waveform_accounting"] = waveform_counts
    if sum(emitted.values()) != len(telemetry) + len(tables["clinical_event.parquet"]):
        raise AssertionError("Source-to-canonical reconciliation failed")
    if len({r["event_id"] for r in audit if r["outcome"] == "EMITTED"}) != sum(emitted.values()):
        raise AssertionError("Source event IDs are not one-to-one")
    return reports


def summary(output, patients, tables, report, run_id, rerun, rules):
    lines = [
        "# Canonical MIMIC Reference Dataset", "", f"Schema version: {VERSION}; deterministic build/run_id: `{run_id}`", "",
        "Build: `.venv/Scripts/python.exe -B scripts/reference/build_reference.py`",
        "Tests: `.venv/Scripts/python.exe -B -m unittest discover -s scripts/reference -p test_reference.py -v`", "",
        "## Selected patients and stays", "",
        "| Key | Subject | Admission | Stay | Representative waveform | Simulated bed | Approximate age |",
        "| --- | --- | --- | --- | --- | --- | ---: |",
    ]
    for patient, selection in zip(patients, SELECTED):
        lines.append(f"| {patient['patient_key']} | {patient['subject_id']} | {patient['hadm_id']} | {patient['stay_id']} | "
                     f"{selection[3]} | {patient['bed_id']} | {patient['age']} |")
    lines += ["", "## Canonical files and validation", "",
              "| Output | Rows | Unique patients | Unique stays | Duplicate event IDs | Required nulls |",
              "| --- | ---: | ---: | ---: | ---: | ---: |"]
    for name in PARQUETS:
        stats = report[name]
        lines.append(f"| {name} | {stats['row_count']} | {stats['unique_patients']} | {stats['unique_stay_ids']} | "
                     f"{stats['duplicate_event_id_count'] if stats['duplicate_event_id_count'] is not None else 'N/A'} | "
                     f"{sum(stats['required_null_counts'].values())} |")
    lines += ["", "All field null counts, required-field null counts, timestamp ranges, signals/types, and source reconciliation "
              "are in reference_validation.json. Nullable timing/provenance fields remain null where unavailable.",
              f"Rerun equality against previously written Parquet tables: {rerun}.",
              "Every written Parquet table is read back and compared with the validated in-memory Arrow table including schema metadata.", "",
              "## Available data per stay", "",
              "| Stay | Numeric vital signals | Clinical event types | Waveform signals |",
              "| --- | --- | --- | --- |"]
    for patient in patients:
        stay = patient["stay_id"]
        values = []
        for name, field in (("telemetry_event.parquet", "signal_name"), ("clinical_event.parquet", "event_type"),
                            ("waveform_manifest.parquet", "signal_name")):
            values.append(", ".join(sorted({r[field] for r in tables[name].to_pylist() if r["stay_id"] == stay})))
        lines.append(f"| {stay} | " + " | ".join(values) + " |")
    lines += [
        "", "## Transformation and provenance rules", "",
        "- Scope is exactly the three reviewed stays; the three named representative waveform records only. "
        "The alternative MATCHED record 85583557 is intentionally outside this initial representative-only build.",
        "- Patient keys and simulated bed IDs follow the reviewed ticket order: P001/BED-001, P002/BED-002, P003/BED-003. "
        "bed_id is project-simulated, not a MIMIC bed identity.",
        "- age = anchor_age + ICU admission year - anchor_year, when both anchors exist. This is an approximate "
        "year-based age, not an exact birthday-based age. Raw anchors and age_is_approximate are preserved. "
        "care_unit uses first_careunit; last_care_unit is retained separately.",
        "- CSV files are streamed; selected scoped rows are recorded individually in source_row_audit.csv, "
        "including excluded/rejected rows and their reasons. Only selected results are held in memory.",
        "- A source_record_id is relative Clinical CSV path + #record=N, where N is the one-based CSV data-record "
        "ordinal excluding the header (not a physical line number). source_row_number/source_file preserve the lookup. "
        "Native labevent_id/orderid are additionally retained for Clinical events.",
        "- event_id = evt- + SHA256(JSON array [schema_version, source_table, stay_id, source_record_id, "
        "original timestamp ISO string, itemid]). Distinct raw records remain distinct even when their values/times match. "
        "No de-duplication, averaging, interpolation or consolidation of chart/lab observations.",
        "- run_id is a deterministic hash of schema version, approved selection, builder code hash, "
        "Clinical input byte hashes and reviewed mapping/candidate CSV hashes; no current time or random UUID.",
        "- Numeric telemetry comes from explicitly named vital d_items labels in chartevents. RR uses measured "
        "Respiratory Rate, excluding ventilator set/total/spontaneous variants. Blood pressure methods are not merged; "
        "source_item_id identifies invasive/noninvasive/manual observations.",
        "- HR is bpm; blood pressures mmHg; SPO2 percent; RR insp/min is renamed breaths/min without scaling. "
        "Only finite valuenum and verified source units pass basic validation to GOOD. No artifact/BR-7 classification.",
        "- Clinical LAB items include matched blood d_labitems and charted corresponding lab labels. "
        "Bilirubin fractions, GCS component labels and individual source items remain distinct. GCS unit is score; "
        "ml is normalized to mL. Other Clinical units and original value_text are preserved.",
        "- Lab association requires exact subject_id/hadm_id and charttime within the selected ICU interval. "
        "ICU chart/output events require matching subject_id/hadm_id/stay_id and ICU charttime; input events require "
        "the same IDs and a positive ICU interval overlap (or an in-ICU bolus).",
        "- VASOPRESSOR selection uses inputevents labels for norepinephrine, epinephrine, vasopressin, "
        "phenylephrine and dopamine; rewritten orders are rejected. Other infusions/drugs are not relabeled vasopressors.",
        "- URINE selects verified outputevents collection labels, including Foley and Void. Irrigant/urine mixtures "
        "and unrelated outputs are excluded, not interpreted as pure urine.",
        "- All timestamps retain original deidentified MIMIC wall times in timestamp[us], with unspecified timezone "
        "and no shifting. source_event_time equals event_time; producer_time remains null. Sequence numbers run from "
        "1 per stay after stable sorting by event_time, signal_name and raw record ordinal.",
        "- available_time is original valid storetime >= chart_time. Missing/malformed/earlier storetime yields null "
        "available_time plus an explicit availability_status/validation flag; charttime is never substituted for unknown "
        "lab availability. Consumers must withhold unknown-availability results and never expose a result before available_time.",
        "- Waveform segment times are derived from parent start + cumulative declared frame counts/frame frequency, "
        "including gaps. Layout headers and null segments carry no observations and are excluded with accounting. "
        "Manifest intervals are restricted to complete frames within the ICU interval.",
        "- sampling_rate_hz = frame_frequency_hz x samples_per_frame. The manifest retains segment channel index, "
        "frame and sample offsets, source header and source data path. WFDB headers remain necessary to decode shared/"
        "compressed data files; no .hea/.dat files are copied and no samples are decoded.",
        "", "## Source reconciliation", "",
    ]
    for table, counts in report["source_accounting"].items():
        lines.append(f"- {table}: {counts['scoped_rows']} scoped source rows; outcomes {json.dumps(counts['outcomes'], sort_keys=True)}.")
    lines += [f"- Waveform metadata: {json.dumps(report['waveform_accounting'], sort_keys=True)}.",
              "", "### Actual observed source items and units", "",
              "| Source table | Item ID | Canonical measurement | Unit |", "| --- | --- | --- | --- |"]
    unique_rules = sorted({(r["source_table"], r["item_id"], r["canonical_name"], r["unit"] or "") for r in rules})
    lines.extend("| " + " | ".join(rule) + " |" for rule in unique_rules)
    lines += ["", "## Known missing data and limitations", "",
              "- Stay 37323533 has strong ECG/ABP/Pleth waveform coverage, but limited lab coverage and no verified urine output or lactate in this ICU interval.",
              "- Stay 35727289 has the strongest Clinical coverage of the selected stays, but no ABP waveform.",
              "- Stay 39864867 is a secondary/missing-data case, with no ABP waveform or verified bilirubin in this ICU interval.",
              "- Verified PaO2 is unavailable for all three selected stays; unspecified/venous blood pO2 is not PaO2 and is not included. Missing values are never filled with zero.",
              "- No selected vasopressor input rows were verified. Other inputevents do not establish vasopressor availability.",
              "- Full SOFA cannot be assumed; no SOFA or clinical risk score is calculated. No clinical validity is claimed.",
              "- Waveform usability is based on headers and nonempty referenced files; decoding, sample corruption and artifact/quality assessments are deferred.",
              f"- Raw checks: {report['raw_integrity']['file_count']:,} files; zero membership/size/mtime changes. "
              f"{report['raw_integrity']['hashed_input_count']} Clinical/header inputs additionally have unchanged SHA256. "
              "Raw .dat files are checked by metadata only; waveform samples are not opened.",
              "", "## Unexpected source-data problems", ""]
    rejected = [r for r in read_csv(output / "source_row_audit.csv") if r["outcome"] == "REJECTED"]
    flagged = Counter(flag for row in tables["clinical_event.parquet"].to_pylist() for flag in row["validation_flags"])
    lines += [f"- Rejected scoped source rows: {len(rejected)}; full reasons and source IDs in source_row_audit.csv.",
              f"- Clinical validation flags: {dict(flagged)}.",
              "", "Stopped after canonical dataset creation. No replay producer or streaming/serving infrastructure implemented.", ""]
    (output / "reference_dataset_summary.md").write_text("\n".join(lines), encoding="utf-8")


def main():
    paths, root, output, selected = load_context()
    before = raw_snapshot(paths)
    inventory = read_csv(paths["WORKING_DATA_ROOT"] / "inventory/clinical_inventory.csv")
    tracked = {str((root / r["file"]).resolve()): digest(root / r["file"]) for r in inventory}
    build_inputs = dict(version=VERSION, selection=SELECTED, builder_sha256=digest(Path(__file__)),
                        clinical_sha256={r["file"]: tracked[str((root / r["file"]).resolve())] for r in inventory},
                        mapping_sha256=digest(paths["WORKING_DATA_ROOT"] / "mapping/waveform_clinical_mapping.csv"),
                        candidates_sha256=digest(paths["WORKING_DATA_ROOT"] / "mapping/reference_candidates.csv"))
    run_id = "REFERENCE-" + stable_hash(build_inputs)[:24]
    patients = build_patients(root, selected)
    telemetry, clinical, audit, rules = build_events(root, patients, run_id)
    manifest, wave_counts = build_waveforms(paths, selected, patients, tracked)
    specs = schemas(run_id)
    raw_tables = dict(zip(PARQUETS, (patients, telemetry, clinical, manifest)))
    tables = {name: pa.Table.from_pylist(rows, schema=specs[name]) for name, rows in raw_tables.items()}
    report = validate(tables, audit, patients, wave_counts)
    after = raw_snapshot(paths)
    if before != after or any(digest(Path(path)) != value for path, value in tracked.items()):
        raise RuntimeError("Raw files changed during transformation; outputs not written")
    report["raw_integrity"] = dict(file_count=len(before), metadata_changes=0,
                                   hashed_input_count=len(tracked), changed_sha256_count=0)
    rerun = "NOT_YET_TESTED (first build)"
    existing = [name for name in PARQUETS if (output / name).exists()]
    if len(existing) == len(PARQUETS):
        for name in PARQUETS:
            if not pq.read_table(output / name).equals(tables[name], check_metadata=True):
                raise RuntimeError(f"Previous canonical table differs: {name}; review before replacing")
        rerun = "PASS (all four tables logically identical, including schema metadata)"
    elif existing:
        rerun = "NOT_YET_TESTED (rebuilding partial previous output)"
    report["rerun_equality"] = rerun
    report["build_inputs"] = build_inputs
    output.mkdir(parents=True, exist_ok=True)
    # Verify the complete set before replacing any final Parquet file.
    for name, table in tables.items():
        staged = output / (name + ".building")
        pq.write_table(table, staged, compression="snappy", version="2.6")
        if not pq.read_table(staged).equals(table, check_metadata=True):
            raise RuntimeError(f"Parquet readback differs: {name}")
    for name in tables:
        (output / (name + ".building")).replace(output / name)
    with (output / "source_row_audit.csv").open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(audit[0]))
        writer.writeheader()
        writer.writerows(audit)
    (output / "reference_validation.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    summary(output, patients, tables, report, run_id, rerun, rules)
    print("Canonical rows:", {name: len(table) for name, table in tables.items()}, flush=True)
    print("Rerun equality:", rerun, flush=True)
    print("Raw files unchanged; Parquet readback and source accounting passed.", flush=True)
    print("Saved", output, flush=True)


if __name__ == "__main__":
    main()
