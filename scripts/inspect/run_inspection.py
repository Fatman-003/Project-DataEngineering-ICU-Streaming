"""Phase 0 only: stream selected Clinical CSVs and inspect local WFDB headers.

Run from project root: .venv/Scripts/python.exe -B scripts/inspect/run_inspection.py
Uses the standard library and the existing wfdb installation.
"""
import csv
import gzip
import json
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import wfdb

PROJECT = Path(__file__).resolve().parents[2]
TABLES = (
    "icu/icustays.csv.gz", "icu/chartevents.csv.gz", "icu/d_items.csv.gz",
    "icu/inputevents.csv.gz", "icu/outputevents.csv.gz", "hosp/patients.csv.gz",
    "hosp/admissions.csv.gz", "hosp/labevents.csv.gz", "hosp/d_labitems.csv.gz",
)
IDS = ("subject_id", "hadm_id", "stay_id", "itemid")
NULLS = {"", "null", "na", "n/a", "nan"}
PATTERNS = {
    "HR": r"^heart rate$",
    "MAP": r"blood pressure.*mean",
    "SBP": r"blood pressure.*systolic",
    "SpO2": r"^(?:oxygen|o2) saturation pulseoxymetry$|^spo2$",
    "RR": r"^respiratory rate(?:\s*\(.*\))?$",
    "GCS": r"^gcs -",
    "Lactate": r"^lactate$|^lactic acid$",
    "Creatinine": r"^creatinine(?:, whole blood| \(serum\)| \(whole blood\))?$",
    "Platelet": r"^z?platelet count$",
    "Bilirubin": r"^(?:total |direct )?bilirubin(?:, (?:total|direct|indirect))?$",
    "PaO2 candidate": r"^po2$|^arterial po2$",
}


def load_paths():
    """Read literal .env paths without executing or interpolating its contents."""
    config = {}
    for line in (PROJECT / ".env").read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        key, separator, value = line.partition("=")
        if not separator:
            raise ValueError("Malformed .env assignment")
        value = value.strip()
        if value.startswith(("'", '"')):
            if len(value) < 2 or value[-1] != value[0]:
                raise ValueError(f"Unclosed quote in .env: {key}")
            value = value[1:-1]
        else:
            value = value.split(" #", 1)[0].rstrip()
        config[key.strip()] = value
    paths = {}
    for key in ("MIMIC_CLINICAL_ROOT", "MIMIC_WAVEFORM_ROOT", "WORKING_DATA_ROOT"):
        if not config.get(key):
            raise ValueError(f"Missing .env setting: {key}")
        paths[key] = (PROJECT / config[key]).resolve()
    output = paths["WORKING_DATA_ROOT"] / "inventory"
    if output.resolve() != (PROJECT / "data/working/inventory").resolve():
        raise ValueError("WORKING_DATA_ROOT must resolve to project data/working for this ticket")
    for key in ("MIMIC_CLINICAL_ROOT", "MIMIC_WAVEFORM_ROOT"):
        if output.is_relative_to(paths[key]) or paths[key].is_relative_to(output):
            raise ValueError("Inventory output must not overlap a raw data root")
    return paths, output


def raw_snapshot(paths):
    """Check file membership, sizes and modification times; never open .dat samples."""
    snapshot = {}
    for key in ("MIMIC_CLINICAL_ROOT", "MIMIC_WAVEFORM_ROOT"):
        for path in paths[key].rglob("*"):
            if path.is_file():
                stat = path.stat()
                snapshot[str(path)] = (stat.st_size, stat.st_mtime_ns)
    return snapshot


def write_csv(path, rows, fields):
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list)) else value
                for key, value in row.items()
            })


def inspect_clinical(configured_root, risks):
    root = configured_root
    if root.is_dir() and not (root / "icu").is_dir() and not (root / "hosp").is_dir():
        candidates = [p for p in root.iterdir() if p.is_dir()
                      and (p / "icu").is_dir() and (p / "hosp").is_dir()]
        if len(candidates) == 1:
            root = candidates[0]
            risks.append(f"Clinical .env root contains one nested dataset; inspected {root}. Configuration unchanged.")
    inventories, observed, dictionaries = [], {}, {}
    for table in TABLES:
        path = root / table
        result = dict(file=table, path=str(path), exists=path.is_file(), size_bytes=None,
                      status="missing", row_count=None, rows_scanned=0, columns=[],
                      identifier_columns=[], id_coverage={}, timestamp_columns=[],
                      timestamp_ranges={}, null_counts={}, observed_itemids={}, sample_rows=[],
                      data_types="CSV text; no source schema inferred", error="")
        counts, definitions = Counter(), {}
        if not result["exists"]:
            risks.append(f"Missing Clinical file: {path}")
            inventories.append(result)
            continue
        result["size_bytes"] = path.stat().st_size
        try:
            with gzip.open(path, "rt", encoding="utf-8-sig", newline="") as stream:
                reader = csv.DictReader(stream)
                columns = reader.fieldnames or []
                if not columns or len(set(columns)) != len(columns):
                    raise ValueError("Missing or duplicate CSV column names")
                result["columns"] = columns
                identifiers = [c for c in IDS if c in columns]
                times = [c for c in columns if "time" in c.lower()
                         or c.lower().endswith("date") or c.lower() == "dod"]
                result["identifier_columns"], result["timestamp_columns"] = identifiers, times
                unique = {c: set() for c in identifiers}
                missing = Counter({c: 0 for c in columns})
                ranges = {c: {"min": None, "max": None} for c in times}
                for row in reader:
                    if None in row or any(value is None for value in row.values()):
                        raise ValueError(f"Wrong CSV field count at data row {result['rows_scanned'] + 1}")
                    result["rows_scanned"] += 1
                    if len(result["sample_rows"]) < 2:
                        result["sample_rows"].append(row.copy())
                    for column, value in row.items():
                        if value.strip().lower() in NULLS:
                            missing[column] += 1
                        elif column in unique:
                            unique[column].add(value)
                        elif column in ranges:
                            limits = ranges[column]
                            limits["min"] = min(limits["min"], value) if limits["min"] else value
                            limits["max"] = max(limits["max"], value) if limits["max"] else value
                    itemid = row.get("itemid", "")
                    if itemid.strip().lower() not in NULLS:
                        counts[itemid] += 1
                        if table.endswith(("/d_items.csv.gz", "/d_labitems.csv.gz")):
                            definitions[itemid] = row
                result.update(
                    status="ok", row_count=result["rows_scanned"], null_counts=dict(missing),
                    timestamp_ranges=ranges, observed_itemids=dict(sorted(counts.items())),
                    id_coverage={c: {
                        "unique": len(unique[c]), "non_null_rows": result["rows_scanned"] - missing[c],
                        "null_rows": missing[c], "non_null_fraction":
                        (result["rows_scanned"] - missing[c]) / result["rows_scanned"]
                        if result["rows_scanned"] else None
                    } for c in identifiers},
                )
            if not result["row_count"]:
                risks.append(f"Clinical file has no data rows: {table}")
            for column in identifiers + times + [c for c in ("value", "valuenum", "amount", "rate") if c in columns]:
                if missing[column]:
                    risks.append(f"{table}: {column} missing in {missing[column]:,}/{result['row_count']:,} rows.")
        except (OSError, EOFError, csv.Error, UnicodeError, ValueError) as exc:
            result.update(status="error", error=f"{type(exc).__name__}: {exc}")
            risks.append(f"Clinical scan incomplete: {table}: {result['error']}")
        observed[table], dictionaries[table] = counts, definitions
        inventories.append(result)
        print(f"Clinical {table}: {result['status']}, rows={result['row_count']}", flush=True)
    items = []
    status = {row["file"]: row["status"] for row in inventories}
    for events, dictionary in (("icu/chartevents.csv.gz", "icu/d_items.csv.gz"),
                               ("hosp/labevents.csv.gz", "hosp/d_labitems.csv.gz")):
        for itemid, definition in sorted(dictionaries.get(dictionary, {}).items()):
            if definition.get("linksto", "chartevents") != "chartevents":
                continue
            if "fluid" in definition and definition["fluid"] != "Blood":
                continue
            for metric, pattern in PATTERNS.items():
                if re.search(pattern, definition["label"], re.I):
                    count = observed.get(events, {}).get(itemid, 0)
                    items.append(dict(
                        metric=metric, event_file=events, dictionary_file=dictionary, itemid=itemid,
                        label=definition["label"], fluid=definition.get("fluid", ""),
                        unit=definition.get("unitname", ""), observed_rows=count,
                        event_scan_complete=status[events] == "ok",
                    ))
        unknown = set(observed.get(events, {})) - set(dictionaries.get(dictionary, {}))
        if unknown:
            risks.append(f"{events}: observed item IDs absent from dictionary: {', '.join(sorted(unknown))}")
    for metric in PATTERNS:
        if not any(r["metric"] == metric and r["observed_rows"] for r in items):
            risks.append(f"No observed relevant item found by label search for {metric}; check scan status and label-search limits.")
    risks.append("Item relevance is a label-based inspection, not a validated clinical definition. "
                 "RR includes ventilator set/total/spontaneous variants; bilirubin includes fractions. "
                 "Blood pO2 alone does not establish arterial specimen (PaO2).")
    return root, inventories, items


def inspect_waveform(root, risks):
    if not root.is_dir():
        risks.append(f"Waveform root missing: {root}")
    headers, dat_count, patient_dirs = [], 0, set()
    for path in root.rglob("*"):
        if path.is_dir() and re.fullmatch(r"p\d{8}", path.name):
            patient_dirs.add(path.relative_to(root).as_posix())
        elif path.is_file():
            if path.suffix.lower() == ".hea":
                headers.append(path)
            elif path.suffix.lower() == ".dat":
                dat_count += 1
    records, rows, references = {}, {}, {}
    for path in sorted(headers):
        patient_parts = [p for p in path.relative_to(root).parts[:-1] if re.fullmatch(r"p\d{8}", p)]
        patient = patient_parts[-1] if patient_parts else ""
        row = dict(
            patient=patient, subject_id=patient[1:] if patient else "", record=path.stem,
            recording_id=path.parent.name if re.fullmatch(r"\d{8}", path.parent.name) else path.stem,
            header_path=str(path), role="record", parent_records=[], status="ok", signal_names=[],
            number_of_signals=None, frame_frequency_hz=None, signal_frequencies_hz={},
            start_date="", start_time="", start_timestamp="", duration_seconds=None,
            frame_count=None, segment_count=0, missing_references=[], error="",
        )
        try:
            record = wfdb.rdheader(str(path.with_suffix("")), rd_segments=False)
            records[path] = record
            row.update(number_of_signals=record.n_sig, frame_frequency_hz=record.fs,
                       frame_count=record.sig_len, segment_count=getattr(record, "n_seg", 0))
            if record.fs and record.sig_len is not None:
                row["duration_seconds"] = record.sig_len / record.fs
            if record.base_date:
                row["start_date"] = record.base_date.isoformat()
            if record.base_time:
                row["start_time"] = record.base_time.isoformat()
            if record.base_date and record.base_time:
                row["start_timestamp"] = datetime.combine(record.base_date, record.base_time).isoformat()
            names = getattr(record, "sig_name", None) or []
            row["signal_names"] = names
            row["signal_frequencies_hz"] = {
                name: [record.fs * factor] for name, factor in
                zip(names, getattr(record, "samps_per_frame", None) or [1] * len(names))
            }
            segment_names = getattr(record, "seg_name", None) or []
            segment_lengths = getattr(record, "seg_len", None) or []
            for name, length in zip(segment_names, segment_lengths):
                if name == "~":
                    continue
                child = (path.parent / (name + ".hea")).resolve()
                if not child.is_relative_to(root):
                    row["missing_references"].append(name + " (outside raw root)")
                    continue
                references.setdefault(child, []).append((path, "layout" if length == 0 else "segment"))
                if not child.is_file():
                    row["missing_references"].append(name + ".hea")
            for name in set(getattr(record, "file_name", None) or []):
                if name != "~":
                    data_path = (path.parent / name).resolve()
                    if not data_path.is_relative_to(root) or not data_path.is_file():
                        row["missing_references"].append(name)
        except Exception as exc:
            row.update(status="error", error=f"{type(exc).__name__}: {exc}")
            risks.append(f"Malformed/unreadable WFDB header {path.relative_to(root)}: {row['error']}")
        if row["missing_references"]:
            risks.append(f"{path.relative_to(root)}: missing header/data references: {row['missing_references']}")
        rows[path] = row
    for child, parents in references.items():
        if child in rows:
            rows[child]["role"] = "/".join(sorted({role for _, role in parents}))
            rows[child]["parent_records"] = sorted({parent.stem for parent, _ in parents})
    orphan_groups = set()
    for path, row in rows.items():
        if row["role"] == "record" and re.fullmatch(re.escape(path.parent.name) + r"_\d+", path.stem):
            record = records.get(path)
            is_layout = record is not None and record.sig_len == 0 and all(
                name == "~" for name in (getattr(record, "file_name", None) or [])
            )
            row["role"] = "orphan_layout" if is_layout else "orphan_segment"
            orphan_groups.add(str(path.parent.relative_to(root)))
    for group in sorted(orphan_groups):
        risks.append(f"Unreferenced segment/layout headers in recording directory {group}; "
                     "recording identity inferred from directory/name only; parent header absent or does not reference these headers.")
    # Aggregate only referenced local header metadata; never read signal samples.
    for path, record in records.items():
        if not getattr(record, "n_seg", 0):
            continue
        rates = {}
        for name in record.seg_name:
            child = rows.get((path.parent / (name + ".hea")).resolve())
            if child:
                for signal, frequencies in child["signal_frequencies_hz"].items():
                    rates.setdefault(signal, set()).update(frequencies)
        rows[path]["signal_names"] = sorted(rates)
        rows[path]["signal_frequencies_hz"] = {s: sorted(v) for s, v in sorted(rates.items())}
    inventory = list(rows.values())
    top = [row for row in inventory if row["role"] == "record"]
    summary = dict(
        patient_directory_count=len(patient_dirs), patient_directories=sorted(patient_dirs),
        record_count=len({(row["patient"], row["recording_id"]) for row in inventory}),
        parent_record_header_count=len(top), orphan_header_count=sum(row["role"].startswith("orphan_") for row in inventory),
        orphan_recording_directories=sorted(orphan_groups),
        header_count=len(headers), dat_count=dat_count,
        layout_header_count=sum(row["role"] == "layout" for row in inventory),
        segment_header_count=sum(row["role"] == "segment" for row in inventory),
        signal_names=sorted({name for row in inventory for name in row["signal_names"]}),
        frame_frequencies_hz=sorted({row["frame_frequency_hz"] for row in inventory if row["frame_frequency_hz"]}),
        signal_frequencies_hz=sorted({f for row in inventory for values in row["signal_frequencies_hz"].values() for f in values}),
        records_with_start_timestamp=sum(bool(row["start_timestamp"]) for row in top),
        records_with_duration=sum(row["duration_seconds"] is not None for row in top),
        header_errors=sum(row["status"] == "error" for row in inventory),
        headers_with_missing_references=sum(bool(row["missing_references"]) for row in inventory),
        missing_header_reference_count=sum(name.endswith(".hea") for row in inventory for name in row["missing_references"]),
        missing_data_reference_count=sum(name.endswith(".dat") for row in inventory for name in row["missing_references"]),
        parent_duration_seconds_range=[
            min((row["duration_seconds"] for row in top if row["duration_seconds"] is not None), default=None),
            max((row["duration_seconds"] for row in top if row["duration_seconds"] is not None), default=None),
        ],
    )
    missing_time = sum(not row["start_timestamp"] for row in top)
    if missing_time:
        risks.append(f"{missing_time} top-level waveform records lack a complete start timestamp.")
    if not headers or not dat_count:
        risks.append("Waveform root has no headers or no .dat files.")
    if any(not row["patient"] for row in inventory):
        risks.append("Some waveform headers have no patient directory matching p + 8 digits.")
    risks.append("Segment/layout headers commonly omit absolute start time. No segment timestamps inferred. "
                 "WFDB times have no timezone; retained as recorded.")
    print(f"Waveform: {summary['record_count']} recording groups, {len(top)} parent headers, "
          f"{len(headers)} headers, {dat_count} .dat files", flush=True)
    return inventory, summary


def main():
    paths, output = load_paths()
    before = raw_snapshot(paths)
    risks = []
    resolved, clinical, items = inspect_clinical(paths["MIMIC_CLINICAL_ROOT"], risks)
    waveform, wave_summary = inspect_waveform(paths["MIMIC_WAVEFORM_ROOT"], risks)
    after = raw_snapshot(paths)
    changed = sorted(p for p in before.keys() | after.keys() if before.get(p) != after.get(p))
    if changed:
        risks.append(f"Raw file membership/size/mtime changed during inspection: {changed}")
    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "clinical_inventory.csv", clinical, list(clinical[0]))
    wave_fields = (list(waveform[0]) if waveform else [
        "patient", "subject_id", "record", "recording_id", "header_path", "role", "parent_records", "status",
        "signal_names", "number_of_signals", "frame_frequency_hz", "signal_frequencies_hz",
        "start_date", "start_time", "start_timestamp", "duration_seconds", "frame_count",
        "segment_count", "missing_references", "error",
    ])
    write_csv(output / "waveform_inventory.csv", waveform, wave_fields)
    write_csv(output / "relevant_itemids.csv", items, [
        "metric", "event_file", "dictionary_file", "itemid", "label", "fluid",
        "unit", "observed_rows", "event_scan_complete",
    ])
    lines = [
        "# Phase 0 inspection", "", f"Generated (UTC): {datetime.now(timezone.utc).isoformat()}",
        f"Python: {sys.version.split()[0]}; WFDB: {wfdb.__version__}", "",
        "Command (project root): `.venv/Scripts/python.exe -B scripts/inspect/run_inspection.py`", "",
        "Scope: raw inventory only. No Clinical-Waveform mapping or canonical transformation.",
        "CSV rows streamed individually; exact ID sets retained per file, not full event tables. Two sample rows per file.",
        "CSV has no native types; values are preserved as text. Nulls: empty/whitespace, NULL, NA, N/A, NaN (case insensitive).",
        "Timestamp columns detected by name; Clinical min/max are lexical ranges of raw strings, not timezone conversions.",
        "Raw files opened for reading only; .dat samples never opened. Outputs confined to data/working/inventory.",
        f"Raw metadata check: {len(before):,} files before, {len(after):,} after; {len(changed)} membership/size/mtime changes. "
        "This is a metadata check, not a byte-level checksum.", "",
        "## Clinical", "", f"Configured root: `{paths['MIMIC_CLINICAL_ROOT']}`",
        f"Resolved dataset: `{resolved}`", "",
        f"Files found: {sum(row['exists'] for row in clinical)}/9; complete scans: {sum(row['status'] == 'ok' for row in clinical)}/9.", "",
        "| File | Status | Bytes | Rows | Unique subject / admission / stay | Timestamp columns |",
        "| --- | --- | ---: | ---: | --- | --- |",
    ]
    for row in clinical:
        coverage = " / ".join(str(row["id_coverage"][key]["unique"]) if key in row["id_coverage"] else "N/A"
                              for key in IDS[:3])
        lines.append(f"| {row['file']} | {row['status']} | {row['size_bytes']} | {row['row_count']} | {coverage} | {', '.join(row['timestamp_columns'])} |")
    lines += [
        "", "All columns, per-column null counts, ID non-null counts/fractions, timestamp ranges, observed item frequencies and samples are in clinical_inventory.csv.",
        "", "### Relevant item IDs observed in events", "",
        "Dictionary label candidates are checked against actual event counts. relevant_itemids.csv also includes dictionary candidates with zero observed rows.", "",
        "| Metric | Event table | Item IDs (observed row counts) |", "| --- | --- | --- |",
    ]
    for metric in PATTERNS:
        for table in ("icu/chartevents.csv.gz", "hosp/labevents.csv.gz"):
            found = [f"{row['itemid']} ({row['observed_rows']:,})" for row in items
                     if row["metric"] == metric and row["event_file"] == table and row["observed_rows"]]
            lines.append(f"| {metric} | {table} | {', '.join(found) or 'None found by label search'} |")
    lines += [
        "", "## Waveform", "", f"Root: `{paths['MIMIC_WAVEFORM_ROOT']}`", "",
        "Patient directories match p + 8 digits; subject IDs are derived only from this structure.",
        "Record count is the number of distinct patient/recording groups derived from local record directories. "
        "Parent record headers and orphan segment/layout headers are counted separately; inferred groups do not establish complete recordings.",
        "waveform_inventory.csv contains one row per header, including role and parent record.", "",
    ]
    for key, value in wave_summary.items():
        lines.append(f"- {key}: {json.dumps(value, ensure_ascii=False)}")
    lines += [
        "", "Signal frequency = WFDB frame frequency x samples per frame; duration = frame count / frame frequency. "
        "Parent signal names/rates are collected from referenced headers.",
        "", "## Observations / Risks", "",
    ]
    lines.extend(f"- {risk}" for risk in risks)
    (output / "inspection_summary.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Saved inspection results to {output}", flush=True)
    return 1 if changed or any(row["status"] != "ok" for row in clinical) or wave_summary["header_errors"] or not waveform else 0


if __name__ == "__main__":
    raise SystemExit(main())

