"""TICKET-002: evidence-based raw WFDB-to-ICU mapping; no canonical output.

Run: .venv/Scripts/python.exe -B scripts/mapping/run_mapping.py
"""
import csv
import gzip
import json
import math
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import wfdb

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "scripts/inspect"))
from run_inspection import load_paths, raw_snapshot, write_csv

STATUSES = ("MATCHED", "AMBIGUOUS", "UNMATCHED", "INCOMPLETE")
METRICS = ("HR", "MAP", "SBP", "SpO2", "RR", "GCS", "Lactate",
           "Creatinine", "Platelet", "Bilirubin", "PaO2", "inputevents", "outputevents")
ECG_SIGNALS = {"I", "II", "III", "III+", "V", "V2", "V5", "aVF", "aVL", "aVR", "MCL"}


def read_csv(path):
    with (gzip.open(path, "rt", encoding="utf-8-sig", newline="") if path.suffix == ".gz"
          else path.open(encoding="utf-8-sig", newline="")) as stream:
        yield from csv.DictReader(stream)


def timestamp(value):
    try:
        return datetime.fromisoformat(value) if value else None
    except (ValueError, TypeError):
        return None


def overlap_seconds(start, end, intime, outtime):
    if None in (start, end, intime, outtime):
        return 0.0
    return max(0.0, (min(end, outtime) - max(start, intime)).total_seconds())


def identity_mapping(subject, header_subjects, header_admissions, start, end,
                     patients, admissions, stays):
    """Pure decision logic. Returns status, reason, plausible ICU evidence."""
    if len(header_subjects) > 1 or (header_subjects and header_subjects != {subject}):
        return "AMBIGUOUS", "Directory and header patient identities conflict.", []
    if not subject:
        return "AMBIGUOUS", "No reliable patient identifier.", []
    if len(header_admissions) > 1:
        return "AMBIGUOUS", "Headers contain conflicting admission IDs.", []
    hadm = next(iter(header_admissions), "")
    if hadm and any(a["hadm_id"] == hadm and a["subject_id"] != subject for a in admissions):
        return "AMBIGUOUS", "Header admission belongs to another Clinical patient.", []
    if subject not in patients:
        return "UNMATCHED", "Patient absent from downloaded Clinical patients.csv.gz.", []
    subject_stays = [s for s in stays if s["subject_id"] == subject]
    if not subject_stays:
        return "UNMATCHED", "No downloaded ICU stay for this Clinical patient.", []
    if not start or not end or end <= start:
        return "AMBIGUOUS", "Recording interval is missing or invalid.", []
    candidates = []
    for stay in subject_stays:
        intime, outtime = timestamp(stay["intime"]), timestamp(stay["outtime"])
        if not intime or not outtime or outtime <= intime:
            return "AMBIGUOUS", "Clinical patient has an invalid ICU interval; cannot exclude a competing stay.", []
        seconds = overlap_seconds(start, end, intime, outtime)
        if seconds > 0:
            candidates.append(dict(subject_id=subject, hadm_id=stay["hadm_id"], stay_id=stay["stay_id"],
                                   icu_intime=stay["intime"], icu_outtime=stay["outtime"],
                                   overlap_start=max(start, intime).isoformat(),
                                   overlap_end=min(end, outtime).isoformat(), overlap_seconds=seconds))
    if len(candidates) > 1:
        return "AMBIGUOUS", "Recording overlaps multiple ICU stays; no stay assigned.", candidates
    if not candidates:
        return "UNMATCHED", "No positive recording overlap with a downloaded ICU stay for this patient.", []
    candidate = candidates[0]
    if hadm and candidate["hadm_id"] != hadm:
        return "AMBIGUOUS", "Header admission conflicts with the time-overlapping ICU stay.", candidates
    if not any(a["subject_id"] == subject and a["hadm_id"] == candidate["hadm_id"] for a in admissions):
        return "AMBIGUOUS", "ICU admission cannot be verified in downloaded admissions.csv.gz.", candidates
    evidence = "Directory patient identity"
    if header_subjects:
        evidence += " agrees with header subject_id"
    if hadm:
        evidence += "; header hadm_id agrees with Clinical admission"
    return "MATCHED", evidence + "; positive overlap with exactly one ICU stay.", candidates


def final_mapping(identity_status, reason, candidates, integrity):
    """Integrity takes precedence; never assign a stay to an unusable recording."""
    status = "INCOMPLETE" if integrity["issues"] else identity_status
    if integrity["issues"]:
        reason += " Required waveform files/metadata are incomplete; see waveform_integrity."
    assigned = candidates[0] if status == "MATCHED" else {}
    return status, reason, assigned


def local_reference(directory, name, root):
    path = (directory / name).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f"Header reference escapes waveform root: {name}")
    return path


def inspect_group(directory, patient, record_id, header_paths, root, before):
    """Read each local header once. A data file is only stat'ed, never opened."""
    parent_path = directory / (record_id + ".hea")
    parsed, issues, missing_headers, missing_data, empty_data = {}, [], set(), set(), set()
    subject_ids, admission_ids, rates = set(), set(), defaultdict(set)
    for path in sorted(header_paths):
        try:
            record = wfdb.rdheader(str(path.with_suffix("")), rd_segments=False)
            if record.record_name != path.stem:
                raise ValueError("Header record name disagrees with filename")
            parsed[path] = record
            for comment in record.comments or []:
                for field, target in (("subject_id", subject_ids), ("hadm_id", admission_ids)):
                    match = re.fullmatch(r"\s*" + field + r"\s*[:=]?\s*(\d+)\s*", comment)
                    if match:
                        target.add(match.group(1))
            if not record.fs or not math.isfinite(record.fs) or record.fs <= 0:
                issues.append(f"Invalid frame frequency in {path.name}")
            if record.sig_len is None or record.sig_len < 0:
                issues.append(f"Invalid frame count in {path.name}")
            for name in set(getattr(record, "file_name", None) or []):
                if name == "~":
                    continue
                data = local_reference(directory, name, root)
                stat = before.get(str(data))
                if stat is None:
                    missing_data.add(str(data))
                elif stat[0] == 0:
                    empty_data.add(str(data))
            if getattr(record, "n_seg", 0):
                if record.n_seg != len(record.seg_name) or sum(record.seg_len) != record.sig_len:
                    issues.append(f"Inconsistent segment count/length in {path.name}")
                for name in record.seg_name:
                    if name != "~":
                        child = local_reference(directory, name + ".hea", root)
                        if str(child) not in before:
                            missing_headers.add(str(child))
            elif record.sig_len and record.fs:
                for signal, factor, filename in zip(record.sig_name or [],
                        record.samps_per_frame or [], record.file_name or []):
                    if filename == "~":
                        continue
                    data = local_reference(directory, filename, root)
                    if str(data) in before and before[str(data)][0] > 0:
                        rates[signal].add(record.fs * factor)
        except Exception as exc:
            issues.append(f"{path.name}: {type(exc).__name__}: {exc}")
    parent = parsed.get(parent_path)
    if not parent:
        missing_headers.add(str(parent_path)) if not parent_path.is_file() else None
        issues.append("Parent WFDB header missing or unreadable; start/duration not inferred from segments.")
    if missing_headers:
        issues.append(f"{len(missing_headers)} required header file(s) missing")
    if missing_data:
        issues.append(f"{len(missing_data)} required data file(s) missing")
    if empty_data:
        issues.append(f"{len(empty_data)} required data file(s) empty")
    start, end = None, None
    if parent:
        if parent.base_date is not None and parent.base_time is not None:
            start = datetime.combine(parent.base_date, parent.base_time)
        if parent.fs and parent.fs > 0 and parent.sig_len is not None and parent.sig_len > 0:
            if start:
                end = start + timedelta(seconds=parent.sig_len / parent.fs)
        else:
            issues.append("Parent duration is missing or nonpositive")
    integrity = dict(status="INCOMPLETE" if issues else "COMPLETE_METADATA",
                     parent_header=str(parent_path), header_count=len(header_paths),
                     missing_headers=sorted(missing_headers), missing_data=sorted(missing_data),
                     empty_data=sorted(empty_data), issues=issues,
                     sample_quality="NOT_ASSESSED; no .dat samples read")
    return dict(
        waveform_patient_id=patient, waveform_record_id=record_id,
        waveform_directory=str(directory), waveform_start_time=start.isoformat() if start else "",
        waveform_end_time=end.isoformat() if end else "",
        subject_id=patient[1:], header_subject_ids=sorted(subject_ids), waveform_hadm_ids=sorted(admission_ids),
        hadm_id="", stay_id="", icu_intime="", icu_outtime="", overlap_start="", overlap_end="",
        overlap_seconds="", available_signals=sorted(rates),
        sampling_frequency={s: sorted(values) for s, values in sorted(rates.items())},
        frame_frequency_hz=parent.fs if parent else "",
        mapping_status="", mapping_reason="", identity_mapping_status="",
        candidate_stay_ids=[], candidate_stay_evidence=[], waveform_integrity=integrity,
    ), start, end, subject_ids, admission_ids


def clinical_coverage(mappings, clinical_root, item_rows, stays):
    selected_stays = {row["stay_id"]: row for row in mappings if row["mapping_status"] == "MATCHED"}
    coverage = {stay: {metric: dict(rows=0, numeric_rows=0, itemids=set(), source_rows=Counter())
                       for metric in METRICS} for stay in selected_stays}
    diagnostics = {stay: dict(labs_without_hadm_in_icu_interval=0, blood_pO2_rows=0,
                              blood_pO2_confirmed_arterial_rows=0) for stay in selected_stays}
    if not selected_stays:
        return coverage, diagnostics
    by_subject = defaultdict(list)
    for stay, record in selected_stays.items():
        by_subject[record["subject_id"]].append(stay)
    chart_definitions = {r["itemid"]: r for r in read_csv(clinical_root / "icu/d_items.csv.gz")}
    lab_definitions = {r["itemid"]: r for r in read_csv(clinical_root / "hosp/d_labitems.csv.gz")}
    item_metrics = defaultdict(set)
    for item in item_rows:
        table = item["event_file"]
        definitions = chart_definitions if table == "icu/chartevents.csv.gz" else lab_definitions
        definition = definitions.get(item["itemid"])
        if definition is None or definition["label"] != item["label"]:
            raise ValueError("Relevant-item inventory disagrees with current raw dictionary")
        if item["metric"] in METRICS:
            item_metrics[(table, item["itemid"])].add(item["metric"])
    # Additional explicit arterial label found in the downloaded dictionary.
    for itemid, definition in chart_definitions.items():
        if definition["label"].casefold() == "arterial o2 pressure" and definition["linksto"] == "chartevents":
            item_metrics[("icu/chartevents.csv.gz", itemid)].add("PaO2")
    po2_ids = {itemid for itemid, r in lab_definitions.items()
               if r["label"].casefold() == "po2" and r["fluid"] == "Blood"}
    specimen_ids = {itemid for itemid, r in lab_definitions.items()
                    if r["label"].casefold() == "specimen type" and r["fluid"] == "Blood"}

    def add(stay, metric, row, table):
        evidence = coverage[stay][metric]
        evidence["rows"] += 1
        value = row.get("valuenum", row.get("value", row.get("amount", "")))
        try:
            evidence["numeric_rows"] += int(math.isfinite(float(value)))
        except (ValueError, TypeError):
            pass
        if row.get("itemid"):
            evidence["itemids"].add(row["itemid"])
        evidence["source_rows"][table] += 1

    for table in ("icu/chartevents.csv.gz", "icu/inputevents.csv.gz", "icu/outputevents.csv.gz"):
        for row in read_csv(clinical_root / table):
            stay = row["stay_id"]
            if stay not in selected_stays:
                continue
            target = selected_stays[stay]
            if (row["subject_id"], row["hadm_id"]) != (target["subject_id"], target["hadm_id"]):
                continue
            intime, outtime = timestamp(target["icu_intime"]), timestamp(target["icu_outtime"])
            time = timestamp(row.get("charttime", row.get("starttime", "")))
            if not time:
                continue
            if table == "icu/inputevents.csv.gz":
                end = timestamp(row["endtime"])
                inside = intime <= time < outtime if end == time else bool(
                    end and overlap_seconds(time, end, intime, outtime) > 0)
            else:
                inside = intime <= time < outtime
            if not inside:
                continue
            if table == "icu/chartevents.csv.gz":
                for metric in item_metrics[(table, row["itemid"])]:
                    add(stay, metric, row, table)
            else:
                add(stay, Path(table).name.split(".")[0], row, table)
    specimen_types, po2_rows = defaultdict(set), []
    table = "hosp/labevents.csv.gz"
    for row in read_csv(clinical_root / table):
        subject = row["subject_id"]
        if subject not in by_subject:
            continue
        specimen_key = (subject, row["hadm_id"], row["specimen_id"])
        if row["itemid"] in specimen_ids and row["specimen_id"]:
            specimen_types[specimen_key].add(row["value"].strip().upper())
        time = timestamp(row["charttime"])
        if not time:
            continue
        plausible = [s for s in stays if s["subject_id"] == subject
                     and timestamp(s["intime"]) <= time < timestamp(s["outtime"])]
        if len(plausible) != 1 or plausible[0]["stay_id"] not in selected_stays:
            continue
        stay = plausible[0]["stay_id"]
        if not row["hadm_id"]:
            diagnostics[stay]["labs_without_hadm_in_icu_interval"] += 1
            continue
        if row["hadm_id"] != selected_stays[stay]["hadm_id"]:
            continue
        for metric in item_metrics[(table, row["itemid"])]:
            add(stay, metric, row, table)
        if row["itemid"] in po2_ids:
            diagnostics[stay]["blood_pO2_rows"] += 1
            po2_rows.append((stay, row, specimen_key))
    for stay, row, key in po2_rows:
        if row["specimen_id"] and specimen_types[key] and specimen_types[key] <= {"ART.", "ART", "ARTERIAL"}:
            add(stay, "PaO2", row, table)
            diagnostics[stay]["blood_pO2_confirmed_arterial_rows"] += 1
    for metrics in coverage.values():
        for evidence in metrics.values():
            evidence["itemids"] = sorted(evidence["itemids"])
            evidence["source_rows"] = dict(evidence["source_rows"])
    return coverage, diagnostics


def select_candidates(mappings, coverage, diagnostics):
    matches = [row for row in mappings if row["mapping_status"] == "MATCHED"]

    def rank(row):
        signals = set(row["available_signals"])
        metrics = coverage[row["stay_id"]]
        bundle = int(bool(signals & ECG_SIGNALS) or any(s.startswith("ECG") for s in signals))
        bundle += int("ABP" in signals) + int("Pleth" in signals)
        clinical = sum(bool(e["numeric_rows"]) for e in metrics.values())
        lab_rows = sum(e["source_rows"].get("hosp/labevents.csv.gz", 0) for e in metrics.values())
        return (-bundle, -clinical, -int(lab_rows > 0), -float(row["overlap_seconds"]),
                row["subject_id"], row["waveform_record_id"])

    chosen, seen = [], set()
    for row in sorted(matches, key=rank):
        stay = row["stay_id"]
        if stay in seen:
            continue
        seen.add(stay)
        signals = set(row["available_signals"])
        metric_coverage = coverage[stay]
        candidate = {key: row[key] for key in (
            "waveform_patient_id", "waveform_record_id", "subject_id", "hadm_id", "stay_id",
            "icu_intime", "icu_outtime", "waveform_start_time", "waveform_end_time",
            "overlap_start", "overlap_end", "overlap_seconds", "available_signals", "sampling_frequency",
        )}
        ecg = bool(signals & ECG_SIGNALS) or any(s.startswith("ECG") for s in signals)
        candidate.update(
            rank=len(chosen) + 1, has_ECG=ecg, has_ABP="ABP" in signals, has_Pleth="Pleth" in signals,
            other_matched_record_ids=sorted(r["waveform_record_id"] for r in matches
                                           if r["stay_id"] == stay and r is not row),
            selection_reason=f"Unique identity/admission/time match; all referenced files present and nonempty. "
                             f"ECG={ecg}, ABP={'ABP' in signals}, Pleth={'Pleth' in signals}; "
                             f"{float(row['overlap_seconds']) / 3600:.2f} overlapping ICU hours. "
                             "One representative per distinct stay, ranked by signal bundle, numeric Clinical coverage, "
                             "verified labs, then overlap duration. Signal quality not yet assessed.",
        )
        candidate.update({metric: "AVAILABLE" if metric_coverage[metric]["rows"] else "NO_VERIFIED_ROWS"
                          for metric in METRICS})
        candidate["clinical_coverage_evidence"] = metric_coverage
        candidate["clinical_coverage_limitations"] = diagnostics[stay]
        chosen.append(candidate)
        if len(chosen) == 4:
            break
    return chosen


def main():
    paths, inventory = load_paths()
    output = (paths["WORKING_DATA_ROOT"] / "mapping").resolve()
    if output != (PROJECT / "data/working/mapping").resolve():
        raise ValueError("Mapping output must remain in project data/working/mapping")
    for key in ("MIMIC_CLINICAL_ROOT", "MIMIC_WAVEFORM_ROOT"):
        if output.is_relative_to(paths[key]) or paths[key].is_relative_to(output):
            raise ValueError("Mapping output overlaps a raw root")
    before = raw_snapshot(paths)
    clinical_inventory = list(read_csv(inventory / "clinical_inventory.csv"))
    waveform_inventory = list(read_csv(inventory / "waveform_inventory.csv"))
    item_rows = list(read_csv(inventory / "relevant_itemids.csv"))
    clinical_root = paths["MIMIC_CLINICAL_ROOT"]
    if not (clinical_root / "icu/icustays.csv.gz").is_file():
        candidates = [p for p in clinical_root.iterdir() if p.is_dir()
                      and (p / "icu/icustays.csv.gz").is_file() and (p / "hosp").is_dir()]
        if len(candidates) != 1:
            raise ValueError("Clinical root has no unique nested dataset")
        clinical_root = candidates[0]
    for entry in clinical_inventory:
        path = (clinical_root / entry["file"]).resolve()
        if path != Path(entry["path"]).resolve() or not path.is_file():
            raise ValueError("Clinical inventory paths disagree with .env dataset")
        if path.stat().st_size != int(entry["size_bytes"]):
            raise ValueError("Clinical file size changed since inventory; inspect before mapping")
    patients = {r["subject_id"] for r in read_csv(clinical_root / "hosp/patients.csv.gz")}
    admissions = list(read_csv(clinical_root / "hosp/admissions.csv.gz"))
    stays = list(read_csv(clinical_root / "icu/icustays.csv.gz"))
    if len({s["stay_id"] for s in stays}) != len(stays):
        raise ValueError("Duplicate Clinical stay_id; mapping requires review")
    for stay in stays:
        intime, outtime = timestamp(stay["intime"]), timestamp(stay["outtime"])
        if not intime or not outtime or outtime <= intime:
            raise ValueError("Invalid Clinical ICU interval; mapping requires review")
    root = paths["MIMIC_WAVEFORM_ROOT"]
    groups = defaultdict(set)
    for name in before:
        path = Path(name)
        if not path.is_relative_to(root) or path.suffix.lower() not in (".hea", ".dat"):
            continue
        patient_parts = [p for p in path.relative_to(root).parts[:-1] if re.fullmatch(r"p\d{8}", p)]
        if not patient_parts or not re.fullmatch(r"\d{8}", path.parent.name):
            raise ValueError(f"Unsupported waveform directory structure: {path}")
        key = (path.parent, patient_parts[-1], path.parent.name)
        groups[key]
        if path.suffix.lower() == ".hea":
            groups[key].add(path)
    actual_headers = {str(path) for headers in groups.values() for path in headers}
    if actual_headers != {str(Path(row["header_path"]).resolve()) for row in waveform_inventory}:
        raise ValueError("Waveform header membership changed since inventory; inspect before mapping")
    mappings = []
    for (directory, patient, record_id), header_paths in sorted(groups.items()):
        row, start, end, subject_ids, admission_ids = inspect_group(
            directory, patient, record_id, header_paths, root, before)
        identity_status, reason, candidates = identity_mapping(
            row["subject_id"], subject_ids, admission_ids, start, end, patients, admissions, stays)
        status, reason, assigned = final_mapping(identity_status, reason, candidates, row["waveform_integrity"])
        row.update(mapping_status=status, identity_mapping_status=identity_status, mapping_reason=reason,
                   candidate_stay_ids=[s["stay_id"] for s in candidates], candidate_stay_evidence=candidates)
        for key in ("hadm_id", "stay_id", "icu_intime", "icu_outtime",
                    "overlap_start", "overlap_end", "overlap_seconds"):
            row[key] = assigned.get(key, "")
        mappings.append(row)
    coverage, diagnostics = clinical_coverage(mappings, clinical_root, item_rows, stays)
    candidates = select_candidates(mappings, coverage, diagnostics)
    after = raw_snapshot(paths)
    changed = sorted(p for p in before.keys() | after.keys() if before.get(p) != after.get(p))
    if changed:
        raise RuntimeError("Raw membership/size/mtime changed during mapping; outputs not written")
    assert len(mappings) == len(groups)
    assert all(r["mapping_status"] in STATUSES for r in mappings)
    assert all(bool(r["stay_id"]) == (r["mapping_status"] == "MATCHED") for r in mappings)
    assert all(r["mapping_status"] != "MATCHED" or not r["waveform_integrity"]["issues"] for r in mappings)
    assert len(candidates) <= 4 and len({r["stay_id"] for r in candidates}) == len(candidates)
    if not mappings:
        raise ValueError("No waveform recording groups found")
    output.mkdir(parents=True, exist_ok=True)
    write_csv(output / "waveform_clinical_mapping.csv", mappings, list(mappings[0]))
    candidate_fields = list(candidates[0]) if candidates else [
        "rank", "waveform_patient_id", "waveform_record_id", "subject_id", "hadm_id", "stay_id",
        "selection_reason", *METRICS, "clinical_coverage_evidence", "clinical_coverage_limitations",
    ]
    write_csv(output / "reference_candidates.csv", candidates, candidate_fields)
    counts = Counter(row["mapping_status"] for row in mappings)
    missing_headers = {p for r in mappings for p in r["waveform_integrity"]["missing_headers"]}
    missing_data = {p for r in mappings for p in r["waveform_integrity"]["missing_data"]}
    missing_parents = {r["waveform_integrity"]["parent_header"] for r in mappings
                       if r["waveform_integrity"]["parent_header"] in missing_headers}
    lines = [
        "# TICKET-002: Clinical / Waveform mapping", "",
        f"Generated UTC: {datetime.now(timezone.utc).isoformat()}",
        f"Python {sys.version.split()[0]}; WFDB {wfdb.__version__}", "",
        "Run from project root: `.venv/Scripts/python.exe -B scripts/mapping/run_mapping.py`", "",
        f"Clinical configured root: `{paths['MIMIC_CLINICAL_ROOT']}`",
        f"Clinical resolved dataset: `{clinical_root}`", f"Waveform root: `{root}`", "",
        "## Mapping results", "", "| Status | Recording groups |", "| --- | ---: |",
        *[f"| {status} | {counts[status]} |" for status in STATUSES], "",
        f"Total: {len(mappings)} recording groups; {len(patients)} downloaded Clinical patients; {len(stays)} ICU stays.",
        f"Distinct MATCHED stays: {len(coverage)}. Selected reference stays: {len(candidates)}.", "",
        "## Decision rules", "",
        "- One output row per actual patient/recording directory; segment/layout headers are checked within that group.",
        "- Directory subject_id must agree with any header subject_id. All available header admission IDs must agree.",
        "- MATCHED requires a downloaded patient and admission, positive overlap with exactly one ICU stay, "
        "no header/admission conflict, and complete required file metadata.",
        "- Intervals use [start, end); touching boundaries have zero overlap. Times are compared as recorded, "
        "with no timezone conversion, time shifting, nearest-stay fallback, or extrapolation from orphan segments.",
        "- INCOMPLETE takes precedence over identity status if a parent/child header or referenced data file "
        "is missing, unreadable/invalid, or empty. This conservative whole-record policy does not salvage partial segments.",
        "- identity_mapping_status and candidate_stay_evidence preserve possible identity/time associations "
        "for INCOMPLETE records; stay_id and assigned ICU fields are populated only for MATCHED.",
        "- available_signals and sampling_frequency come from positive-duration headers with existing nonempty "
        "data references; layout-only signal declarations are excluded. Rates equal frame frequency x samples per frame.",
        "- COMPLETE_METADATA confirms file presence/nonempty metadata only. Compressed data decoding, sample counts, "
        "clinical signal quality, and artifact checks remain unassessed; no .dat samples were opened.", "",
        "## Waveform integrity", "",
        f"- Missing referenced child headers: {len(missing_headers - missing_parents)}.",
        f"- Missing parent headers: {len(missing_parents)}.",
        f"- Missing referenced data files: {len(missing_data)}.",
        f"- Orphan headers from the Phase 0 inventory: {sum(r['role'].startswith('orphan_') for r in waveform_inventory)}.",
        "- Exact missing paths and any additional integrity issues are preserved per group in waveform_integrity.",
        f"- Raw metadata check: {len(before):,} files before/after; zero membership/size/mtime changes. "
        "This is not a byte-level checksum.", "",
        "## Reference candidates", "",
        "At most four distinct stays. Ranking prefers ECG/ABP/Pleth availability, numeric Clinical metric "
        "coverage, verified labs, then longer ICU overlap; no signal is mandatory. Additional matched records "
        "for a selected stay are listed in other_matched_record_ids.", "",
        "| Rank | Subject | Admission | Stay | Representative record | ICU overlap hours | ECG / ABP / Pleth |",
        "| ---: | --- | --- | --- | --- | ---: | --- |",
    ]
    for candidate in candidates:
        lines.append(f"| {candidate['rank']} | {candidate['subject_id']} | {candidate['hadm_id']} | "
                     f"{candidate['stay_id']} | {candidate['waveform_record_id']} | "
                     f"{float(candidate['overlap_seconds']) / 3600:.2f} | "
                     f"{candidate['has_ECG']} / {candidate['has_ABP']} / {candidate['has_Pleth']} |")
    if not candidates:
        lines.append("No reliable complete reference candidate is available; no candidate fabricated.")
    lines += [
        "", "### Actual Clinical coverage", "",
        "Counts cover each entire ICU stay, not just the waveform overlap. Chart/input/output events must "
        "agree on subject_id, hadm_id and stay_id and fall in/overlap the ICU interval. Labs require matching "
        "subject_id and explicit hadm_id, charttime in the ICU interval, and exactly one plausible ICU stay.",
        "Labs with missing hadm_id are excluded from verified coverage and counted separately per stay. "
        "NO_VERIFIED_ROWS means no rows meeting these rules, not proof that a measurement never occurred.",
        "Coverage is presence of raw observations, not a usable SOFA component. Numeric-row counts, observed "
        "item IDs and source-table counts are included in reference_candidates.csv. No SOFA calculation.",
        "PaO2 requires the dictionary's explicit Arterial O2 pressure label in chartevents or blood pO2 with "
        "an exclusively arterial Specimen Type for the same subject/admission/specimen_id. Venous/unspecified "
        "blood pO2 is not counted as PaO2. Specimen Type is looked up from the downloaded d_labitems dictionary.", "",
        "| Stay | " + " | ".join(METRICS) + " |",
        "| --- | " + " | ".join("---:" for _ in METRICS) + " |",
    ]
    for candidate in candidates:
        evidence = candidate["clinical_coverage_evidence"]
        lines.append("| " + candidate["stay_id"] + " | " + " | ".join(str(evidence[m]["rows"]) for m in METRICS) + " |")
    lines += ["", "## Major data limitations", "",
              "- Clinical Demo and downloaded waveform patients have limited identity overlap. "
              "An absent patient is UNMATCHED when waveform integrity is complete, never linked by signal similarity.",
              "- Missing raw files remain in place and unrepaired; affected groups are INCOMPLETE even when identity/time supports a stay.",
              "- Separate matched recordings can map to the same stay; selection uses one representative per stay.",
              "- GCS observations do not establish complete simultaneous components; lab names/fractions and RR variants "
              "remain distinct in item evidence. No clinical scores or canonical event values were generated.",
              "- The .env Clinical root contains a uniquely identifiable nested dataset; .env was not changed."]
    for candidate in candidates:
        lines.append(f"- Stay {candidate['stay_id']} coverage limitations: "
                     f"{json.dumps(candidate['clinical_coverage_limitations'])}")
    lines += ["", "Stopped after mapping and candidate selection. No canonical/reference data, replay producer, "
              "streaming infrastructure, or backend/frontend created.", ""]
    (output / "mapping_summary.md").write_text("\n".join(lines), encoding="utf-8")
    print("Mapping counts:", {status: counts[status] for status in STATUSES}, flush=True)
    print("Reference stays:", [r["stay_id"] for r in candidates], flush=True)
    print(f"Saved {output}", flush=True)


if __name__ == "__main__":
    main()

