"""Decision and coverage checks using synthetic data; no raw-file writes."""
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_mapping as mapping


class MappingTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2150, 1, 1)
        self.end = self.start + timedelta(hours=2)
        self.stay = dict(subject_id="10000001", hadm_id="20000001", stay_id="30000001",
                         intime=self.start.isoformat(), outtime=self.end.isoformat())
        self.admission = dict(subject_id="10000001", hadm_id="20000001")

    def decide(self, **changes):
        args = dict(subject="10000001", header_subjects={"10000001"},
                    header_admissions={"20000001"}, start=self.start, end=self.end,
                    patients={"10000001"}, admissions=[self.admission], stays=[self.stay])
        args.update(changes)
        return mapping.identity_mapping(**args)

    def test_identity_decisions(self):
        other_stay = dict(self.stay, stay_id="30000002",
                          intime=(self.end + timedelta(hours=1)).isoformat(),
                          outtime=(self.end + timedelta(hours=3)).isoformat())
        cases = [
            ({}, "MATCHED"),
            (dict(header_subjects=set(), header_admissions=set()), "MATCHED"),
            (dict(stays=[self.stay, other_stay]), "MATCHED"),
            (dict(stays=[self.stay, other_stay], end=self.end + timedelta(hours=2)), "AMBIGUOUS"),
            (dict(start=self.end, end=self.end + timedelta(hours=1)), "UNMATCHED"),
            (dict(patients=set()), "UNMATCHED"),
            (dict(stays=[]), "UNMATCHED"),
            (dict(header_subjects={"10000002"}), "AMBIGUOUS"),
            (dict(header_admissions={"20000001", "20000002"}), "AMBIGUOUS"),
            (dict(header_admissions={"20000002"}), "AMBIGUOUS"),
            (dict(admissions=[dict(self.admission, subject_id="10000002")]), "AMBIGUOUS"),
            (dict(admissions=[]), "AMBIGUOUS"),
            (dict(start=None), "AMBIGUOUS"),
            (dict(end=self.start), "AMBIGUOUS"),
            (dict(stays=[dict(self.stay, outtime="")]), "AMBIGUOUS"),
        ]
        for changes, expected in cases:
            with self.subTest(changes=changes):
                status, _, candidates = self.decide(**changes)
                self.assertEqual(status, expected)
                if status == "MATCHED":
                    self.assertEqual(candidates[0]["stay_id"], "30000001")
                    self.assertGreater(candidates[0]["overlap_seconds"], 0)

    def test_incomplete_precedence_and_no_forced_stay(self):
        identity, reason, candidates = self.decide()
        for original in (identity, "UNMATCHED", "AMBIGUOUS"):
            status, _, assigned = mapping.final_mapping(original, reason, candidates, {"issues": ["missing file"]})
            self.assertEqual(status, "INCOMPLETE")
            self.assertEqual(assigned, {})
        status, _, assigned = mapping.final_mapping(identity, reason, candidates, {"issues": []})
        self.assertEqual(status, "MATCHED")
        self.assertEqual(assigned["stay_id"], "30000001")

    def test_recording_crosses_two_stays_even_with_admission_hint(self):
        conflicting = dict(self.stay, stay_id="30000002", hadm_id="20000002")
        self.assertEqual(self.decide(stays=[self.stay, conflicting])[0], "AMBIGUOUS")

    def test_paO2_requires_arterial_evidence_and_explicit_lab_admission(self):
        target = dict(self.stay, mapping_status="MATCHED", icu_intime=self.stay["intime"],
                      icu_outtime=self.stay["outtime"])
        def lab(itemid, specimen, value, hadm="20000001"):
            return dict(subject_id="10000001", hadm_id=hadm, itemid=itemid, specimen_id=specimen,
                        charttime=self.start.isoformat(), value=value,
                        valuenum=value if itemid == "50821" else "")
        tables = {
            "icu/d_items.csv.gz": [dict(itemid="220224", label="Arterial O2 pressure", linksto="chartevents")],
            "hosp/d_labitems.csv.gz": [
                dict(itemid="50821", label="pO2", fluid="Blood"),
                dict(itemid="52033", label="Specimen Type", fluid="Blood"),
            ],
            "icu/chartevents.csv.gz": [
                dict(subject_id="10000001", hadm_id="20000001", stay_id="30000001",
                     charttime=self.start.isoformat(), itemid="220224", value="100", valuenum="100"),
            ],
            "icu/inputevents.csv.gz": [], "icu/outputevents.csv.gz": [],
            "hosp/labevents.csv.gz": [
                lab("50821", "A", "100"), lab("52033", "A", "ART."),
                lab("50821", "B", "50"), lab("52033", "B", "VEN."),
                lab("50821", "C", "80"),
                lab("50821", "D", "90", hadm=""), lab("52033", "D", "ART.", hadm=""),
                lab("50821", "E", "70", hadm="99999999"),
            ],
        }
        with patch.object(mapping, "read_csv", side_effect=lambda p: iter(tables[p.as_posix()])):
            coverage, diagnostics = mapping.clinical_coverage([target], Path("."), [], [self.stay])
        self.assertEqual(coverage["30000001"]["PaO2"]["rows"], 2)
        self.assertEqual(coverage["30000001"]["PaO2"]["numeric_rows"], 2)
        self.assertEqual(diagnostics["30000001"]["blood_pO2_rows"], 3)
        self.assertEqual(diagnostics["30000001"]["blood_pO2_confirmed_arterial_rows"], 1)
        self.assertGreater(diagnostics["30000001"]["labs_without_hadm_in_icu_interval"], 0)

    def test_no_match_produces_no_candidates(self):
        self.assertEqual(mapping.select_candidates([], {}, {}), [])


if __name__ == "__main__":
    unittest.main()

