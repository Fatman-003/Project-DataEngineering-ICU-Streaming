"""Synthetic checks for canonicalization rules; no raw-file writes."""
import sys
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_reference as reference


class ReferenceTests(unittest.TestCase):
    def test_finite_numeric_values_only(self):
        self.assertEqual(reference.numeric("42.5"), 42.5)
        for value in ("", "nan", "inf", "-inf", "text", None):
            self.assertIsNone(reference.numeric(value))

    def test_availability_never_substitutes_charttime(self):
        chart = datetime(2150, 1, 1, 12)
        self.assertEqual(reference.available_time(chart, "2150-01-01 13:00:00"),
                         (chart + timedelta(hours=1), "VERIFIED_STORETIME"))
        for stored in ("", "invalid", "2150-01-01 11:00:00"):
            available, _ = reference.available_time(chart, stored)
            self.assertIsNone(available)

    def test_ids_preserve_identical_raw_observations(self):
        time = datetime(2150, 1, 1)
        one = reference.event_id("chartevents", "3", "icu/chartevents.csv.gz#record=1", time, "220045")
        self.assertEqual(one, reference.event_id("chartevents", "3", "icu/chartevents.csv.gz#record=1", time, "220045"))
        self.assertNotEqual(one, reference.event_id("chartevents", "3", "icu/chartevents.csv.gz#record=2", time, "220045"))

    def test_manifest_frame_bounds(self):
        start = datetime(2150, 1, 1)
        self.assertEqual(reference.frame_window(start, 100, 10, start + timedelta(seconds=2),
                                                start + timedelta(seconds=5)), (20, 50))
        self.assertEqual(reference.frame_window(start, 100, 10, start - timedelta(seconds=1),
                                                start + timedelta(seconds=20)), (0, 100))
        first, stop = reference.frame_window(start, 100, 10, start + timedelta(seconds=11),
                                            start + timedelta(seconds=20))
        self.assertEqual(first, stop)

    def test_dictionary_selection_does_not_relabel_unrelated_data(self):
        def item(label, table):
            return dict(label=label, linksto=table)
        items = {"1": item("Respiratory Rate", "chartevents"),
                 "2": item("Respiratory Rate (Set)", "chartevents"),
                 "3": item("SpO2 Desat Limit", "chartevents"),
                 "4": item("Norepinephrine", "inputevents"),
                 "5": item("Furosemide (Lasix)", "inputevents"),
                 "6": item("Foley", "outputevents"),
                 "7": item("GU Irrigant/Urine Volume Out", "outputevents"),
                 "8": item("Non Invasive Blood Pressure diastolic", "chartevents")}
        labs = {"10": dict(label="Lactate", fluid="Blood"),
                "11": dict(label="Lactate Dehydrogenase (LD)", fluid="Blood"),
                "12": dict(label="Creatinine", fluid="Urine"),
                "13": dict(label="pO2", fluid="Blood")}
        vitals, clinical = reference.dictionary_rules(items, labs)
        self.assertEqual(vitals, {"1": "RR", "8": "DBP"})
        self.assertEqual(clinical, {("inputevents", "4"): ("VASOPRESSOR", "Norepinephrine"),
                                   ("outputevents", "6"): ("URINE", "Foley"),
                                   ("labevents", "10"): ("LAB", "Lactate")})

    def test_parquet_nested_schema_roundtrip(self):
        schema = reference.schemas("REFERENCE-test")["clinical_event.parquet"]
        table = reference.pa.Table.from_pylist([], schema=schema)
        sink = reference.pa.BufferOutputStream()
        reference.pq.write_table(table, sink)
        loaded = reference.pq.read_table(reference.pa.BufferReader(sink.getvalue()))
        self.assertTrue(table.equals(loaded, check_metadata=True))

    def test_source_accounting_duplicates_and_unknown_lab_availability(self):
        time = datetime(2150, 1, 1, 12)
        patient = dict(patient_key="P001", subject_id="1", hadm_id="2", stay_id="3",
                       icu_intime=time, icu_outtime=time + timedelta(hours=2))
        chart = dict(subject_id="1", hadm_id="2", stay_id="3", charttime=time.isoformat(),
                     storetime=time.isoformat(), itemid="220045", value="80", valuenum="80", valueuom="bpm")
        lab = dict(subject_id="1", hadm_id="2", charttime=time.isoformat(), itemid="50813",
                   value="1.5", valuenum="1.5", valueuom="mmol/L", labevent_id="10", flag="")
        sources = {
            "icu/d_items.csv.gz": [dict(itemid="220045", label="Heart Rate", linksto="chartevents")],
            "hosp/d_labitems.csv.gz": [dict(itemid="50813", label="Lactate", fluid="Blood")],
            "icu/chartevents.csv.gz": [chart.copy(), chart.copy(), dict(chart, valuenum="nan")],
            "hosp/labevents.csv.gz": [
                dict(lab, storetime=(time + timedelta(minutes=20)).isoformat()),
                dict(lab, labevent_id="11", storetime=""),
                dict(lab, labevent_id="12", storetime=(time - timedelta(minutes=1)).isoformat()),
            ],
            "icu/inputevents.csv.gz": [], "icu/outputevents.csv.gz": [],
        }
        def source(path):
            return iter(enumerate(sources[path.as_posix()], 1))
        with patch.object(reference, "source_rows", side_effect=source):
            telemetry, clinical, audit, _ = reference.build_events(Path("."), [patient], "REFERENCE-test")
        self.assertEqual(len(telemetry), 2)
        self.assertEqual(len({r["event_id"] for r in telemetry}), 2)
        self.assertEqual([r["sequence_number"] for r in telemetry], [1, 2])
        self.assertTrue(all(r["producer_time"] is None and r["source_event_time"] == r["event_time"] for r in telemetry))
        self.assertEqual(len(clinical), 3)
        self.assertEqual(sum(r["available_time"] is None for r in clinical), 2)
        self.assertEqual(len(audit), 6)
        self.assertEqual(sum(r["outcome"] == "EMITTED" for r in audit), 5)
        self.assertEqual(sum(r["outcome"] == "REJECTED" for r in audit), 1)


if __name__ == "__main__":
    unittest.main()

