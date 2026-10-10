"""Pure RAW-contract validation checks; no infrastructure or PyFlink required."""
import copy
import math
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "jobs/flink"))
from validation import NUMERIC_BOUNDS, ValidationError, parse_timestamp, validate_event


def numeric(**changes):
    event = {
        "event_id": "numeric-1", "stay_id": "37323533", "subject_id": "10039708",
        "hadm_id": "24928679", "run_id": "REFERENCE-test", "patient_key": "P001",
        "signal_name": "MAP", "value": 62.0,
        "event_time": "2143-09-19T21:11:00.123456",
        "source_event_time": "2143-09-19T21:11:00.123456",
        "producer_time": "2026-10-11T06:00:00.123456+00:00",
        "dataset_type": "REFERENCE", "is_virtual": False, "virtual_patient_id": None,
    }
    event.update(changes)
    return event


def clinical(**changes):
    event = numeric(event_id="clinical-1", event_type="LAB", value=1.5,
                    value_text="1.5", chart_time="2143-09-19T21:10:00",
                    available_time="2143-09-19T21:11:00",
                    validation_flags=[])
    event.pop("signal_name")
    event.update(changes)
    return event


def waveform(**changes):
    event = numeric(event_id="wf-canonical", signal_name="II", waveform_record_id="83411188",
                    start_time="2143-09-19T21:11:00", end_time="2143-09-19T21:11:01",
                    sampling_rate_hz=249.89, samples=[None, 0.2, -0.1, None])
    event.pop("value")
    event.update(changes)
    return event


class ValidationTests(unittest.TestCase):
    def rejection(self, event, stream, error_type):
        with self.assertRaises(ValidationError) as caught:
            validate_event(event, stream)
        self.assertEqual(caught.exception.error_type, error_type)
        self.assertTrue(caught.exception.reason)

    def test_valid_map_62_passes_and_original_fields_remain_untouched(self):
        event = numeric(extra={"untouched": [None, "source"]})
        original = copy.deepcopy(event)
        self.assertIsNone(validate_event(event, "vitals"))
        self.assertEqual(event, original)

    def test_canonical_numeric_signals_and_central_bounds(self):
        self.assertEqual(set(NUMERIC_BOUNDS), {"HR", "MAP", "SBP", "DBP", "SPO2", "RR"})
        for signal in NUMERIC_BOUNDS:
            with self.subTest(signal=signal):
                validate_event(numeric(signal_name=signal, value=0), "vitals")
                self.rejection(numeric(signal_name=signal, value=-0.001), "vitals", "VALUE_ERROR")

    def test_map_minus_500_is_a_value_error(self):
        self.rejection(numeric(value=-500), "vitals", "VALUE_ERROR")

    def test_saturation_inclusive_boundaries_and_above_100(self):
        for value in (0, 100, 96.2):
            validate_event(numeric(signal_name="SPO2", value=value), "vitals")
        self.rejection(numeric(signal_name="SPO2", value=100.001), "vitals", "VALUE_ERROR")

    def test_no_unrequested_clinical_upper_thresholds(self):
        validate_event(numeric(signal_name="HR", value=2000), "vitals")

    def test_nonfinite_numeric_values_are_value_errors(self):
        for value in (math.nan, math.inf, -math.inf, 10 ** 1000):
            with self.subTest(value_type=type(value).__name__):
                self.rejection(numeric(value=value), "vitals", "VALUE_ERROR")

    def test_numeric_type_checks_exclude_bool_strings_null_and_containers(self):
        for value in (True, False, "62", None, [], {}):
            with self.subTest(value=value):
                self.rejection(numeric(value=value), "vitals", "VALUE_ERROR")

    def test_missing_numeric_field_is_schema_error(self):
        for field in ("event_id", "stay_id", "event_time", "producer_time", "signal_name", "value"):
            event = numeric()
            event.pop(field)
            with self.subTest(field=field):
                self.rejection(event, "vitals", "SCHEMA_ERROR")

    def test_ids_follow_actual_string_contract(self):
        for field in ("event_id", "stay_id", "subject_id", "hadm_id", "run_id"):
            for value in (None, "", " ", 37323533, []):
                with self.subTest(field=field, value=value):
                    self.rejection(numeric(**{field: value}), "vitals", "SCHEMA_ERROR")

    def test_unknown_numeric_signal_is_schema_error(self):
        for signal in ("TEMP", "SpO2", "", None, ["MAP"]):
            with self.subTest(signal=signal):
                self.rejection(numeric(signal_name=signal), "vitals", "SCHEMA_ERROR")

    def test_timestamp_strings_and_nulls_are_not_rewritten(self):
        event = clinical(available_time=None, validation_flags=["STORETIME_BEFORE_CHARTTIME"])
        original = copy.deepcopy(event)
        validate_event(event, "clinical")
        self.assertEqual(event, original)
        self.assertIsNone(event["available_time"])

    def test_supported_clinical_types_pass(self):
        for kind in ("LAB", "GCS", "URINE"):
            with self.subTest(kind=kind):
                validate_event(clinical(event_type=kind), "clinical")

    def test_unknown_clinical_event_type_is_schema_error(self):
        self.rejection(clinical(event_type="SOFA"), "clinical", "SCHEMA_ERROR")

    def test_clinical_missing_available_time_differs_from_explicit_null(self):
        event = clinical()
        event.pop("available_time")
        self.rejection(event, "clinical", "SCHEMA_ERROR")
        validate_event(clinical(available_time=None), "clinical")

    def test_clinical_backwards_availability_is_value_error(self):
        self.rejection(clinical(available_time="2143-09-19T21:09:59"), "clinical", "VALUE_ERROR")

    def test_clinical_equal_availability_passes_and_offsets_compare_correctly(self):
        validate_event(clinical(available_time="2143-09-19T21:10:00"), "clinical")
        validate_event(clinical(chart_time="2143-09-19T22:10:00+01:00",
                                available_time="2143-09-19T21:10:00Z"), "clinical")

    def test_clinical_nonfinite_value_is_value_error(self):
        self.rejection(clinical(value=math.inf), "clinical", "VALUE_ERROR")

    def test_clinical_text_and_missing_measurements_preserve_nulls(self):
        for event in (clinical(value=None, value_text="negative"),
                      clinical(value=None, value_text=None)):
            original = copy.deepcopy(event)
            validate_event(event, "clinical")
            self.assertEqual(event, original)

    def test_clinical_value_representation_keys_are_required(self):
        for field in ("value", "value_text", "chart_time"):
            event = clinical()
            event.pop(field)
            with self.subTest(field=field):
                self.rejection(event, "clinical", "SCHEMA_ERROR")
        self.rejection(clinical(value_text=1.5), "clinical", "SCHEMA_ERROR")

    def test_waveform_valid_chunk_and_nullable_samples_pass(self):
        event = waveform()
        original = copy.deepcopy(event)
        validate_event(event, "waveform")
        self.assertEqual(event, original)
        validate_event(waveform(samples=[None, None]), "waveform")

    def test_waveform_event_id_is_optional_for_deterministic_fallback(self):
        event = waveform()
        event.pop("event_id")
        validate_event(event, "waveform")
        validate_event(waveform(event_id=None), "waveform")
        self.rejection(waveform(event_id=""), "waveform", "SCHEMA_ERROR")

    def test_waveform_invalid_frequency_is_value_error(self):
        for frequency in (0, -1, math.nan, math.inf, "249.89", True, None):
            with self.subTest(frequency=frequency):
                self.rejection(waveform(sampling_rate_hz=frequency), "waveform", "VALUE_ERROR")

    def test_waveform_empty_or_wrong_samples_container_is_schema_error(self):
        for samples in ([], None, "samples", {}, (1, 2)):
            with self.subTest(samples=samples):
                self.rejection(waveform(samples=samples), "waveform", "SCHEMA_ERROR")

    def test_waveform_bad_individual_sample_is_value_error(self):
        for sample in (math.nan, math.inf, "missing", [], True):
            with self.subTest(sample=sample):
                self.rejection(waveform(samples=[None, sample]), "waveform", "VALUE_ERROR")

    def test_waveform_interval_must_have_positive_duration(self):
        for end in ("2143-09-19T21:11:00", "2143-09-19T21:10:59"):
            with self.subTest(end=end):
                self.rejection(waveform(end_time=end), "waveform", "VALUE_ERROR")

    def test_waveform_required_fields_are_schema_errors(self):
        for field in ("stay_id", "waveform_record_id", "signal_name", "sampling_rate_hz",
                      "samples", "start_time", "end_time", "event_time", "producer_time"):
            event = waveform()
            event.pop(field)
            with self.subTest(field=field):
                self.rejection(event, "waveform", "SCHEMA_ERROR")

    def test_invalid_timestamp_fields_are_schema_errors(self):
        for field, event, stream in (
                ("event_time", numeric(), "vitals"),
                ("producer_time", numeric(), "vitals"),
                ("chart_time", clinical(), "clinical"),
                ("available_time", clinical(), "clinical"),
                ("start_time", waveform(), "waveform"),
                ("end_time", waveform(), "waveform")):
            for value in ("not-a-date", "2143-02-30T12:00:00", "2143-09-19", 1, True):
                with self.subTest(field=field, value=value):
                    self.rejection(dict(event, **{field: value}), stream, "SCHEMA_ERROR")

    def test_naive_and_aware_timestamp_comparison_uses_explicit_utc(self):
        expected = datetime(2143, 9, 19, 21, 11, tzinfo=timezone.utc)
        self.assertEqual(parse_timestamp("2143-09-19T21:11:00", "event_time"), expected)
        self.assertEqual(parse_timestamp("2143-09-19T22:11:00+01:00", "event_time"), expected)
        self.assertEqual(parse_timestamp("2143-09-19T21:11:00Z", "event_time"), expected)

    def test_timestamp_utc_conversion_overflow_is_a_schema_rejection(self):
        self.rejection(numeric(event_time="9999-12-31T23:59:59-01:00"), "vitals", "SCHEMA_ERROR")

    def test_event_container_and_unknown_stream_are_schema_errors(self):
        self.rejection([], "vitals", "SCHEMA_ERROR")
        self.rejection(numeric(), "alerts", "SCHEMA_ERROR")


if __name__ == "__main__":
    unittest.main()
