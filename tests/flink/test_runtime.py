"""Runtime contracts and shared operator logic without host PyFlink installs."""
import base64
import copy
import hashlib
import json
import os
import sys
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "jobs/flink"))
from dedup import check_and_remember, dedup_identity
from dlq import ERROR_TYPES, encode, log_line, payload_bytes, rejection, rejection_log
from event_time import classify_event, event_timestamp_ms
from runtime_config import DLQ_MAX_MESSAGE_BYTES, DLQ_TOPIC, RuntimeConfig
from runtime_pipeline import parse_and_validate


def numeric(**changes):
    event = {
        "event_id": "evt-runtime-1", "stay_id": "37323533", "signal_name": "MAP",
        "value": 62.0, "event_time": "2143-09-19T21:11:00",
        "source_event_time": "2100-01-01T00:00:00",
        "producer_time": "2026-10-11T06:00:00+00:00",
        "run_id": "engineering-run", "dataset_type": "ENGINEERING_PROBE",
        "replay_session_id": "ticket-006-probe", "is_virtual": True,
    }
    event.update(changes)
    return event


def clinical(**changes):
    event = numeric(event_id="evt-clinical-1", event_type="LAB", value=1.5,
                    value_text="1.5", chart_time="2143-09-19T21:10:00", available_time=None)
    event.pop("signal_name")
    event.update(changes)
    return event


def waveform(**changes):
    event = numeric(event_id="wf-runtime-1", waveform_record_id="83411188", signal_name="II",
                    sampling_rate_hz=249.89, samples=[None, 0.2, -0.1],
                    start_time="2143-09-19T21:11:00", end_time="2143-09-19T21:11:01")
    event.pop("value")
    event.update(changes)
    return event


def source(raw, key=b"37323533", **changes):
    envelope = {
        "topic": "icu.v1.vitals.numeric", "partition": 0, "offset": 17,
        "key_base64": None if key is None else base64.b64encode(key).decode("ascii"),
        "payload_base64": None if raw is None else base64.b64encode(raw).decode("ascii"),
    }
    envelope.update(changes)
    return envelope


class FakeValueState:
    """ValueState interface only; TTL/checkpoint behavior is verified in live Flink."""
    def __init__(self):
        self.current = None
        self.writes = 0

    def value(self):
        return self.current

    def update(self, value):
        self.current = value
        self.writes += 1


class RuntimeParsingTests(unittest.TestCase):
    def test_valid_map_event_has_exclusively_accepted_result_and_preserved_fields(self):
        event = numeric(extra={"original": [None, "kept"]})
        raw = encode(event).encode("utf-8")
        original_source = source(raw)
        before = copy.deepcopy(original_source)
        accepted, failed = parse_and_validate(original_source, "vitals")
        self.assertIsNone(failed)
        self.assertEqual(accepted["event"], event)
        self.assertEqual(accepted["source"], before)
        self.assertEqual(original_source, before)
        self.assertEqual(accepted["stream_type"], "vitals")

    def test_malformed_and_truncated_json_have_parse_error_and_no_accepted_result(self):
        for raw in (b"not JSON", b'{"event_id":', b"", b"{]"):
            with self.subTest(raw=raw):
                accepted, failed = parse_and_validate(source(raw), "vitals")
                self.assertIsNone(accepted)
                self.assertEqual(failed["error_type"], "PARSE_ERROR")
                self.assertEqual(failed["raw_payload"], raw.decode())

    def test_non_object_json_roots_have_parse_error(self):
        for raw in (b"[]", b"[{}]", b"null", b"62", b'"object"', b"true"):
            with self.subTest(raw=raw):
                accepted, failed = parse_and_validate(source(raw), "vitals")
                self.assertIsNone(accepted)
                self.assertEqual(failed["error_type"], "PARSE_ERROR")

    def test_invalid_utf8_rejection_retains_exact_original_bytes(self):
        raw = b'{"event_id":"bad\xff"}'
        accepted, failed = parse_and_validate(source(raw), "vitals")
        self.assertIsNone(accepted)
        self.assertEqual(failed["error_type"], "PARSE_ERROR")
        self.assertIsNone(failed["raw_payload"])
        self.assertEqual(base64.b64decode(failed["raw_payload_base64"]), raw)

    def test_kafka_tombstone_is_auditable_parse_rejection(self):
        accepted, failed = parse_and_validate(source(None), "vitals")
        self.assertIsNone(accepted)
        self.assertEqual(failed["error_type"], "PARSE_ERROR")
        self.assertTrue(failed["raw_payload_is_null"])
        self.assertIsNone(failed["raw_payload_base64"])
        self.assertIsNone(failed["raw_payload"])
        self.assertIsNone(payload_bytes(source(None)))

    def test_non_json_numeric_constants_retain_existing_strict_parser_policy(self):
        for constant in (b"NaN", b"Infinity", b"-Infinity"):
            raw = b'{"value":' + constant + b"}"
            with self.subTest(constant=constant):
                accepted, failed = parse_and_validate(source(raw), "vitals")
                self.assertIsNone(accepted)
                self.assertEqual(failed["error_type"], "PARSE_ERROR")

    def test_json_number_overflow_is_value_error_instead_of_serialization_crash(self):
        raw = encode(numeric()).replace('"value":62.0', '"value":1e999').encode()
        accepted, failed = parse_and_validate(source(raw), "vitals")
        self.assertIsNone(accepted)
        self.assertEqual(failed["error_type"], "VALUE_ERROR")
        self.assertEqual(failed["event_id"], "evt-runtime-1")
        self.assertEqual(json.loads(encode(failed))["raw_payload"], raw.decode())

    def test_missing_required_identity_routes_schema_rejection(self):
        event = numeric()
        event.pop("stay_id")
        accepted, failed = parse_and_validate(source(encode(event).encode()), "vitals")
        self.assertIsNone(accepted)
        self.assertEqual(failed["error_type"], "SCHEMA_ERROR")
        self.assertIsNone(failed["stay_id"])

    def test_negative_map_routes_value_rejection(self):
        accepted, failed = parse_and_validate(source(encode(numeric(value=-500)).encode()), "vitals")
        self.assertIsNone(accepted)
        self.assertEqual(failed["error_type"], "VALUE_ERROR")
        self.assertIn("MAP", failed["error_reason"])

    def test_null_clinical_availability_survives_parse_validation_unchanged(self):
        event = clinical(validation_flags=["STORETIME_BEFORE_CHARTTIME"])
        accepted, failed = parse_and_validate(source(encode(event).encode()), "clinical")
        self.assertIsNone(failed)
        self.assertIsNone(accepted["event"]["available_time"])
        self.assertEqual(accepted["event"], event)

    def test_valid_waveform_keeps_full_nullable_samples_in_internal_record(self):
        event = waveform()
        accepted, failed = parse_and_validate(source(encode(event).encode()), "waveform")
        self.assertIsNone(failed)
        self.assertEqual(accepted["event"], event)

    def test_escaped_unpaired_surrogate_ids_are_auditable_schema_rejections(self):
        for field in ("event_id", "stay_id"):
            with self.subTest(field=field):
                event = numeric(**{field: "bad-\ud800-id"})
                raw = json.dumps(event, ensure_ascii=True).encode("utf-8")
                accepted, failed = parse_and_validate(source(raw), "vitals")
                self.assertIsNone(accepted)
                self.assertEqual(failed["error_type"], "SCHEMA_ERROR")
                self.assertIn(field, failed["error_reason"])
                self.assertIn("surrogate", failed["error_reason"])
                self.assertEqual(base64.b64decode(failed["raw_payload_base64"]), raw)
                self.assertEqual(json.loads(encode(failed).encode("utf-8")), failed)


class DeadLetterTests(unittest.TestCase):
    def test_envelope_contains_version_original_kafka_metadata_and_recoverable_fields(self):
        event = numeric()
        raw = encode(event).encode()
        original = source(raw, topic="icu.v1.vitals.numeric", partition=2, offset=93)
        failed = rejection(original, "vitals", "VALUE_ERROR", "test reason", event,
                           failed_at="2026-10-11T06:01:00+00:00")
        self.assertEqual(failed["schema_version"], "1.0.0")
        self.assertEqual(failed["original_topic"], original["topic"])
        self.assertEqual(failed["original_partition"], 2)
        self.assertEqual(failed["original_offset"], 93)
        self.assertEqual(failed["original_key"], "37323533")
        self.assertEqual(failed["original_key_base64"], original["key_base64"])
        self.assertEqual(failed["stream_type"], "vitals")
        self.assertEqual(failed["failed_at"], "2026-10-11T06:01:00+00:00")
        self.assertEqual(failed["error_reason"], "test reason")
        for field in ("event_id", "stay_id", "run_id", "dataset_type", "replay_session_id"):
            self.assertEqual(failed[field], event[field])

    def test_envelope_preserves_exact_utf8_payload_not_a_reserialized_object(self):
        raw = ' { "name": "\u03bb", "value": -500 }\n'.encode("utf-8")
        failed = rejection(source(raw), "vitals", "VALUE_ERROR", "invalid value")
        self.assertEqual(failed["raw_payload"], raw.decode("utf-8"))
        self.assertEqual(base64.b64decode(failed["raw_payload_base64"]), raw)
        self.assertFalse(failed["raw_payload_is_null"])

    def test_binary_and_null_keys_are_lossless(self):
        for key in (b"\xff\x00key", None, b""):
            with self.subTest(key=key):
                failed = rejection(source(b"bad", key=key), "vitals", "PARSE_ERROR", "bad")
                if key is None:
                    self.assertIsNone(failed["original_key_base64"])
                    self.assertIsNone(failed["original_key"])
                else:
                    self.assertEqual(base64.b64decode(failed["original_key_base64"]), key)
                    if key:
                        self.assertIsNone(failed["original_key"])
                    else:
                        self.assertEqual(failed["original_key"], "")

    def test_payload_hash_is_deterministic_sha256_of_actual_raw_bytes(self):
        raw = b'{ "value": -500 }'
        first = rejection(source(raw), "vitals", "VALUE_ERROR", "first")
        second = rejection(source(raw), "vitals", "VALUE_ERROR", "second")
        self.assertEqual(first["raw_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(first["raw_sha256"], second["raw_sha256"])
        changed = rejection(source(raw + b" "), "vitals", "VALUE_ERROR", "first")
        self.assertNotEqual(first["raw_sha256"], changed["raw_sha256"])

    def test_dlq_identity_is_stable_across_failure_clock_and_reason(self):
        original = source(b"bad")
        first = rejection(original, "vitals", "PARSE_ERROR", "one", failed_at="time-one")
        second = rejection(original, "vitals", "PARSE_ERROR", "two", failed_at="time-two")
        self.assertEqual(first["dlq_id"], second["dlq_id"])
        self.assertTrue(first["dlq_id"].startswith("dlq-"))

    def test_dlq_identity_distinguishes_source_offsets_error_type_and_tombstones(self):
        first = rejection(source(b""), "vitals", "PARSE_ERROR", "bad")
        different_offset = rejection(source(b"", offset=18), "vitals", "PARSE_ERROR", "bad")
        different_error = rejection(source(b""), "vitals", "SCHEMA_ERROR", "bad")
        tombstone = rejection(source(None), "vitals", "PARSE_ERROR", "bad")
        self.assertEqual(first["raw_sha256"], tombstone["raw_sha256"])
        self.assertEqual(len({item["dlq_id"] for item in (
            first, different_offset, different_error, tombstone)}), 4)

    def test_unrecoverable_and_invalid_identity_fields_are_explicit_null(self):
        event = {"event_id": [], "stay_id": 37323533, "run_id": " "}
        failed = rejection(source(b"bad"), "vitals", "SCHEMA_ERROR", "invalid", event)
        for field in ("event_id", "stay_id", "run_id", "dataset_type", "replay_session_id"):
            self.assertIsNone(failed[field])

    def test_rejection_log_is_bounded_and_never_contains_waveform_array(self):
        event = waveform(samples=[0.123456] * 20000)
        failed = rejection(source(encode(event).encode()), "waveform", "DUPLICATE_EVENT", "duplicate", event)
        logged = rejection_log(failed)
        decoded = json.loads(logged)
        self.assertEqual(decoded["kind"], "DLQ_EVENT")
        self.assertEqual(decoded["raw_sha256"], failed["raw_sha256"])
        self.assertNotIn("samples", decoded)
        self.assertNotIn("raw_payload", decoded)
        self.assertLess(len(logged), 1500)

    def test_clean_log_shows_only_five_waveform_samples_and_source_metadata(self):
        event = waveform(samples=list(range(20000)))
        record = {"event": event, "source": source(b"unused"), "stream_type": "waveform"}
        logged = json.loads(log_line(record, "OUT_OF_ORDER_EVENT", watermark_ms=100))
        self.assertEqual(logged["kind"], "OUT_OF_ORDER_EVENT")
        self.assertEqual(logged["sample_count"], 20000)
        self.assertEqual(logged["samples_preview"], [0, 1, 2, 3, 4])
        self.assertNotIn("samples", logged)
        self.assertEqual(logged["topic"], record["source"]["topic"])
        self.assertEqual(logged["offset"], 17)
        self.assertEqual(logged["watermark_ms"], 100)

    def test_large_malformed_payload_exceeds_old_limit_but_fits_central_dlq_limit(self):
        raw = b"!" * 450000
        accepted, failed = parse_and_validate(source(raw), "vitals")
        self.assertIsNone(accepted)
        self.assertEqual(failed["error_type"], "PARSE_ERROR")
        encoded = encode(failed).encode("utf-8")
        self.assertGreater(len(encoded), 1024 * 1024)
        self.assertEqual(DLQ_MAX_MESSAGE_BYTES, 8 * 1024 * 1024)
        self.assertLess(len(encoded), DLQ_MAX_MESSAGE_BYTES)
        self.assertEqual(failed["raw_sha256"], hashlib.sha256(raw).hexdigest())
        self.assertEqual(base64.b64decode(failed["raw_payload_base64"]), raw)

    def test_one_mib_control_bytes_with_json_escaping_and_base64_fits_dlq_limit(self):
        raw = b"\x00" * (1024 * 1024)
        accepted, failed = parse_and_validate(source(raw), "vitals")
        self.assertIsNone(accepted)
        self.assertEqual(failed["error_type"], "PARSE_ERROR")
        encoded = encode(failed).encode("utf-8")
        # Each NUL expands to six JSON characters plus its lossless Base64 copy.
        self.assertGreater(len(encoded), 7 * 1024 * 1024)
        self.assertLess(len(encoded), DLQ_MAX_MESSAGE_BYTES)
        decoded = json.loads(encoded)
        self.assertEqual(decoded["raw_payload"].encode("utf-8"), raw)
        self.assertEqual(base64.b64decode(decoded["raw_payload_base64"]), raw)
        self.assertEqual(decoded["raw_sha256"], hashlib.sha256(raw).hexdigest())

    def test_recoverable_event_time_and_measurement_type_are_in_dlq_and_audit_log(self):
        for stream, event, expected_field in (("vitals", numeric(), "signal_name"),
                                               ("clinical", clinical(), "event_type"),
                                               ("waveform", waveform(), "signal_name")):
            with self.subTest(stream=stream):
                failed = rejection(source(encode(event).encode()), stream, "DUPLICATE_EVENT",
                                   "duplicate", event)
                logged = json.loads(rejection_log(failed))
                for field in ("event_time", expected_field):
                    self.assertEqual(failed[field], event[field])
                    self.assertEqual(logged[field], event[field])

    def test_unknown_error_type_and_nonfinite_envelope_serialization_fail_explicitly(self):
        self.assertEqual(ERROR_TYPES, {"PARSE_ERROR", "SCHEMA_ERROR", "VALUE_ERROR",
                                       "DUPLICATE_EVENT", "LATE_EVENT"})
        with self.assertRaises(ValueError):
            rejection(source(b"bad"), "vitals", "OTHER", "bad")
        with self.assertRaises(ValueError):
            encode({"value": float("nan")})


class DedupTests(unittest.TestCase):
    def test_event_id_is_primary_numeric_and_clinical_identity(self):
        for stream, event in (("vitals", numeric()), ("clinical", clinical())):
            with self.subTest(stream=stream):
                first = dedup_identity(event, stream)
                altered = dict(event, value=99, stay_id="different-stay")
                self.assertEqual(first, dedup_identity(altered, stream))
                self.assertNotEqual(first, dedup_identity(dict(event, event_id="different-id"), stream))

    def test_stream_namespaces_prevent_cross_stream_event_id_collisions(self):
        self.assertEqual(len({dedup_identity({"event_id": "same"}, stream)
                              for stream in ("vitals", "clinical", "waveform")}), 3)

    def test_waveform_canonical_event_id_takes_precedence(self):
        event = waveform()
        self.assertEqual(dedup_identity(event, "waveform"), "waveform:id:wf-runtime-1")
        self.assertEqual(dedup_identity(event, "waveform"),
                         dedup_identity(dict(event, start_time="different"), "waveform"))

    def test_waveform_fallback_is_deterministic_and_each_composite_field_matters(self):
        event = waveform(event_id=None)
        expected = dedup_identity(event, "waveform")
        self.assertTrue(expected.startswith("waveform:chunk:"))
        self.assertEqual(expected, dedup_identity(copy.deepcopy(event), "waveform"))
        self.assertEqual(expected, dedup_identity(dict(event, samples=[9]), "waveform"))
        for field in ("stay_id", "waveform_record_id", "signal_name", "start_time", "end_time"):
            with self.subTest(field=field):
                self.assertNotEqual(expected, dedup_identity(dict(event, **{field: "different"}), "waveform"))

    def test_numeric_and_clinical_have_no_random_or_composite_fallback(self):
        for stream in ("vitals", "clinical"):
            with self.subTest(stream=stream), self.assertRaises(ValueError):
                dedup_identity({"stay_id": "37323533"}, stream)

    def test_duplicate_numeric_detected_by_shared_managed_state_check(self):
        self.assert_duplicate(numeric(), "vitals")

    def test_duplicate_clinical_detected_by_shared_managed_state_check(self):
        self.assert_duplicate(clinical(), "clinical")

    def test_duplicate_waveform_detected_with_canonical_and_fallback_identity(self):
        self.assert_duplicate(waveform(), "waveform")
        self.assert_duplicate(waveform(event_id=None), "waveform")

    def assert_duplicate(self, event, stream):
        state_by_key = {}
        flags = []
        for incoming in (event, copy.deepcopy(event)):
            state = state_by_key.setdefault(dedup_identity(incoming, stream), FakeValueState())
            flags.append(check_and_remember(state))
        self.assertEqual(flags, [False, True])
        self.assertEqual(len(state_by_key), 1)
        self.assertEqual(next(iter(state_by_key.values())).writes, 1)

    def test_expired_state_interface_allows_identity_again_without_permanent_dedup_claim(self):
        state = FakeValueState()
        self.assertFalse(check_and_remember(state))
        self.assertTrue(check_and_remember(state))
        # Flink's never_return_expired TTL makes expired value() return None.
        state.current = None
        self.assertFalse(check_and_remember(state))
        self.assertEqual(state.writes, 2)


class EventTimeTests(unittest.TestCase):
    def test_timestamp_extraction_uses_event_time_not_source_or_producer(self):
        first = numeric(event_time="1970-01-01T00:00:01.234567")
        self.assertEqual(event_timestamp_ms(first), 1234)
        self.assertEqual(event_timestamp_ms(dict(first, source_event_time="2200-01-01T00:00:00",
                                                producer_time="2300-01-01T00:00:00")), 1234)

    def test_utc_convention_is_timezone_independent_and_microseconds_floor(self):
        self.assertEqual(event_timestamp_ms(numeric(event_time="1970-01-01T01:00:00+01:00")), 0)
        self.assertEqual(event_timestamp_ms(numeric(event_time="1970-01-01T00:00:00Z")), 0)
        self.assertEqual(event_timestamp_ms(numeric(event_time="1969-12-31T23:59:59.999999")), -1)

    def test_in_order_or_equal_max_timestamp_remains_clean(self):
        self.assertEqual(classify_event(100000, 69999, None), "CLEAN_EVENT")
        self.assertEqual(classify_event(100000, 69999, 100000), "CLEAN_EVENT")
        self.assertEqual(classify_event(100001, 69999, 100000), "CLEAN_EVENT")

    def test_tolerated_older_timestamp_is_out_of_order_and_not_late(self):
        self.assertEqual(classify_event(90000, 69999, 100000), "OUT_OF_ORDER_EVENT")
        self.assertEqual(classify_event(70000, 69999, 100000), "OUT_OF_ORDER_EVENT")

    def test_behind_and_equal_effective_watermark_are_late(self):
        self.assertEqual(classify_event(69998, 69999, 100000), "LATE_EVENT")
        self.assertEqual(classify_event(69999, 69999, 100000), "LATE_EVENT")
        self.assertEqual(classify_event(69999, 69999, None), "LATE_EVENT")

    def test_old_record_before_watermark_emission_uses_actual_watermark(self):
        self.assertEqual(classify_event(1000, -(2 ** 63), 100000), "OUT_OF_ORDER_EVENT")
        self.assertEqual(classify_event(1000, 69999, 100000), "LATE_EVENT")


class RuntimeConfigTests(unittest.TestCase):
    def test_defaults_match_local_ticket_boundaries(self):
        with patch.dict(os.environ, {}, clear=True):
            config = RuntimeConfig.from_env()
        self.assertEqual(config.bootstrap_servers, "icu-kafka:9094")
        self.assertEqual(config.dlq_topic, "icu.v1.deadletter")
        self.assertEqual(config.dlq_topic, DLQ_TOPIC)
        self.assertEqual(config.max_out_of_orderness_seconds, 30)
        self.assertEqual(config.dedup_state_ttl_hours, 24)
        self.assertEqual(config.checkpoint_interval_ms, 10000)

    def test_environment_settings_override_defaults_in_one_place(self):
        values = {"KAFKA_BOOTSTRAP_SERVERS": "broker:9094", "DLQ_TOPIC": DLQ_TOPIC,
                  "MAX_OUT_OF_ORDERNESS_SECONDS": "5", "DEDUP_STATE_TTL_HOURS": "2",
                  "CHECKPOINT_INTERVAL_MS": "2000"}
        with patch.dict(os.environ, values, clear=True):
            config = RuntimeConfig.from_env()
        self.assertEqual(config.bootstrap_servers, "broker:9094")
        self.assertEqual(config.max_out_of_orderness_seconds, 5)
        self.assertEqual(config.dedup_state_ttl_hours, 2)
        self.assertEqual(config.checkpoint_interval_ms, 2000)

    def test_invalid_ranges_and_unapproved_output_topic_are_rejected(self):
        for change in ({"bootstrap_servers": " "}, {"dlq_topic": "icu.v1.processed"},
                       {"max_out_of_orderness_seconds": -1}, {"dedup_state_ttl_hours": 0},
                       {"checkpoint_interval_ms": 0}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                replace(RuntimeConfig(), **change).validate()
        replace(RuntimeConfig(), max_out_of_orderness_seconds=0).validate()

    def test_malformed_numeric_environment_setting_fails_clearly(self):
        with patch.dict(os.environ, {"MAX_OUT_OF_ORDERNESS_SECONDS": "thirty"}, clear=True):
            with self.assertRaises(ValueError):
                RuntimeConfig.from_env()


if __name__ == "__main__":
    unittest.main()
