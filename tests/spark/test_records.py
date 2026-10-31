"""Kafka envelope/record helpers must mirror Flink's Java (de)serializers."""
import base64
import json
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

PROJECT = Path(__file__).resolve().parents[2]
for folder in ("jobs/flink", "jobs/spark", "scripts/demo"):
    sys.path.insert(0, str(PROJECT / folder))
from business_contract import validate
from business_outputs import telemetry
from spark_config import SparkConfig
from dlq import rejection
from records import kafka_record, record_key, source_record
from run_icu_demo import DEFAULT_START, PATIENTS, chart_event
from runtime_pipeline import parse_and_validate


def kafka_row(event, offset=7, key=b"35727289", headers=None, raw=None):
    value = raw if raw is not None else json.dumps(event).encode()
    return source_record("icu.v2.chartevents", 0, offset, key, value, headers)


class SourceRecordTests(unittest.TestCase):
    def test_envelope_matches_flink_deserializer_shape(self):
        event = chart_event(PATIENTS[1], "MAP", 0, DEFAULT_START)
        source = kafka_row(event, headers=[{"key": "trace", "value": bytearray(b"abc")}])
        self.assertEqual(set(source), {"topic", "partition", "offset", "headers", "key_base64", "payload_base64"})
        self.assertEqual(source["headers"], {"trace": "abc"})
        self.assertEqual(base64.b64decode(source["key_base64"]), b"35727289")
        record, failed = parse_and_validate(source, "chartevents")
        self.assertIsNone(failed)
        self.assertEqual(record["event"], event)

    def test_tombstone_and_missing_key_are_preserved_as_null(self):
        source = source_record("icu.v2.chartevents", 0, 3, None, None, None)
        self.assertEqual((source["key_base64"], source["payload_base64"], source["headers"]), (None, None, {}))
        _, failed = parse_and_validate(source, "chartevents")
        self.assertEqual(failed["error_code"], "JSON_PARSE_ERROR")


class KafkaRecordTests(unittest.TestCase):
    def test_business_record_is_keyed_by_stay_and_passes_contract_with_headers(self):
        event = chart_event(PATIENTS[1], "MAP", 0, DEFAULT_START)
        message = dict(telemetry(event, {"map": 62}), producer="icu-spark")
        topic, key, value, headers = kafka_record("icu.alt.spark.telemetry", message)
        self.assertEqual((topic, key), ("icu.alt.spark.telemetry", str(event["stay_id"])))
        kafka_headers = [(h["key"], h["value"]) for h in headers]
        validate(json.loads(value), "icu.telemetry", key.encode(), kafka_headers, require_headers=True)
        self.assertIn(("producer", b"icu-spark"), kafka_headers)

    def test_dlq_key_falls_back_to_original_kafka_key_then_dlq_id(self):
        source = kafka_row({}, raw=b"{not json")
        envelope = rejection(source, "chartevents", "PARSE_ERROR", "bad json")
        self.assertEqual(record_key(envelope), "35727289")
        envelope = rejection(dict(source, key_base64=None), "chartevents", "PARSE_ERROR", "bad json")
        self.assertEqual(record_key(envelope), envelope["dlq_id"])
        names = [h["key"] for h in kafka_record("icu.alt.spark.deadletter", envelope)[3]]
        self.assertEqual(names[-2:], ["dlq-source-topic", "dlq-error-code"])

    def test_non_integral_stay_is_not_used_as_key(self):
        self.assertEqual(record_key({"stay_id": 1.5, "kafka_key": "k"}), "k")
        self.assertEqual(record_key({"stay_id": True, "dlq_id": "d"}), "d")


class ConfigTests(unittest.TestCase):
    def test_defaults_match_flink_parallelism_and_reduced_fetch_wait(self):
        with patch.dict(os.environ, {}, clear=True):
            config = SparkConfig.from_env()
        self.assertEqual((config.master, config.shuffle_partitions, config.trigger, config.fetch_max_wait_ms),
                         ("local[1]", 1, "", 10))
        self.assertEqual(config.checkpoint("wide"), "/opt/icu/checkpoints/wide")

    def test_rejects_unknown_queries_and_bad_numbers(self):
        for env in ({"SPARK_QUERIES": "wide,bogus"}, {"SPARK_SHUFFLE_PARTITIONS": "0"},
                    {"SPARK_KAFKA_FETCH_MAX_WAIT_MS": "-1"}):
            with patch.dict(os.environ, env, clear=True), self.assertRaises(ValueError):
                SparkConfig.from_env()


if __name__ == "__main__":
    unittest.main()
