"""V2 contracts and computation tests; source identities are checked against real CSV.GZ."""
import asyncio
import base64
import json
import math
import statistics
import sys
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch
from aiohttp.test_utils import TestClient, TestServer

PROJECT = Path(__file__).resolve().parents[1]
for folder in ("jobs/flink", "scripts/demo", "services/realtime_gateway"):
    sys.path.insert(0, str(PROJECT / folder))
from registry import load_patients, validate_source_identities, source_rows
from contracts import CORE_TOPICS, TOPICS, BUSINESS_TOPICS, CHART_FIELDS, LAB_FIELDS, SIGNALS, parse_event, stay_key
from validation import validate_event, ValidationError, parse_timestamp
from runtime_pipeline import parse_and_validate
from runtime_config import RuntimeConfig, WindowAlertConfig
from run_icu_demo import events_at, demo_events, chart_event, DEFAULT_START
from dlq import rejection, encode
from dedup import check_and_remember, dedup_identity
from event_time import classify_event, event_timestamp_ms, watermark_strategy
from business_outputs import telemetry
from business_contract import FIELDS, metadata
def processed_event(r, ignored=None):
    return telemetry(r["event"], {FIELDS[r["event"]["signal_name"]]:r["event"]["valuenum"]})
from window_aggregates import create_accumulator, add_record, merge_accumulators, aggregate_result
from hypotension import advance_episode
from gateway import GatewayState, KafkaFeed, create_app, publish, CLIENTS, STATE, GROUP_ID, SUBSCRIPTIONS

PATIENTS = load_patients()


def event(signal="MAP", second=0, patient=0):
    return chart_event(PATIENTS[patient], signal, second, DEFAULT_START)


def source(value, offset=0):
    raw = value if isinstance(value, bytes) else encode(value).encode()
    return {"topic": TOPICS["chartevents"], "partition": 0, "offset": offset,
            "payload_base64": base64.b64encode(raw).decode(), "key_base64": None}


def record(value=None):
    value = value or event()
    return {"event": value, "source": source(value), "stream_type": "chartevents",
            "classification": "CLEAN_EVENT", "event_time_ms": event_timestamp_ms(value)}


def summary():
    wide=processed_event(record())
    acc=add_record(create_accumulator(),wide)
    return aggregate_result(acc,str(PATIENTS[0]['stay_id']),0,300000)

def alert():
    state=None
    for second in range(31):
        state,outputs=advance_episode(state,record(dict(event(second=second),valuenum=62)),WindowAlertConfig())
        if outputs: return outputs[0]


class RegistryTests(unittest.TestCase):
    def test_four_real_triples_in_source(self):
        self.assertEqual(validate_source_identities(), PATIENTS)

    def test_frozen_p04(self):
        self.assertEqual(PATIENTS[3], dict(demo_id="P04", bed_id="BED-04", subject_id=10007818,
                                          hadm_id=22987108, stay_id=32359580))

    def test_p04_has_all_six_vital_items(self):
        seen = {int(e["itemid"]) for e in source_rows("icu/chartevents.csv.gz")
                if int(e["stay_id"]) == PATIENTS[3]["stay_id"]}
        self.assertTrue({s[0] for s in SIGNALS.values()} <= seen)

    def test_registry_rejects_fifth(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.yaml"
            path.write_text(json.dumps({"patients": PATIENTS + [PATIENTS[0]]}))
            with self.assertRaises(ValueError):
                load_patients(path)

    def test_registry_rejects_virtual_identity(self):
        patients = json.loads(json.dumps(PATIENTS))
        patients[3]["stay_id"] = "virtual-stay"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.yaml"
            path.write_text(json.dumps({"patients": patients}))
            with self.assertRaises(ValueError):
                load_patients(path)

    def test_all_generated_events_use_the_four_identities(self):
        triples = {(p["subject_id"], p["hadm_id"], p["stay_id"]) for p in PATIENTS}
        for topic, value in demo_events():
            self.assertIn((value["subject_id"], value["hadm_id"], int(stay_key(value))), triples)
            self.assertEqual(set(value), CHART_FIELDS if topic == TOPICS["chartevents"] else LAB_FIELDS)

    def test_registry_itemids_exist_in_source_dictionaries(self):
        chart_items = {int(e["itemid"]): e for e in source_rows("icu/d_items.csv.gz")}
        for itemid, _, _, _ in SIGNALS.values():
            self.assertEqual(chart_items[itemid]["linksto"], "chartevents")
        self.assertEqual(next(e for e in source_rows("hosp/d_labitems.csv.gz") if e["itemid"] == "50813")["label"], "Lactate")


class ContractTests(unittest.TestCase):
    def test_six_topics(self):
        self.assertEqual(len(set(CORE_TOPICS)), 6)
        self.assertEqual(set(TOPICS.values()), {"icu.v2.chartevents","icu.v2.labevents"})

    def test_strict_json_rejects_non_objects_and_constants(self):
        for raw in ('[]', 'null', '{"v":NaN}', '{"v":Infinity}', '{"v":1e999}', '{"v":1,"v":2}', '{'):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                parse_event(raw)

    def test_chartevents_validation(self):
        for patient in range(4):
            for signal in SIGNALS:
                self.assertIsNone(validate_event(event(signal=signal, patient=patient), "chartevents"))

    def test_labevents_validation_and_mapped_stay(self):
        labs = [e for t, e in events_at(0) if t == TOPICS["labevents"]]
        self.assertEqual(len(labs), 4)
        for value in labs:
            self.assertNotIn("stay_id", value)
            validate_event(value, "labevents")
            self.assertEqual(stay_key(value), str(value["mapped_stay_id"]))

    def test_extra_metadata_rejected(self):
        for field in ("data_origin", "run_id", "patient_key", "scenario_uuid", "producer_time", "source_classification"):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                validate_event(dict(event(), **{field: "unexpected"}), "chartevents")

    def test_foreign_or_mismatched_identities_rejected(self):
        for changes in ({"stay_id": 1}, {"subject_id": PATIENTS[1]["subject_id"]}, {"bed_id": "BED-05"},
                        {"hadm_id": "21457723"}, {"stay_id": True}):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                validate_event(dict(event(), **changes), "chartevents")

    def test_invalid_values_rejected(self):
        for value in (-1, 1000, True, None, "62", float("inf"), float("nan"), 10**1000):
            with self.subTest(value=str(value)[:30]), self.assertRaises(ValidationError):
                validate_event(dict(event(), valuenum=value), "chartevents")

    def test_invalid_spo2_rejected(self):
        with self.assertRaises(ValidationError):
            validate_event(dict(event("SpO2"), valuenum=101), "chartevents")

    def test_invalid_lab_rejected(self):
        lab = next(e for t, e in events_at(0) if t == TOPICS["labevents"])
        for changes in ({"valuenum": -1}, {"mapped_stay_id": 123}, {"itemid": 220045}):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                validate_event(dict(lab, **changes), "labevents")

    def test_invalid_schema_dictionary_and_time(self):
        for changes in ({"schema_version": "1.0"}, {"itemid": 42}, {"valueuom": "bpm"},
                        {"event_time": "2026-10-16"}, {"event_time": "2026-10-16T20:00:00"}):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                validate_event(dict(event(), **changes), "chartevents")

    def test_parsing_dlq_and_recoverable_identity(self):
        _,rejected=parse_and_validate(source(dict(event(),valuenum=-1)),"chartevents")
        self.assertEqual(rejected["error_code"],"VALUE_OUT_OF_RANGE")
        self.assertEqual(rejected["original_payload"]["stay_id"],PATIENTS[0]["stay_id"])
        self.assertEqual(rejected["original_payload"]["valuenum"],-1)

    def test_non_utf8_dlq_preserves_bytes(self):
        _, rejected = parse_and_validate(source(b"\xff\x00"), "chartevents")
        self.assertEqual(rejected["error_stage"], "DESERIALIZATION")
        self.assertEqual(base64.b64decode(rejected["raw_payload_base64"]), b"\xff\x00")

    def test_tombstone_rejected(self):
        envelope = dict(source(b""), payload_base64=None)
        _, rejected = parse_and_validate(envelope, "chartevents")
        self.assertEqual(rejected["error_stage"], "DESERIALIZATION")
        self.assertIsNone(rejected["original_payload"])

    def test_wide_projection_has_only_documented_clinical_fields(self):
        result=processed_event(record())
        self.assertEqual(result['map'],event()['valuenum'])
        self.assertNotIn('respiratory_rate',result)
        self.assertNotIn('dbp',result)

    def test_wide_identity_is_stable(self):
        self.assertEqual(processed_event(record())['message_id'],processed_event(record())['message_id'])

    def test_stay_keys_are_decimal_id_bytes(self):
        for _, value in events_at(0):
            self.assertIn(stay_key(value).encode(), {str(p["stay_id"]).encode() for p in PATIENTS})

    def test_stable_event_and_dlq_ids(self):
        self.assertEqual(list(events_at(180)), list(events_at(180)))
        self.assertNotEqual(event()["event_id"], event(second=1)["event_id"])
        one = rejection(source(event()), "chartevents", "LATE_EVENT", "late")
        two = rejection(source(event()), "chartevents", "LATE_EVENT", "late")
        self.assertEqual(one["dlq_id"], two["dlq_id"])
        self.assertNotEqual(one["dlq_id"], rejection(source(event(), 1), "chartevents", "LATE_EVENT", "late")["dlq_id"])


class ComputationTests(unittest.TestCase):
    def test_watermark_wrapper_retains_the_logical_timestamp_assigner(self):
        # PyFlink with_idleness creates a new wrapper, so assigner must be attached last.
        class Strategy:
            timestamp_assigner = None

            def with_idleness(self, timeout):
                return Strategy()

            def with_timestamp_assigner(self, assigner):
                self.timestamp_assigner = assigner
                return self

        common = ModuleType("pyflink.common")
        common.Duration = SimpleNamespace(of_seconds=lambda seconds: seconds)
        common.WatermarkStrategy = SimpleNamespace(for_bounded_out_of_orderness=lambda seconds: Strategy())
        timestamp_module = ModuleType("pyflink.common.watermark_strategy")
        timestamp_module.TimestampAssigner = type("TimestampAssigner", (), {})
        modules = {"pyflink": ModuleType("pyflink"), "pyflink.common": common,
                   "pyflink.common.watermark_strategy": timestamp_module}
        with patch.dict(sys.modules, modules):
            strategy = watermark_strategy(30)
        self.assertIsNotNone(strategy.timestamp_assigner)
        self.assertEqual(strategy.timestamp_assigner.extract_timestamp(encode(record()), 123), event_timestamp_ms(event()))

    def test_managed_dedup_state_transition(self):
        state = MagicMock()
        state.value.return_value = None
        self.assertFalse(check_and_remember(state))
        state.update.assert_called_once_with(True)
        state.value.return_value = True
        self.assertTrue(check_and_remember(state))
        self.assertEqual(dedup_identity(event(), "chartevents"), dedup_identity(event(), "chartevents"))

    def test_late_and_out_of_order_boundaries(self):
        self.assertEqual(classify_event(100, 100, 120), "LATE_EVENT")
        self.assertEqual(classify_event(101, 100, 120), "OUT_OF_ORDER_EVENT")
        self.assertEqual(classify_event(120, 100, 120), "CLEAN_EVENT")

    def test_wide_means_merge_and_missing_observations(self):
        a=add_record(create_accumulator(),{'heart_rate':80,'map':60})
        b=add_record(create_accumulator(),{'heart_rate':100,'spo2':98})
        result=aggregate_result(merge_accumulators(a,b),'35727289',0,300000)
        self.assertEqual(result['avg_heart_rate'],90)
        self.assertEqual(result['avg_map'],60)
        self.assertEqual(result['avg_spo2'],98)
        self.assertIsNone(result['avg_sbp'])
        self.assertIsNone(aggregate_result(create_accumulator(),'1',0,300000))
        with self.assertRaises(ValueError): aggregate_result(a,'1',0,900000)


class GatewayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.client = TestClient(TestServer(create_app(feed_factory=None)))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.client.close()

    async def test_snapshot_and_patient_api(self):
        for route in ("/api/snapshot", "/api/patients"):
            response = await self.client.get(route)
            self.assertEqual(set(await response.json()), {"P01", "P02", "P03", "P04"})
        response = await self.client.get("/api/patients/P04")
        self.assertEqual((await response.json())["stay_id"], PATIENTS[3]["stay_id"])
        self.assertEqual((await self.client.get("/api/patients/P05")).status, 404)

    async def test_health_without_database(self):
        response = await self.client.get("/api/health")
        self.assertEqual(response.status, 200)
        self.assertEqual((await response.json())["consumer_group"], GROUP_ID)

    async def test_one_ws_snapshot_and_all_three_updates(self):
        ws = await self.client.ws_connect("/ws")
        self.assertEqual((await ws.receive_json())["message_type"], "SNAPSHOT")
        for topic, value, kind in ((BUSINESS_TOPICS["telemetry"], processed_event(record()), "TELEMETRY_SAMPLE"),
                                  (BUSINESS_TOPICS["aggregates"], summary(), "VITALS_WINDOW_5M"),
                                  (BUSINESS_TOPICS["alerts"], alert(), "ALERT")):
            publish(self.client.server.app, topic, value)
            update = await asyncio.wait_for(ws.receive_json(), 2)
            self.assertEqual(update["message_type"], kind)
            self.assertEqual(update["payload"]["demo_id"], "P01")
        await ws.close()

    async def test_connect_snapshot_includes_existing_state(self):
        publish(self.client.server.app, BUSINESS_TOPICS["telemetry"], processed_event(record()))
        ws = await self.client.ws_connect("/ws")
        self.assertEqual((await ws.receive_json())["payload"]["P01"]["vitals"]["MAP"]["value"], event()["valuenum"])
        await ws.close()

    async def test_slow_client_cannot_block_other_clients(self):
        slow, fast = asyncio.Queue(maxsize=1), asyncio.Queue(maxsize=10)
        slow.put_nowait("old")
        self.client.server.app[CLIENTS].update((slow, fast))
        publish(self.client.server.app, BUSINESS_TOPICS["telemetry"], processed_event(record()))
        self.assertIsNone(slow.get_nowait())
        self.assertEqual(fast.get_nowait()["message_type"], "TELEMETRY_SAMPLE")

    async def test_out_of_order_backfill_cannot_regress_current_vitals(self):
        state = GatewayState()
        state.ingest(BUSINESS_TOPICS["telemetry"], processed_event(record(event(second=20))))
        self.assertIsNone(state.ingest(BUSINESS_TOPICS["telemetry"], processed_event(record(event(second=10)))))
        self.assertEqual(parse_timestamp(state.snapshot()["P01"]["vitals"]["MAP"]["event_time"]), parse_timestamp(event(second=20)["event_time"]))

    async def test_alert_redelivery_is_idempotent(self):
        state = GatewayState()
        self.assertIsNotNone(state.ingest(BUSINESS_TOPICS["alerts"], alert()))
        self.assertIsNone(state.ingest(BUSINESS_TOPICS["alerts"], alert()))
        self.assertEqual(len(state.snapshot()["P01"]["alerts"]), 1)

    async def test_alert_is_forwarded_without_any_vital_or_summary(self):
        ws = await self.client.ws_connect("/ws")
        await ws.receive_json()
        emitted = "2026-10-17T00:00:00.000000Z"
        received = "2026-10-17T00:00:00.050000Z"
        value = dict(alert(), produced_at=emitted)
        publish(self.client.server.app, BUSINESS_TOPICS["alerts"], value, received_at=received)
        update = await asyncio.wait_for(ws.receive_json(), 0.5)
        self.assertEqual(update["message_type"], "ALERT")
        self.assertEqual(update["payload"]["alert_emitted_at"], emitted)
        self.assertEqual(update["payload"]["gateway_received_at"], received)
        self.assertEqual(self.client.server.app[STATE].snapshot()["P01"]["vitals"], {})
        await ws.close()

    async def test_gateway_rejects_fifth_identity_and_non_15m_summary(self):
        state = GatewayState()
        for topic, value in ((BUSINESS_TOPICS["alerts"], dict(alert(), stay_id=1)),
                             (BUSINESS_TOPICS["aggregates"], dict(summary(), event_type="OLD_WINDOW"))):
            with self.assertRaises(ValueError):
                state.ingest(topic, value)


class InfrastructureTests(unittest.TestCase):
    def test_gateway_uses_a_list_subscription_and_rebuilds_from_beginning(self):
        with patch("confluent_kafka.Consumer") as constructor, patch("confluent_kafka.Producer"):
            feed = KafkaFeed()
            feed.subscribe()
            args, kwargs = feed.consumer.subscribe.call_args
            self.assertEqual(args[0], list(SUBSCRIPTIONS))
            from confluent_kafka import OFFSET_BEGINNING, TopicPartition
            partition = TopicPartition(SUBSCRIPTIONS[0], 0, 100)
            kwargs["on_assign"](feed.consumer, [partition])
            self.assertEqual(partition.offset, OFFSET_BEGINNING)
            self.assertTrue(feed.connected)
            self.assertFalse(constructor.call_args.args[0]["enable.auto.commit"])

    def test_broker_listener_update_preserves_storage_and_identity(self):
        sys.path.insert(0, str(PROJECT / "docker/flink"))
        from prepare_kafka import updated_properties, properties, PROPERTIES
        original = ("node.id=1\ninter.broker.listener.name=PLAINTEXT\nlog.dirs=/existing/data\n"
                    "listeners=PLAINTEXT://:9092,CONTROLLER://:9093\n"
                    "advertised.listeners=PLAINTEXT://localhost:9092\n"
                    "listener.security.protocol.map=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT\n")
        updated = updated_properties(original)
        image_default = original.replace(
            "advertised.listeners=PLAINTEXT://localhost:9092\n",
            "advertised.listeners=PLAINTEXT://localhost:9092,CONTROLLER://localhost:9093\n")
        self.assertEqual(updated_properties(image_default), updated)
        self.assertEqual(properties(updated)["log.dirs"], "/existing/data")
        self.assertEqual(updated_properties(updated), updated)
        self.assertTrue(all(properties(updated)[key] == value for key, value in PROPERTIES.items()))
        with self.assertRaises(ValueError):
            updated_properties(original.replace("node.id=1", "node.id=2"))
        with self.assertRaises(ValueError):
            updated_properties(original.replace("localhost:9092", "localhost:19092"))

    def test_fixed_ports_and_provectus_config(self):
        text = (PROJECT / "docker/flink/compose.yaml").read_text()
        for expected in ("provectuslabs/kafka-ui:v0.7.2", "container_name: icu-kafka-ui", "127.0.0.1:8081:8081",
                         "127.0.0.1:8082:8080", "127.0.0.1:8083:8083", "KAFKA_CLUSTERS_0_NAME: ICU-LOCAL",
                         "KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: icu-kafka:9094", "name: icu-streaming"):
            self.assertIn(expected, text)
        self.assertNotIn("kafbat", text.lower())
        self.assertNotIn(":latest", text)

    def test_downstream_groups_and_subscriptions_independent(self):
        self.assertEqual(GROUP_ID, "icu-ui-gateway-v2")
        self.assertEqual(set(SUBSCRIPTIONS), {BUSINESS_TOPICS[k] for k in ("telemetry", "aggregates", "alerts")})
        text = (PROJECT / "services/realtime_gateway/gateway.py").read_text().lower()
        for dependency in ("psycopg", "sqlalchemy", "timescale", "5432"):
            self.assertNotIn(dependency, text)
        self.assertNotIn("depends_on:", (PROJECT / "docker/flink/compose.yaml").read_text().split("  realtime-gateway:")[1].split("networks:")[0])

    def test_only_15m_core_and_no_waveform_input(self):
        self.assertEqual(set(TOPICS), {"chartevents", "labevents"})
        self.assertIn("icu.vitals.5m",CORE_TOPICS)
        text = (PROJECT / "jobs/flink/runtime_pipeline.py").read_text()
        self.assertNotIn("waveform", text)
        self.assertNotIn("clinical.events", text)
        self.assertNotIn("outputevents", text)
        RuntimeConfig().validate()
        WindowAlertConfig().validate()


if __name__ == "__main__":
    unittest.main()
