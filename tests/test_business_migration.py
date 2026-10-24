"""Versioned contracts, bounded wide assembly and migration/lifecycle safety."""
import base64
import copy
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch, sentinel

PROJECT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(PROJECT/"jobs/flink"),str(PROJECT/"scripts/demo"),str(PROJECT/"services/storage_writer"),
              str(PROJECT/"services/realtime_gateway")]
from business_contract import (validate, metadata, headers, deadletter, ContractError, FIELDS, stable_uuid, timestamp)
from business_outputs import flink_telemetry, telemetry
from contracts import BUSINESS_TOPICS, CORE_TOPICS, parse_event
from run_icu_demo import PATIENTS, DEFAULT_START, chart_event, events_at
from test_map_operator import ManagedOperatorFixture
from test_storage import FakeConnection, FakeConsumer
from db import persist_one, mapped_record
from writer import reject_and_commit
from db_admin import migrate_sources, retire_legacy
from gateway import GatewayState
from event_time import event_timestamp_ms


def wide():
    event=chart_event(PATIENTS[0],"MAP",0,DEFAULT_START)
    return telemetry(event,{"heart_rate":80,"map":62,"sbp":120,"spo2":98})


def source(value=b'{"bad":'):
    return dict(topic="icu.v2.chartevents",partition=0,offset=7,
                payload_base64=base64.b64encode(value).decode(),key_base64=base64.b64encode(b"35727289").decode(),
                headers={"x-original":"unchanged"})


class BusinessContractTests(unittest.TestCase):
    def test_metadata_key_headers_and_supported_minor(self):
        event=wide()
        validate(event,"icu.telemetry",str(event["stay_id"]).encode(),headers(event),True)
        event["schema_version"]="1.1"
        event["future_optional"]=True
        validate(event,"icu.telemetry",str(event["stay_id"]).encode(),headers(event),True)

    def test_major_malformed_uuid_key_and_header_mismatch(self):
        for changes,key,hdr in (({"schema_version":"2.0"},None,None),({"message_id":"BAD"},None,None),
                                ({},b"foreign",None),({},None,[])):
            event=dict(wide(),**changes)
            with self.assertRaises(ContractError):
                validate(event,"icu.telemetry",key,headers(event) if hdr is None else hdr,True)

    def test_numeric_ranges_types_nulls_unknown_optional_and_missing(self):
        for value in (-1,351,float("nan"),float("inf"),"80",True):
            with self.subTest(value=value),self.assertRaises(ContractError):
                validate(dict(wide(),heart_rate=value),"icu.telemetry")
        event=wide()
        event.update(heart_rate=None,map=None,sbp=None)
        validate(event,"icu.telemetry")
        with self.assertRaises(ContractError): validate(dict(event,spo2=None),"icu.telemetry")
        for field in ("stay_id","message_id","produced_at"):
            event=wide();del event[field]
            with self.assertRaises(ContractError): validate(event,"icu.telemetry")

    def test_timestamps_and_window_alignment(self):
        for value in ("2026-10-23","2026-10-23T10:00:00","2026-10-23T10:00:00+07:00","2026-99-23T10:00:00Z"):
            with self.assertRaises(ContractError): timestamp(value)
        event=dict(metadata("VITALS_WINDOW_5M","w"),window_end="2026-10-23T10:00:01Z",
                   stay_id=PATIENTS[0]["stay_id"],avg_map=62)
        with self.assertRaises(ContractError): validate(event,"icu.vitals.5m")
        event["window_end"]="2026-10-23T10:00:00Z"
        validate(event,"icu.vitals.5m")

    def test_alert_enums_null_trigger_and_stable_episode_identity(self):
        from hypotension import alert_identity
        event=dict(metadata("SEPSIS_ALERT_CREATED","a","icu-alert-engine"),stay_id=PATIENTS[0]["stay_id"],
                   alert_id=alert_identity(PATIENTS[0]["stay_id"],0),alert_time="2026-10-23T10:00:00Z",
                   alert_type="HYPOTENSION_SUSTAINED",severity="CRITICAL",trigger_value=None,
                   trigger_message="Screening alert",status="ACTIVE")
        for kind in ("HYPOTENSION_SUSTAINED","QSOFA_HIGH_RISK","SENSOR_DISCONNECTED"):
            validate(dict(event,alert_type=kind),"icu.alerts")
        for changes in ({"severity":"HIGH"},{"status":"ACKNOWLEDGED"},{"alert_type":"SUSTAINED_HYPOTENSION"}):
            with self.assertRaises(ContractError): validate(dict(event,**changes),"icu.alerts")
        self.assertEqual(event["alert_id"],alert_identity(PATIENTS[0]["stay_id"],0))
        self.assertNotEqual(event["alert_id"],alert_identity(PATIENTS[0]["stay_id"],1))

    def test_dlq_preserves_bytes_json_headers_and_original_key(self):
        for raw in (b'{"secret_free":1}',b"\xff\x00",b'{"bad":',b'{"v":1e999}',b'{"v":1,"v":2}'):
            original=source(raw)
            result=deadletter(original,"JSON_PARSE_ERROR","safe error",stage="DESERIALIZATION")
            self.assertEqual(result["source_topic"],"icu.v2.chartevents")
            self.assertEqual(result["original_headers"],original["headers"])
            self.assertEqual(result["kafka_key"],"35727289")
            if raw.startswith(b'{"secret_free"'):
                self.assertEqual(result["original_payload"],{"secret_free":1})
                self.assertIsNone(result["raw_payload_base64"])
            else:
                self.assertIsNone(result["original_payload"])
                self.assertEqual(base64.b64decode(result["raw_payload_base64"]),raw)


class WideOperatorTests(unittest.TestCase):
    def fixture(self):
        return ManagedOperatorFixture(factory=lambda:flink_telemetry(24,sentinel.dlq))

    def test_24_long_events_produce_four_wide_messages_and_no_extra_fields(self):
        outputs=[]
        for patient in PATIENTS:
            fixture=self.fixture()
            for signal in ("HR","MAP","SBP","DBP","SpO2","RR"):
                outputs+=fixture.deliver({"event":chart_event(patient,signal,0,DEFAULT_START)})
            self.assertEqual(fixture.advance_watermark(1),[])
        self.assertEqual(len(outputs),4)
        for event in outputs:
            validate(event,"icu.telemetry")
            self.assertNotIn("dbp",event)
            self.assertNotIn("rr",event)

    def test_missing_signal_finalizes_with_null_on_watermark(self):
        fixture=self.fixture()
        fixture.deliver({"event":chart_event(PATIENTS[0],"MAP",0,DEFAULT_START)})
        result=fixture.advance_watermark(1)
        self.assertEqual(len(result),1)
        self.assertEqual(result[0]["map"],chart_event(PATIENTS[0],"MAP",0,DEFAULT_START)["valuenum"])
        self.assertIsNone(result[0]["heart_rate"])
        self.assertEqual(fixture.pending.stored,{})

    def test_component_order_is_deterministic_and_only_one_logical_output(self):
        outputs=[]
        for order in (("HR","MAP","SBP","SpO2"),("SpO2","SBP","MAP","HR")):
            fixture=self.fixture();current=[]
            for signal in order:
                current+=fixture.deliver({"event":chart_event(PATIENTS[0],signal,0,DEFAULT_START)})
            self.assertEqual(len(current),1)
            outputs.append(current[0])
        # Message identity must use a canonical timestamp, independent of the last component.
        self.assertEqual(outputs[0]["message_id"],outputs[1]["message_id"])
        for f in FIELDS.values(): self.assertEqual(outputs[0][f],outputs[1][f])

    def test_equivalent_timestamp_formats_share_one_wide_identity(self):
        fixture=self.fixture()
        outputs=[]
        for index,signal in enumerate(FIELDS):
            event=chart_event(PATIENTS[0],signal,0,DEFAULT_START)
            if index % 2: event["event_time"]=event["event_time"].replace("Z",".000000+00:00")
            outputs+=fixture.deliver({"event":event})
        self.assertEqual(len(outputs),1)
        self.assertEqual(outputs[0]["message_id"],wide()["message_id"])

    def test_distinct_microsecond_samples_in_same_flink_millisecond_do_not_collide(self):
        fixture=self.fixture(); outputs=[]
        for second in (0.0001,0.0002):
            for signal in FIELDS:
                outputs+=fixture.deliver({"event":chart_event(PATIENTS[0],signal,second,DEFAULT_START)})
        self.assertEqual(len(outputs),2)
        self.assertNotEqual(outputs[0]["sample_time"],outputs[1]["sample_time"])
        self.assertNotEqual(outputs[0]["message_id"],outputs[1]["message_id"])

    def test_accepted_component_cannot_correct_already_emitted_record(self):
        fixture=self.fixture()
        for signal in FIELDS:
            fixture.deliver({"event":chart_event(PATIENTS[0],signal,0,DEFAULT_START)})
        event=chart_event(PATIENTS[0],"HR",0,DEFAULT_START,"-different")
        event["valuenum"]=90
        self.assertEqual(fixture.deliver({"event":event,"source":source(json.dumps(event).encode())}),[])
        self.assertEqual(fixture.dlq[0]["error_code"],"IDEMPOTENCY_CONFLICT")


class LifecycleMigrationTests(unittest.TestCase):
    def test_existing_alert_update_and_missing_alert_are_not_inserts(self):
        from test_v2 import alert
        event=alert()
        update=dict(metadata("SEPSIS_ALERT_STATUS_CHANGED","u","icu-api"),
                    **{k:event[k] for k in ("alert_time","alert_id","stay_id")},status="ACKNOWLEDGED")
        connection=FakeConnection()
        connection.rowcount=1
        persist_one(connection,"icu.alerts",update,str(event["stay_id"]).encode())
        self.assertEqual(connection.calls,["begin","insert","db_commit"])
        connection.rowcount=0
        with self.assertRaises(ContractError) as failure:
            persist_one(connection,"icu.alerts",update,str(event["stay_id"]).encode())
        self.assertEqual(failure.exception.code,"ALERT_NOT_FOUND")
        self.assertEqual(connection.calls[-1],"rollback")

    def test_gateway_status_update_changes_one_alert_and_accepts_equivalent_clock(self):
        from test_v2 import alert
        event=alert();state=GatewayState()
        state.ingest("icu.alerts",event)
        update=dict(metadata("SEPSIS_ALERT_STATUS_CHANGED","u","icu-api"),
                    **{k:event[k] for k in ("alert_time","alert_id","stay_id")},status="FALSE_ALARM")
        update["alert_time"]=update["alert_time"].replace(".000Z","Z")
        state.ingest("icu.alerts",update)
        self.assertIsNone(state.ingest("icu.alerts",update))
        self.assertEqual(len(state.snapshot()["P01"]["alerts"]),1)
        self.assertEqual(state.snapshot()["P01"]["alerts"][0]["status"],"FALSE_ALARM")

    def test_breaking_schema_requires_backup_and_quiescence(self):
        connection=Mock()
        connection.execute.return_value.fetchone.return_value=(1,)
        for backup,quiesced in ((None,True),("missing",True),(None,False)):
            with self.assertRaises(RuntimeError): migrate_sources(connection,backup,quiesced)
        self.assertFalse(any("DROP" in str(c) for c in connection.execute.call_args_list))
        with self.assertRaises(RuntimeError): retire_legacy(connection)

    def test_dlq_failure_does_not_commit_original_offset(self):
        consumer,producer,message=Mock(),Mock(),Mock()
        message.topic.return_value="icu.telemetry"
        message.partition.return_value=0;message.offset.return_value=0
        message.key.return_value=b"35727289";message.value.return_value=b"{}";message.headers.return_value=[]
        producer.flush.return_value=1
        with self.assertRaises(RuntimeError):
            reject_and_commit(consumer,producer,message,"DB_WRITE_ERROR","STORAGE",True)
        consumer.commit.assert_not_called()
        producer.flush.return_value=0
        reject_and_commit(consumer,producer,message,"DB_WRITE_ERROR","STORAGE",True)
        consumer.commit.assert_called_once()
