"""Bounded LONG-to-WIDE assembly and Kafka sinks with versioned headers."""
import json
from business_contract import FIELDS, metadata, encode, BUSINESS_TOPICS
from runtime_config import DLQ_MAX_MESSAGE_BYTES
from event_time import event_timestamp_ms
from window_aggregates import iso_timestamp
from dlq import rejection
from validation import parse_timestamp


def telemetry(event, values):
    sample_time = parse_timestamp(event["event_time"]).isoformat(timespec="microseconds").replace("+00:00", "Z")
    return dict(metadata("TELEMETRY_SAMPLE", [event["stay_id"], sample_time]),
                sample_time=sample_time, stay_id=event["stay_id"],
                **{field: values.get(field) for field in FIELDS.values()})


def flink_telemetry(ttl_hours, dlq_tag):
    from pyflink.common import Types
    from pyflink.datastream import KeyedProcessFunction
    from pyflink.datastream.state import MapStateDescriptor
    from dedup import managed_ttl

    class Assemble(KeyedProcessFunction):
        def open(self, runtime_context):
            descriptor = MapStateDescriptor("wide-ticks", Types.LONG(), Types.STRING())
            descriptor.enable_time_to_live(managed_ttl(ttl_hours))
            self.pending = runtime_context.get_map_state(descriptor)

        def process_element(self, value, ctx):
            record = json.loads(value)
            event = record["event"]
            field = FIELDS.get(event["signal_name"])
            if field is None:
                return
            timestamp = event_timestamp_ms(event)
            stored = self.pending.get(timestamp)
            samples = {} if stored is None else json.loads(stored)
            sample_time = parse_timestamp(event["event_time"]).isoformat(timespec="microseconds")
            bucket = samples.setdefault(sample_time, {"event": event, "values": {}, "emitted": False})
            values = bucket["values"]
            if bucket["emitted"]:
                if values.get(field) != event["valuenum"]:
                    yield dlq_tag, encode(rejection(record["source"], "chartevents",
                                                   "IDEMPOTENCY_CONFLICT", "Finalized wide sample cannot be corrected", event))
                return
            values[field] = min(values.get(field, event["valuenum"]), event["valuenum"])
            if len(values) == len(FIELDS):
                bucket["emitted"] = True
                yield encode(telemetry(event, values))
            self.pending.put(timestamp, encode(samples))
            ctx.timer_service().register_event_time_timer(timestamp)

        def on_timer(self, timestamp, ctx):
            stored = self.pending.get(timestamp)
            if stored is not None:
                self.pending.remove(timestamp)
                for _, bucket in sorted(json.loads(stored).items()):
                    if not bucket["emitted"] and bucket["values"]:
                        yield encode(telemetry(bucket["event"], bucket["values"]))

    return Assemble()


def attach_telemetry(clean, ttl_hours, dlq_tag):
    from pyflink.common import Types
    return clean.key_by(lambda value: str(json.loads(value)["event"]["stay_id"]), key_type=Types.STRING()).process(
        flink_telemetry(ttl_hours, dlq_tag), output_type=Types.STRING()).name(
        "One wide telemetry per stay/sample_time").uid("business-wide")


def attach_sink(stream, name, config):
    from pyflink.datastream.connectors.kafka import KafkaSink
    from pyflink.datastream.connectors.base import DeliveryGuarantee
    from pyflink.java_gateway import get_gateway
    topic = BUSINESS_TOPICS.get(name, config.dlq_topic if name == "deadletter" else None)
    if topic is None:
        raise ValueError(f"Unknown sink {name}")
    builder = (KafkaSink.builder().set_bootstrap_servers(config.bootstrap_servers)
               .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
               .set_property("acks", "all").set_property("linger.ms", "0")
               .set_property("max.request.size", str(DLQ_MAX_MESSAGE_BYTES)))
    builder._j_builder.setRecordSerializer(
        get_gateway().jvm.org.icu.streaming.DeadLetterSerializationSchema(topic))
    stream.sink_to(builder.build()).name("Kafka: " + topic).uid("business-sink-" + name)
