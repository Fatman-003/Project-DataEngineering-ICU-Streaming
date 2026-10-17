"""Small cleaned contracts and independent Kafka sinks."""
import json
from datetime import datetime, timezone
from dlq import encode
from contracts import BUSINESS_TOPICS
from runtime_config import DLQ_MAX_MESSAGE_BYTES


def processed_event(record, processed_at=None):
    return dict(record["event"], quality_status="VALID",
                ordering_status="OUT_OF_ORDER" if record["classification"] == "OUT_OF_ORDER_EVENT" else "IN_ORDER",
                processed_at=processed_at or datetime.now(timezone.utc).isoformat())


def processed_json(value):
    return encode(processed_event(json.loads(value)))


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
    stream.sink_to(builder.build()).name("Kafka: " + topic).uid("v2-sink-" + name)