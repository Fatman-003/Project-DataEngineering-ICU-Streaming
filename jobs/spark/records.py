"""Kafka row <-> shared Flink record shapes, without any Spark import.

`source_record` builds the same envelope as Flink's KafkaMetadataDeserializer, so
`dlq.rejection` sees identical input. `kafka_record` mirrors the key and header
rules of Flink's DeadLetterSerializationSchema, which every Flink sink uses.
"""
import base64
from business_contract import encode, headers

OUTPUT_SCHEMA = "topic string, key string, value string, headers array<struct<key:string,value:binary>>"


def source_record(topic, partition, offset, key, value, kafka_headers):
    return {"topic": topic, "partition": int(partition), "offset": int(offset),
            "headers": {h["key"]: None if h["value"] is None else bytes(h["value"]).decode("utf-8", errors="replace")
                        for h in (kafka_headers if kafka_headers is not None else [])},
            "key_base64": None if key is None else base64.b64encode(bytes(key)).decode(),
            "payload_base64": None if value is None else base64.b64encode(bytes(value)).decode()}


def record_key(envelope):
    for field in ("stay_id", "mapped_stay_id"):
        value = envelope.get(field)
        if isinstance(value, (int, str)) and not isinstance(value, bool) and str(value):
            return str(value)
    if isinstance(envelope.get("kafka_key"), str):
        return envelope["kafka_key"]
    if not isinstance(envelope.get("dlq_id"), str) or not envelope["dlq_id"]:
        raise ValueError("DLQ envelope has no stable fallback key")
    return envelope["dlq_id"]


def kafka_record(topic, envelope):
    """One output row for the Kafka sink: (topic, key, value, headers)."""
    return (topic, record_key(envelope), encode(envelope),
            [{"key": name, "value": data} for name, data in headers(envelope)])
