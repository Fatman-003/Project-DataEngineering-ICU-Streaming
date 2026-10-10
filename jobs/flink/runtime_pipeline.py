"""TICKET-006: validate, managed dedup, logical event-time, and Kafka DLQ."""
import argparse
import json
from dataclasses import replace
from pathlib import Path
from contracts import parse_event, selected_topics
from runtime_config import RuntimeConfig, DLQ_MAX_MESSAGE_BYTES
from validation import ValidationError, validate_event
from dedup import dedup_identity, flink_deduplicator
from event_time import watermark_strategy, flink_time_classifier
from dlq import payload_bytes, rejection, rejection_log, encode, log_line


def parse_and_validate(source, stream):
    """Return (accepted record, rejection envelope); exactly one is non-null."""
    try:
        raw = payload_bytes(source)
        if raw is None:
            raise ValueError("Kafka tombstone has no JSON payload")
        event = parse_event(raw.decode("utf-8", errors="strict"))
    except (ValueError, TypeError, UnicodeError, RecursionError) as error:
        return None, rejection(source, stream, "PARSE_ERROR", str(error))
    try:
        validate_event(event, stream)
    except ValidationError as error:
        return None, rejection(source, stream, error.error_type, error.reason, event)
    try:
        encode(event)  # Reject float overflow in optional/nested fields as well.
    except ValueError:
        return None, rejection(source, stream, "VALUE_ERROR", "JSON numbers must be finite", event)
    return {"source": source, "event": event, "stream_type": stream}, None


def arguments():
    config = RuntimeConfig.from_env()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream", choices=("all", "vitals", "clinical", "waveform"), default="all")
    parser.add_argument("--offset", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--group-id", default="icu-flink-runtime")
    parser.add_argument("--bootstrap-servers", default=config.bootstrap_servers)
    parser.add_argument("--max-out-of-orderness-seconds", type=int,
                        default=config.max_out_of_orderness_seconds)
    parser.add_argument("--dedup-state-ttl-hours", type=int, default=config.dedup_state_ttl_hours)
    args = parser.parse_args()
    config = replace(config, bootstrap_servers=args.bootstrap_servers,
                     max_out_of_orderness_seconds=args.max_out_of_orderness_seconds,
                     dedup_state_ttl_hours=args.dedup_state_ttl_hours)
    config.validate()
    return args, config


def clean_log(value):
    record = json.loads(value)
    return log_line(record, record["classification"], **{field: record[field] for field in (
        "event_time_ms", "watermark_ms", "previous_max_ms")})


def audit_rejection(value):
    print(rejection_log(json.loads(value)))
    return value


def main():
    from pyflink.common import Types, WatermarkStrategy, Configuration
    from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction, OutputTag
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, KafkaSink
    from pyflink.datastream.connectors.base import DeliveryGuarantee
    from pyflink.java_gateway import get_gateway

    args, config = arguments()
    dlq_tag = OutputTag("rejected-record", Types.STRING())

    class ParseValidate(ProcessFunction):
        def __init__(self, stream):
            self.stream = stream

        def process_element(self, value, ctx):
            source = json.loads(value)
            record, failed = parse_and_validate(source, self.stream)
            if failed is not None:
                yield dlq_tag, encode(failed)
            else:
                yield encode(record)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(config.checkpoint_interval_ms)
    execution = Configuration()
    execution.set_integer("python.fn-execution.bundle.size", 1)
    env.configure(execution)
    env.get_config().set_auto_watermark_interval(200)
    for path in Path("/opt/flink/jobs").glob("*.py"):
        env.add_python_file(str(path))
    offsets = KafkaOffsetsInitializer.earliest() if args.offset == "earliest" else KafkaOffsetsInitializer.latest()
    gateway = get_gateway()
    errors = []
    for stream, topic in selected_topics(args.stream).items():
        builder = (KafkaSource.builder().set_bootstrap_servers(config.bootstrap_servers)
                   .set_topics(topic).set_group_id(args.group_id).set_starting_offsets(offsets)
                   .set_property("partition.discovery.interval.ms", "10000")
                   .set_property("request.timeout.ms", "10000")
                   .set_property("default.api.timeout.ms", "15000"))
        builder._j_builder.setDeserializer(gateway.jvm.org.icu.streaming.KafkaMetadataDeserializer())
        source = env.from_source(builder.build(), WatermarkStrategy.no_watermarks(),
                                 f"RAW metadata: {stream}").uid(f"runtime-source-{stream}")
        valid = source.process(ParseValidate(stream), output_type=Types.STRING()).name(
            f"Parse and validate: {stream}").uid(f"runtime-validate-{stream}")
        deduped = valid.key_by(
            lambda value, stream=stream: dedup_identity(json.loads(value)["event"], stream),
            key_type=Types.STRING()).process(
                flink_deduplicator(config.dedup_state_ttl_hours, dlq_tag), output_type=Types.STRING()
            ).name(f"Managed dedup: {stream}").uid(f"runtime-dedup-{stream}")
        timed = deduped.assign_timestamps_and_watermarks(
            watermark_strategy(config.max_out_of_orderness_seconds)).name(f"Event-time: {stream}")
        clean = timed.key_by(lambda value: json.loads(value)["event"]["stay_id"],
                             key_type=Types.STRING()).process(
            flink_time_classifier(config.dedup_state_ttl_hours, dlq_tag), output_type=Types.STRING()
        ).name(f"Classify event-time: {stream}").uid(f"runtime-time-{stream}")
        clean.map(clean_log, output_type=Types.STRING()).name(f"Clean audit: {stream}").print("icu-clean")
        errors.extend((valid.get_side_output(dlq_tag), deduped.get_side_output(dlq_tag),
                       clean.get_side_output(dlq_tag)))
    rejected = errors[0].union(*errors[1:]).map(audit_rejection, output_type=Types.STRING())
    sink_builder = (KafkaSink.builder().set_bootstrap_servers(config.bootstrap_servers)
                    .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .set_property("acks", "all")
                    .set_property("request.timeout.ms", "10000")
                    .set_property("delivery.timeout.ms", "30000"))
    sink_builder.set_property("max.request.size", str(DLQ_MAX_MESSAGE_BYTES))
    sink_builder._j_builder.setRecordSerializer(
        gateway.jvm.org.icu.streaming.DeadLetterSerializationSchema(config.dlq_topic))
    rejected.sink_to(sink_builder.build()).name("Kafka DLQ sink").uid("runtime-dlq-sink")
    print(f"Runtime TICKET-006 config={config}, stream={args.stream}, offset={args.offset}", flush=True)
    env.execute("ICU runtime validation / dedup / event-time / DLQ — TICKET-006")


if __name__ == "__main__":
    main()
