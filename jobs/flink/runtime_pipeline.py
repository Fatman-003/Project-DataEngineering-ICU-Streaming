"""Canonical V2 Flink job: two RAW inputs, four business sinks, one deadletter sink."""
import argparse
import json
from dataclasses import replace
from pathlib import Path
from contracts import parse_event, selected_topics, stay_key
from runtime_config import RuntimeConfig, WindowAlertConfig
from validation import ValidationError, validate_event
from dedup import dedup_identity, flink_deduplicator
from event_time import watermark_strategy, flink_time_classifier
from dlq import payload_bytes, rejection, encode
from business_outputs import attach_sink, processed_json


def parse_and_validate(source, stream):
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
    return {"source": source, "event": event, "stream_type": stream}, None


def main():
    from pyflink.common import Types, WatermarkStrategy, Configuration
    from pyflink.datastream import StreamExecutionEnvironment, ProcessFunction, OutputTag
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
    from pyflink.java_gateway import get_gateway
    from window_aggregates import attach_windows
    from hypotension import attach_hypotension

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--offset", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--checkpoint-interval-ms", type=int, default=10000)
    args = parser.parse_args()
    config, business = RuntimeConfig.from_env(), WindowAlertConfig.from_env()
    config = replace(config, checkpoint_interval_ms=args.checkpoint_interval_ms)
    config.validate()
    dlq_tag = OutputTag("v2-rejected", Types.STRING())

    class ParseValidate(ProcessFunction):
        def __init__(self, stream):
            self.stream = stream

        def process_element(self, value, ctx):
            record, failed = parse_and_validate(json.loads(value), self.stream)
            if failed is not None:
                yield dlq_tag, encode(failed)
            else:
                yield encode(record)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(config.checkpoint_interval_ms)
    execution = Configuration()
    execution.set_integer("python.fn-execution.bundle.size", 50)
    execution.set_integer("python.fn-execution.bundle.time", 50)
    env.configure(execution)
    env.get_config().set_auto_watermark_interval(200)
    for path in Path("/opt/flink/jobs").glob("*.py"):
        env.add_python_file(str(path))
    offsets = KafkaOffsetsInitializer.earliest() if args.offset == "earliest" else KafkaOffsetsInitializer.latest()
    errors = []
    for stream, topic in selected_topics().items():
        builder = (KafkaSource.builder().set_bootstrap_servers(config.bootstrap_servers)
                   .set_topics(topic).set_group_id("icu-flink-v2").set_starting_offsets(offsets)
                   .set_property("partition.discovery.interval.ms", "10000"))
        builder._j_builder.setDeserializer(get_gateway().jvm.org.icu.streaming.KafkaMetadataDeserializer())
        source = env.from_source(builder.build(), WatermarkStrategy.no_watermarks(), topic).uid("v2-source-" + stream)
        valid = source.process(ParseValidate(stream), output_type=Types.STRING()).name(
            "Validate " + stream).uid("v2-validate-" + stream)
        deduped = valid.key_by(lambda value, stream=stream: dedup_identity(json.loads(value)["event"], stream),
                              key_type=Types.STRING()).process(
            flink_deduplicator(config.dedup_state_ttl_hours, dlq_tag), output_type=Types.STRING()
        ).name("Managed dedup " + stream).uid("v2-dedup-" + stream)
        timed = deduped.assign_timestamps_and_watermarks(
            watermark_strategy(config.max_out_of_orderness_seconds))
        clean = timed.key_by(lambda value: stay_key(json.loads(value)["event"]), key_type=Types.STRING()).process(
            flink_time_classifier(config.dedup_state_ttl_hours, dlq_tag), output_type=Types.STRING()
        ).name("Event-time quality " + stream).uid("v2-time-" + stream)
        attach_sink(clean.map(processed_json, output_type=Types.STRING()), stream, config)
        if stream == "chartevents":
            attach_sink(attach_windows(clean, business), "aggregates", config)
            attach_sink(attach_hypotension(clean, business, config.dedup_state_ttl_hours), "alerts", config)
        errors.extend((valid.get_side_output(dlq_tag), deduped.get_side_output(dlq_tag),
                       clean.get_side_output(dlq_tag)))
    attach_sink(errors[0].union(*errors[1:]), "deadletter", config)
    env.execute("ICU V2 telemetry, 15m statistics and sustained hypotension")


if __name__ == "__main__":
    main()
