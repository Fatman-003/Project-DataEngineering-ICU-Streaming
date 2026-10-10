"""TICKET-005: KafkaSource -> string -> RAW JSON inspection -> print."""
import argparse
import os
from contracts import selected_topics, inspection_line


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream", choices=("all", "vitals", "clinical", "waveform"),
                        default="all")
    parser.add_argument("--bootstrap-servers",
                        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "icu-kafka:9094"))
    parser.add_argument("--group-id", default="icu-flink-raw-inspect")
    parser.add_argument("--offset", choices=("earliest", "latest"), default="earliest")
    return parser.parse_args()


def main():
    # PyFlink exists only inside the pinned Docker image, not in the host .venv.
    from pyflink.common import Types, WatermarkStrategy
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction
    from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer

    class InspectRaw(FlatMapFunction):
        def __init__(self, stream):
            self.stream = stream

        def flat_map(self, raw):
            yield inspection_line(raw, self.stream)

    args = arguments()
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10000)
    env.add_python_file("/opt/flink/jobs/contracts.py")
    offsets = KafkaOffsetsInitializer.earliest() if args.offset == "earliest" else KafkaOffsetsInitializer.latest()
    for stream, topic in selected_topics(args.stream).items():
        source = (KafkaSource.builder()
                  .set_bootstrap_servers(args.bootstrap_servers)
                  .set_topics(topic)
                  .set_group_id(args.group_id)
                  .set_starting_offsets(offsets)
                  .set_value_only_deserializer(SimpleStringSchema())
                  .set_property("partition.discovery.interval.ms", "10000")
                  .set_property("request.timeout.ms", "10000")
                  .set_property("default.api.timeout.ms", "15000")
                  .build())
        raw = env.from_source(source, WatermarkStrategy.no_watermarks(),
                              f"RAW Kafka: {topic}").uid(f"source-{stream}")
        inspected = raw.flat_map(InspectRaw(stream), output_type=Types.STRING()).name(
            f"Inspect JSON: {stream}").uid(f"inspect-{stream}")
        inspected.print(f"icu-raw-{stream}").name(f"Verification log: {stream}")
    print(f"RAW inspection: {selected_topics(args.stream)}, bootstrap={args.bootstrap_servers}, "
          f"offset={args.offset}, parallelism=1, checkpoints=10s", flush=True)
    env.execute("ICU RAW JSON inspection — TICKET-005")


if __name__ == "__main__":
    main()
