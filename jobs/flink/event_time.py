"""Logical event-time extraction and effective Flink-watermark classification."""
import json
from datetime import datetime, timezone, timedelta
from validation import parse_timestamp


def event_timestamp_ms(event):
    value = parse_timestamp(event["event_time"], "event_time")
    return (value - datetime(1970, 1, 1, tzinfo=timezone.utc)) // timedelta(milliseconds=1)


def classify_event(timestamp_ms, watermark_ms, previous_max_ms):
    # Flink has already declared timestamps <= its effective watermark complete.
    if timestamp_ms <= watermark_ms:
        return "LATE_EVENT"
    if previous_max_ms is not None and timestamp_ms < previous_max_ms:
        return "OUT_OF_ORDER_EVENT"
    return "CLEAN_EVENT"


def watermark_strategy(tolerance_seconds):
    from pyflink.common import Duration, WatermarkStrategy
    from pyflink.common.watermark_strategy import TimestampAssigner

    class LogicalTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return event_timestamp_ms(json.loads(value)["event"])

    return (WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(tolerance_seconds))
            .with_idleness(Duration.of_seconds(5))
            .with_timestamp_assigner(LogicalTimestampAssigner()))


def flink_time_classifier(ttl_hours, dlq_tag):
    from pyflink.common import Types
    from pyflink.datastream import KeyedProcessFunction
    from pyflink.datastream.state import ValueStateDescriptor
    from dedup import managed_ttl
    from dlq import encode, rejection

    class ClassifyTime(KeyedProcessFunction):
        def open(self, runtime_context):
            descriptor = ValueStateDescriptor("max-clean-event-time", Types.LONG())
            descriptor.enable_time_to_live(managed_ttl(ttl_hours))
            self.maximum = runtime_context.get_state(descriptor)

        def process_element(self, value, ctx):
            record = json.loads(value)
            timestamp = ctx.timestamp()
            watermark = ctx.timer_service().current_watermark()
            maximum = self.maximum.value()
            kind = classify_event(timestamp, watermark, maximum)
            if kind == "LATE_EVENT":
                yield dlq_tag, encode(rejection(
                    record["source"], record["stream_type"], kind,
                    f"event_time_ms={timestamp} <= effective_watermark_ms={watermark}", record["event"]))
                return
            if maximum is None or timestamp > maximum:
                self.maximum.update(timestamp)
            record.update(classification=kind, event_time_ms=timestamp,
                          watermark_ms=watermark, previous_max_ms=maximum)
            # Preserve the complete validated event and source envelope internally.
            yield encode(record)

    return ClassifyTime()
