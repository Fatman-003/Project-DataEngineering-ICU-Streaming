"""Deterministic identity and Flink managed-state deduplication."""
import json


def dedup_identity(event, stream):
    return f"{stream}:id:{event['event_id']}"


def check_and_remember(state):
    """ValueState interface, shared by the managed operator and pure unit tests."""
    if state.value() is not None:
        return True
    state.update(True)
    return False


def managed_ttl(hours):
    from pyflink.common import Time
    from pyflink.datastream.state import StateTtlConfig
    return (StateTtlConfig.new_builder(Time.hours(hours))
            .update_ttl_on_create_and_write().never_return_expired()
            .cleanup_incrementally(10, True).build())


def flink_deduplicator(ttl_hours, dlq_tag):
    from pyflink.common import Types
    from pyflink.datastream import KeyedProcessFunction
    from pyflink.datastream.state import ValueStateDescriptor
    from dlq import encode, rejection

    class Deduplicate(KeyedProcessFunction):
        def open(self, runtime_context):
            descriptor = ValueStateDescriptor("seen-event", Types.BOOLEAN())
            descriptor.enable_time_to_live(managed_ttl(ttl_hours))
            self.seen = runtime_context.get_state(descriptor)

        def process_element(self, value, ctx):
            record = json.loads(value)
            if check_and_remember(self.seen):
                yield dlq_tag, encode(rejection(
                    record["source"], record["stream_type"], "DUPLICATE_EVENT",
                    "Event identity already seen within processing-time state TTL", record["event"]))
            else:
                yield value

    return Deduplicate()
