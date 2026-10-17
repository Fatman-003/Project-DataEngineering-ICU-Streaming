"""Immediate MAP alerts from accepted observations; watermarks compact history only.

Known observations are replayed in event-time order, with recovery winning ties.
An urgent alert cannot anticipate a recovery that has not arrived yet; delivery
is not retracted if a subsequently accepted out-of-order recovery changes history.
"""
import hashlib
import json
from datetime import datetime, timezone
from dlq import encode
from event_time import event_timestamp_ms
from registry import IDENTITY_FIELDS
from window_aggregates import iso_timestamp

ALERT_TYPE = "SUSTAINED_HYPOTENSION"


def alert_identity(stay_id, episode_start_ms):
    return "alert-" + hashlib.sha256(encode([stay_id, ALERT_TYPE, episode_start_ms]).encode()).hexdigest()


def select_timestamp_observation(records, threshold):
    if not records:
        raise ValueError("Empty MAP bucket")
    recoveries = [record for record in records if record["event"]["valuenum"] >= threshold]
    return min(recoveries or records, key=lambda record: record["event"]["event_id"])


def alert_candidate(state, record, config):
    event = record["event"]
    return {
        "schema_version": "2.0", "alert_id": alert_identity(event["stay_id"], state["start_ms"]),
        **{field: event[field] for field in IDENTITY_FIELDS},
        "alert_type": ALERT_TYPE, "severity": "HIGH", "signal_name": "MAP",
        "trigger_value": event["valuenum"], "threshold_value": config.map_threshold,
        "episode_start": iso_timestamp(state["start_ms"]),
        "trigger_time": event["event_time"], "status": "ACTIVE",
    }


def advance_episode(state, record, config):
    timestamp = event_timestamp_ms(record["event"])
    value = record["event"]["valuenum"]
    if state is not None and timestamp < state["last_low_ms"]:
        raise ValueError("MAP records must be finalized in event-time order")
    if value >= config.map_threshold:
        return None, []
    if state is not None and timestamp - state["last_low_ms"] > config.map_max_sample_gap_seconds * 1000:
        state = None
    state = ({"start_ms": timestamp, "last_low_ms": timestamp, "alerted": False}
             if state is None else dict(state, last_low_ms=timestamp))
    results = []
    if not state["alerted"] and timestamp - state["start_ms"] >= config.map_sustained_seconds * 1000:
        results.append(alert_candidate(state, record, config))
        state["alerted"] = True
    return state, results


def evaluate_pending(state, records, config):
    """Return a candidate only for the currently active, observed episode."""
    candidate = None
    for record in records:
        previous_start = None if state is None else state["start_ms"]
        state, results = advance_episode(state, record, config)
        if state is None or state["start_ms"] != previous_start:
            candidate = None
        if results:
            candidate = results[0]
    return state, candidate


def flink_hypotension(config, ttl_hours):
    from pyflink.common import Types
    from pyflink.datastream import KeyedProcessFunction
    from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
    from dedup import managed_ttl

    class Hypotension(KeyedProcessFunction):
        def open(self, runtime_context):
            pending = MapStateDescriptor("map-pending", Types.LONG(), Types.STRING())
            episode = ValueStateDescriptor("map-episode", Types.STRING())
            emitted = MapStateDescriptor("map-emitted-episodes", Types.LONG(), Types.BOOLEAN())
            for descriptor in (pending, episode, emitted):
                descriptor.enable_time_to_live(managed_ttl(ttl_hours))
            self.pending = runtime_context.get_map_state(pending)
            self.episode = runtime_context.get_state(episode)
            self.emitted = runtime_context.get_map_state(emitted)

        def process_element(self, value, ctx):
            record = json.loads(value)
            timestamp = event_timestamp_ms(record["event"])
            if timestamp <= ctx.timer_service().current_watermark():
                return []  # Defensive: upstream late classification normally rejects it.
            current = self.pending.get(timestamp)
            representative = select_timestamp_observation(
                [record] if current is None else [json.loads(current), record], config.map_threshold)
            self.pending.put(timestamp, encode(representative))
            ctx.timer_service().register_event_time_timer(timestamp)
            current_episode = self.episode.value()
            state, candidate = evaluate_pending(
                None if current_episode is None else json.loads(current_episode),
                [json.loads(value) for _, value in sorted(self.pending.items())], config)
            if candidate is not None and not self.emitted.contains(state["start_ms"]):
                self.emitted.put(state["start_ms"], True)
                candidate["alert_emitted_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
                return [encode(candidate)]
            return []

        def on_timer(self, timestamp, ctx):
            current = self.pending.get(timestamp)
            if current is None:
                return
            self.pending.remove(timestamp)
            current_episode = self.episode.value()
            state, _ = advance_episode(
                None if current_episode is None else json.loads(current_episode),
                json.loads(current), config)
            if state is None:
                self.episode.clear()
            else:
                self.episode.update(encode(state))
            return []  # Timers compact state; they never gate or deliver urgent alerts.

    return Hypotension()


def attach_hypotension(clean, config, ttl_hours):
    from pyflink.common import Types
    return clean.filter(lambda value: json.loads(value)["event"]["signal_name"] == "MAP").key_by(
        lambda value: str(json.loads(value)["event"]["stay_id"]), key_type=Types.STRING()).process(
        flink_hypotension(config, ttl_hours), output_type=Types.STRING()).name(
        "MAP <65 sustained 300 seconds").uid("v2-map-rule")
