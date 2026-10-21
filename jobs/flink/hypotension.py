"""Incremental MAP evidence with event-time recovery debounce.

In-order observations advance cached state directly. Out-of-order/tied buckets
replay only unfinalized history, with a normal MAP winning timestamp ties.
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
    """Advance one ordered MAP observation; only low intervals add evidence.

    An interval follows its preceding observation up to this timestamp, provided
    the sample gap is valid. A high MAP closes that low interval and starts a
    recovery candidate. A later MAP confirms recovery at >=30s, including a low
    observation which ends a normal interval of that length. Watermarks alone
    never supply low or recovery evidence.
    """
    timestamp = event_timestamp_ms(record["event"])
    value = record["event"]["valuenum"]
    if state is not None and timestamp < state["last_map_ms"]:
        raise ValueError("MAP records must be finalized in event-time order")
    if state is not None and timestamp - state["last_map_ms"] > config.map_max_sample_gap_seconds * 1000:
        state = None
    if state is not None and state["recovery_start_ms"] is not None:
        if timestamp - state["recovery_start_ms"] >= config.map_recovery_seconds * 1000:
            state = None  # Confirmed recovery: old low evidence and re-arm flag end here.
        else:
            state = dict(state, last_map_ms=timestamp,
                         recovery_start_ms=None if value < config.map_threshold else state["recovery_start_ms"])
    elif state is not None:
        state = dict(state, low_ms=state["low_ms"] + timestamp - state["last_map_ms"],
                     last_map_ms=timestamp,
                     recovery_start_ms=timestamp if value >= config.map_threshold else None)
    if state is None:
        if value >= config.map_threshold:
            return None, []
        state = {"start_ms": timestamp, "last_map_ms": timestamp, "low_ms": 0,
                 "recovery_start_ms": None, "alerted": False}
    results = []
    if (value < config.map_threshold and not state["alerted"]
            and state["low_ms"] >= config.map_sustained_seconds * 1000):
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
            observed = ValueStateDescriptor("map-observed", Types.STRING())
            emitted = MapStateDescriptor("map-emitted-episodes", Types.LONG(), Types.BOOLEAN())
            for descriptor in (pending, episode, observed, emitted):
                descriptor.enable_time_to_live(managed_ttl(ttl_hours))
            self.pending = runtime_context.get_map_state(pending)
            self.episode = runtime_context.get_state(episode)
            self.observed = runtime_context.get_state(observed)
            self.emitted = runtime_context.get_map_state(emitted)

        def process_element(self, value, ctx):
            record = json.loads(value)
            if record["event"]["signal_name"] != "MAP":
                return []  # Only MAP observations can recover/re-arm this rule.
            timestamp = event_timestamp_ms(record["event"])
            if timestamp <= ctx.timer_service().current_watermark():
                return []  # Defensive: upstream late classification normally rejects it.
            current = self.pending.get(timestamp)
            representative = select_timestamp_observation(
                [record] if current is None else [json.loads(current), record], config.map_threshold)
            self.pending.put(timestamp, encode(representative))
            ctx.timer_service().register_event_time_timer(timestamp)
            cached = self.observed.value()
            cached = None if cached is None else json.loads(cached)
            if cached is not None and timestamp > cached["last_timestamp_ms"]:
                state, results = advance_episode(cached["state"], representative, config)
                candidate = results[0] if results else None
                last_timestamp = timestamp
            else:
                # Only accepted OOO/equal-time updates replay the short pending
                # tail; the finalized prefix stays incremental in map-episode.
                current_episode = self.episode.value()
                ordered = sorted(self.pending.items())
                state, candidate = evaluate_pending(
                    None if current_episode is None else json.loads(current_episode),
                    [json.loads(value) for _, value in ordered], config)
                last_timestamp = ordered[-1][0]
            self.observed.update(encode({"last_timestamp_ms": last_timestamp, "state": state}))
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
        f"MAP <{config.map_threshold:g}: {config.map_sustained_seconds}s low evidence, "
        f"{config.map_recovery_seconds}s recovery").uid("v2-map-rule")
