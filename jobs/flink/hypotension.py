"""Finalized 1s MAP decisions; 30s low evidence, five positive buckets in 10s.

A deterministic bottom-k sample retains at most 256 actual values per second.
Normal/HF100/HF200 retain every MAP value and compute the exact median.
Each accepted observation contributes once after upstream validation/dedup/time
classification. Only the <=2s watermark tail is retained, not the episode history.
"""
import json
import hashlib
from statistics import median
from business_contract import metadata, stable_uuid, encode
from event_time import event_timestamp_ms
from window_aggregates import iso_timestamp
from runtime_config import MAP_BUCKET_SECONDS, MAP_BUCKET_MAX_SAMPLES

ALERT_TYPE = "HYPOTENSION_SUSTAINED"


def alert_identity(stay_id, episode_start_ms):
    return stable_uuid(stay_id, ALERT_TYPE, episode_start_ms)


def bucket_add(samples, event):
    token = hashlib.sha256(event["event_id"].encode()).hexdigest()
    if token in samples or len(samples) < MAP_BUCKET_MAX_SAMPLES:
        samples[token] = event["valuenum"]
    else:
        largest = max(samples)
        if token < largest:
            del samples[largest]
            samples[token] = event["valuenum"]
    return samples


def median_map(samples):
    return median(samples.values())


def alert_candidate(state, record, config):
    event = record["event"]
    return dict(metadata("SEPSIS_ALERT_CREATED", [event["stay_id"], state["start_ms"]],
                         producer="icu-alert-engine"),
                alert_id=alert_identity(event["stay_id"], state["start_ms"]),
                alert_time=event["event_time"], stay_id=event["stay_id"],
                alert_type=ALERT_TYPE, severity="CRITICAL", trigger_value=event["valuenum"],
                trigger_message=f"MAP < {config.map_threshold:g} mmHg: >= {config.map_sustained_seconds}s accumulated low evidence",
                status="ACTIVE")


def advance_episode(state, record, config):
    """Advance exactly one finalized second. Rolling interval is (t-10s,t]."""
    event = record["event"]
    timestamp, low = event_timestamp_ms(event), event["valuenum"] < config.map_threshold
    first_observation = record.get("first_observation_ms", timestamp)
    last_observation = record.get("last_observation_ms", timestamp)
    if state is not None:
        if timestamp <= state["last_map_ms"]:
            raise ValueError("Finalized MAP buckets must be strictly ordered")
        if (timestamp - state["last_map_ms"] > config.map_max_sample_gap_seconds * 1000
                or first_observation - state["last_observation_ms"] > config.map_max_sample_gap_seconds * 1000):
            state = None
    if state is None:
        if not low:
            return None, []
        state = dict(start_ms=timestamp, last_map_ms=timestamp, low_ms=0, last_low=True,
                     positives=[], alerted=False, last_observation_ms=last_observation)
    else:
        state = dict(state)
        if state["last_low"]:
            state["low_ms"] += timestamp - state["last_map_ms"]
        state.update(last_map_ms=timestamp, last_low=low, last_observation_ms=last_observation)
        positives = [t for t in state["positives"] if timestamp - t < config.map_recovery_window_seconds * 1000]
        if not low:
            positives.append(timestamp)
        state["positives"] = positives
        if len(positives) >= config.map_recovery_observations:
            return None, []
    if low and not state["alerted"] and state["low_ms"] >= config.map_sustained_seconds * 1000:
        result = alert_candidate(state, record, config)
        state["alerted"] = True
        return state, [result]
    return state, []


def flink_hypotension(config, ttl_hours):
    from pyflink.common import Types
    from pyflink.datastream import KeyedProcessFunction
    from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
    from dedup import managed_ttl

    class Hypotension(KeyedProcessFunction):
        def open(self, runtime_context):
            pending = MapStateDescriptor("map-seconds", Types.LONG(), Types.STRING())
            episode = ValueStateDescriptor("map-episode-v3", Types.STRING())
            for descriptor in (pending, episode):
                descriptor.enable_time_to_live(managed_ttl(ttl_hours))
            self.pending = runtime_context.get_map_state(pending)
            self.episode = runtime_context.get_state(episode)

        def process_element(self, value, ctx):
            record = json.loads(value)
            if record["event"]["signal_name"] != "MAP":
                return []
            timestamp = event_timestamp_ms(record["event"])
            if timestamp <= ctx.timer_service().current_watermark():
                return []
            start = timestamp // (MAP_BUCKET_SECONDS * 1000) * (MAP_BUCKET_SECONDS * 1000)
            stored = self.pending.get(start)
            bucket = {"samples": {}, "event": record["event"], "first_ms": timestamp, "last_ms": timestamp} if stored is None else json.loads(stored)
            bucket["first_ms"] = min(bucket["first_ms"], timestamp)
            bucket["last_ms"] = max(bucket["last_ms"], timestamp)
            bucket_add(bucket["samples"], record["event"])
            self.pending.put(start, encode(bucket))
            ctx.timer_service().register_event_time_timer(start + MAP_BUCKET_SECONDS * 1000 - 1)
            return []

        def on_timer(self, timestamp, ctx):
            start = timestamp - MAP_BUCKET_SECONDS * 1000 + 1
            stored = self.pending.get(start)
            if stored is None:
                return []
            self.pending.remove(start)
            bucket = json.loads(stored)
            event = dict(bucket["event"], event_time=iso_timestamp(start),
                         valuenum=median_map(bucket["samples"]))
            state = self.episode.value()
            state, outputs = advance_episode(None if state is None else json.loads(state),
                                             {"event": event, "first_observation_ms": bucket["first_ms"],
                                              "last_observation_ms": bucket["last_ms"]}, config)
            if state is None:
                self.episode.clear()
            else:
                self.episode.update(encode(state))
            return [encode(result) for result in outputs]

    return Hypotension()


def attach_hypotension(clean, config, ttl_hours):
    from pyflink.common import Types
    return clean.filter(lambda value: json.loads(value)["event"]["signal_name"] == "MAP").key_by(
        lambda value: str(json.loads(value)["event"]["stay_id"]), key_type=Types.STRING()).process(
        flink_hypotension(config, ttl_hours), output_type=Types.STRING()).name(
        "MAP 1s median: 30s low, recovery 5/10s").uid("business-map-rule")
