"""Engine-independent expected outputs, computed offline from the collected RAW records.

Plain loops in offset order; no watermark machinery. Shared helpers are used only
for contract formatting/identity (`telemetry`, `metadata`) and for the documented
alert rule (`bucket_add`, `median_map`, `advance_episode`), as the plan requires.

Closability (plan §9.2): an item counts only once some RAW chartevent has
`event_time >= threshold`, the point after which a watermark with 2 s tolerance
must have passed it:
  incomplete sample at T  -> T + 2001 ms   (Flink timer at T, watermark = max - 2001)
  window ending at E      -> E + 2000 ms
  MAP bucket starting at S -> S + 3000 ms  (timer at S + 999)
Arrival-dependent events (event_time <= max(previous accepted) - 2000 ms) may be
LATE in either engine, so they are excluded and counted separately.
"""
import base64
import json
import statistics
import sys
from collections import Counter, defaultdict
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT / "jobs/flink"))
from business_contract import FIELDS, metadata                     # noqa: E402
from business_outputs import telemetry                             # noqa: E402
from event_time import event_timestamp_ms                          # noqa: E402
from hypotension import CONTEXT_SIGNALS, advance_episode, bucket_add, median_map  # noqa: E402
from runtime_config import WindowAlertConfig                       # noqa: E402
from runtime_pipeline import parse_and_validate                    # noqa: E402
from window_aggregates import iso_timestamp                        # noqa: E402

TOLERANCE_MS = 2000
WINDOW_MS, SLIDE_MS, BUCKET_MS = 300_000, 60_000, 1000


def source(row):
    return {"topic": row["topic"], "partition": int(row["partition"]), "offset": int(row["offset"]), "headers": {},
            "key_base64": None if row["key"] is None else base64.b64encode(row["key"]).decode(),
            "payload_base64": None if row["payload"] is None else base64.b64encode(row["payload"]).decode()}


def classify(chart_rows):
    """Validation, first-seen dedup and arrival-dependent split, in offset order."""
    dlq, accepted, engine_dependent, seen, maximum = Counter(), [], [], set(), None
    for row in sorted(chart_rows, key=lambda r: (r["partition"], r["offset"])):
        record, failed = parse_and_validate(source(row), "chartevents")
        if failed is not None:
            dlq[failed["error_message"].split(":", 1)[0]] += 1
            continue
        event = record["event"]
        if event["event_id"] in seen:
            dlq["DUPLICATE_EVENT"] += 1
            continue
        seen.add(event["event_id"])
        timestamp = event_timestamp_ms(event)
        if maximum is not None and timestamp <= maximum - TOLERANCE_MS:
            engine_dependent.append(event)
            continue
        maximum = timestamp if maximum is None else max(maximum, timestamp)
        accepted.append((timestamp, event))
    return dlq, accepted, engine_dependent, maximum


def wide_samples(accepted, max_ts, dlq):
    samples = {}
    for timestamp, event in accepted:
        field = FIELDS.get(event["signal_name"])
        if field is None:
            continue
        sample = samples.setdefault((event["stay_id"], timestamp), {"event": event, "values": {}, "frozen": False})
        if sample["frozen"]:
            if sample["values"][field] != event["valuenum"]:
                dlq["IDEMPOTENCY_CONFLICT"] += 1
            continue
        sample["values"][field] = min(sample["values"].get(field, event["valuenum"]), event["valuenum"])
        sample["frozen"] = len(sample["values"]) == len(FIELDS)
    closed, tail = {}, {}
    for (stay, timestamp), sample in samples.items():
        message = telemetry(sample["event"], sample["values"])
        target = closed if sample["frozen"] or max_ts >= timestamp + TOLERANCE_MS + 1 else tail
        target[(stay, message["sample_time"])] = dict(message, sample_ts=timestamp)
    return closed, tail


def windows(samples, max_ts):
    values = defaultdict(lambda: defaultdict(list))
    for (stay, _), sample in samples.items():
        first_end = sample["sample_ts"] // SLIDE_MS * SLIDE_MS + SLIDE_MS
        for end in range(first_end, first_end + WINDOW_MS, SLIDE_MS):
            for field in FIELDS.values():
                if sample[field] is not None:
                    values[(stay, end)][field].append(sample[field])
    closed, tail = {}, {}
    for (stay, end), data in values.items():
        fields = {field: data.get(field, []) for field in FIELDS.values()}
        if not any(fields.values()):
            continue
        message = dict(metadata("VITALS_WINDOW_5M", [stay, end]), window_end=iso_timestamp(end), stay_id=stay,
                       **{"avg_" + f: statistics.fmean(v) if v else None for f, v in fields.items()},
                       **{"var_" + f: statistics.pvariance(v) if len(v) >= 2 else None for f, v in fields.items()})
        (closed if max_ts >= end + TOLERANCE_MS else tail)[(stay, message["window_end"])] = message
    return closed, tail


def alerts(accepted, max_ts, config=None):
    config = config or WindowAlertConfig()
    buckets = defaultdict(dict)
    for timestamp, event in accepted:
        if event["signal_name"] not in ("MAP", *CONTEXT_SIGNALS):
            continue
        start = timestamp // BUCKET_MS * BUCKET_MS
        bucket = buckets[event["stay_id"]].setdefault(start, {"map": None, "context": {}})
        if event["signal_name"] == "MAP":
            if bucket["map"] is None:
                bucket["map"] = {"samples": {}, "event": event, "first": timestamp, "last": timestamp}
            bucket["map"]["first"] = min(bucket["map"]["first"], timestamp)
            bucket["map"]["last"] = max(bucket["map"]["last"], timestamp)
            bucket_add(bucket["map"]["samples"], event)
        else:
            bucket_add(bucket["context"].setdefault(event["signal_name"], {}), event)
    closed, pending = [], 0
    for stay, by_start in buckets.items():
        state = None
        for start in sorted(by_start):
            bucket = by_start[start]
            if bucket["map"] is None:
                continue
            if max_ts < start + BUCKET_MS - 1 + TOLERANCE_MS + 1:
                pending += 1
                continue
            event = dict(bucket["map"]["event"], event_time=iso_timestamp(start),
                         valuenum=median_map(bucket["map"]["samples"]))
            state, outputs = advance_episode(state, {
                "event": event, "first_observation_ms": bucket["map"]["first"],
                "last_observation_ms": bucket["map"]["last"],
                "context": {signal: median_map(samples) for signal, samples in bucket["context"].items()}}, config)
            closed.extend(outputs)
    return closed, pending


def build_reference(raw_rows):
    chart = [row for row in raw_rows if row["topic"] == "icu.v2.chartevents"]
    dlq, accepted, engine_dependent, max_ts = classify(chart)
    if max_ts is None:
        return {"telemetry": {}, "telemetry_tail": {}, "windows": {}, "windows_tail": {}, "alerts": [],
                "alert_buckets_tail": 0, "dlq": dict(dlq), "engine_dependent": [], "max_event_ts": None}
    samples, samples_tail = wide_samples(accepted, max_ts, dlq)
    window_closed, window_tail = windows({**samples, **samples_tail}, max_ts)
    alert_list, alert_tail = alerts(accepted, max_ts)
    return {"telemetry": samples, "telemetry_tail": samples_tail, "windows": window_closed,
            "windows_tail": window_tail, "alerts": alert_list, "alert_buckets_tail": alert_tail,
            "dlq": dict(dlq), "engine_dependent": [e["event_id"] for e in engine_dependent],
            "max_event_ts": iso_timestamp(max_ts)}
