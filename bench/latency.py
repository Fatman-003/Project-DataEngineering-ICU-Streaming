"""Engine-independent end-to-end latency from broker LogAppendTime (plan §10.1).

    python bench/latency.py --run bench/results/<experiment>/<run>

latency = output append_ts - T_ready, with T_ready the earliest moment an ideal
engine could emit the output:
  telemetry (4 fields)  max append_ts of its 4 component RAW events (first copies)
  window ending at E     append_ts of the first RAW chartevent with event_time >= E + 2 s
  alert at A             append_ts of the first RAW chartevent with event_time >= A + 3 s
RAW events count only if valid and first-seen (what advances a watermark).
Spark windows are also split at the first Spark telemetry with sample_time >= E + 2 s
(the record that advances Q_window's watermark): q_wide_part + q_window_part.
Writes latency.csv and latency_summary.json.
"""
import argparse
import base64
import bisect
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs/flink"))
from business_contract import FIELDS            # noqa: E402
from event_time import event_timestamp_ms       # noqa: E402
from runtime_pipeline import parse_and_validate  # noqa: E402

PERCENTILES = (50, 95, 99)


def ms(text):
    return event_timestamp_ms({"event_time": text})


class FirstAtOrAfter:
    """append_ts of the first record (in offset order) whose running-max time reaches a threshold."""

    def __init__(self, times_and_appends):
        self.running, self.appends, maximum = [], [], None
        for event_ms, append_ts in times_and_appends:
            if maximum is None or event_ms > maximum:
                maximum = event_ms
                self.running.append(maximum)
                self.appends.append(append_ts)

    def __call__(self, threshold):
        index = bisect.bisect_left(self.running, threshold)
        return None if index == len(self.running) else self.appends[index]


def raw_index(raw):
    """Valid, first-seen chartevents in offset order: (event, append_ts)."""
    seen, accepted = set(), []
    for row in sorted((r for r in raw if r["topic"] == "icu.v2.chartevents"), key=lambda r: r["offset"]):
        source = {"topic": row["topic"], "partition": 0, "offset": int(row["offset"]), "headers": {},
                  "key_base64": None, "payload_base64": base64.b64encode(row["payload"]).decode()}
        record, failed = parse_and_validate(source, "chartevents")
        if failed is None and record["event"]["event_id"] not in seen:
            seen.add(record["event"]["event_id"])
            accepted.append((record["event"], int(row["append_ts"])))
    return accepted


def percentile(sorted_values, p):
    if not sorted_values:
        return None
    rank = (len(sorted_values) - 1) * p / 100
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (rank - low)


def summarize(values):
    values = sorted(values)
    return {"count": len(values), **{f"p{p}": percentile(values, p) for p in PERCENTILES},
            "max": values[-1] if values else None, "mean": sum(values) / len(values) if values else None,
            "negative": sum(v < 0 for v in values)}


def first_outputs(outputs, engine, kind):
    """First copy (lowest offset) per message_id: (message, append_ts)."""
    first = {}
    for row in sorted((r for r in outputs if r["engine"] == engine and r["kind"] == kind), key=lambda r: r["offset"]):
        message = json.loads(row["payload"])
        first.setdefault(message["message_id"], (message, int(row["append_ts"])))
    return list(first.values())


def compute(raw, outputs):
    accepted = raw_index(raw)
    watermark_ready = FirstAtOrAfter((event_timestamp_ms(e), t) for e, t in accepted)
    components = {}
    for event, append_ts in accepted:
        field = FIELDS.get(event["signal_name"])
        if field:
            components.setdefault((event["stay_id"], event_timestamp_ms(event)), {}).setdefault(field, append_ts)
    rows, summary = [], {}
    for engine in sorted({r["engine"] for r in outputs}):
        lat = {"telemetry": [], "window": [], "alert": [], "window_q_wide": [], "window_q_window": []}
        for message, out_ts in first_outputs(outputs, engine, "telemetry"):
            parts = components.get((message["stay_id"], ms(message["sample_time"])), {})
            if len(parts) == len(FIELDS):
                lat["telemetry"].append(out_ts - max(parts.values()))
                rows.append((engine, "telemetry", message["message_id"], max(parts.values()), out_ts))
        # Spark's telemetry topic in offset order is exactly Q_window's input order.
        spark_wide = (FirstAtOrAfter((ms(m["sample_time"]), t) for m, t in first_outputs(outputs, engine, "telemetry"))
                      if engine == "spark" else None)
        for message, out_ts in first_outputs(outputs, engine, "aggregates"):
            end = ms(message["window_end"])
            ready = watermark_ready(end + 2000)
            if ready is None:
                continue
            lat["window"].append(out_ts - ready)
            rows.append((engine, "window", message["message_id"], ready, out_ts))
            if spark_wide is not None and (wide_ready := spark_wide(end + 2000)) is not None:
                lat["window_q_wide"].append(wide_ready - ready)
                lat["window_q_window"].append(out_ts - wide_ready)
        for message, out_ts in first_outputs(outputs, engine, "alerts"):
            ready = watermark_ready(ms(message["alert_time"]) + 3000)
            if ready is not None:
                lat["alert"].append(out_ts - ready)
                rows.append((engine, "alert", message["message_id"], ready, out_ts))
        summary[engine] = {kind: summarize(values) for kind, values in lat.items() if values}
    return rows, summary


def evaluate(run_dir):
    import pandas as pd
    run_dir = Path(run_dir)
    raw = pd.read_parquet(run_dir / "raw.parquet").to_dict("records")
    outputs = pd.read_parquet(run_dir / "outputs.parquet").to_dict("records")
    rows, summary = compute(raw, outputs)
    pd.DataFrame(rows, columns=["engine", "kind", "message_id", "t_ready_ms", "t_out_ms"]).assign(
        latency_ms=lambda d: d.t_out_ms - d.t_ready_ms).to_csv(run_dir / "latency.csv", index=False)
    (run_dir / "latency_summary.json").write_text(json.dumps(summary, indent=2))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--run", required=True)
    print(json.dumps(evaluate(parser.parse_args().run), indent=2))


if __name__ == "__main__":
    main()
