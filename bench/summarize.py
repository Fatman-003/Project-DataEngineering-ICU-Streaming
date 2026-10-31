"""Aggregate every finished run into bench/results/summary.json and summary.md.

    python bench/summarize.py

Each configuration reports the median over its repeats with the min-max range.
"""
import csv
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"
KINDS = ("telemetry", "window", "alert")


def load_runs():
    runs = []
    for path in sorted(RESULTS.glob("E*/*/run.json")):
        run = json.loads(path.read_text())
        run["dir"] = path.parent
        runs.append(run)
    return runs


def config_key(spec):
    trigger = spec["trigger"] + (f"+fetch{spec['fetch_wait_ms']}" if spec.get("fetch_wait_ms") else "")
    return spec["experiment"], spec["engine"], spec["profile"], trigger


def spread(values):
    values = [v for v in values if v is not None]
    if not values:
        return None
    return {"median": statistics.median(values), "min": min(values), "max": max(values), "n": len(values)}


def resources(run_dir, containers, since_ms=None, until_ms=None):
    """Mean summed CPU % and peak summed memory over the engine's containers."""
    path = run_dir / "resources.csv"
    if not path.exists():
        return None, None
    by_time = defaultdict(lambda: [0.0, 0])
    with open(path) as handle:
        for row in csv.DictReader(handle):
            ts = int(row["ts_ms"]) // 1000
            if row["container"] in containers and (since_ms is None or ts * 1000 >= since_ms) \
                    and (until_ms is None or ts * 1000 <= until_ms):
                by_time[(ts, row["container"])] = [float(row["cpu_pct"]), int(row["mem_bytes"] or 0)]
    seconds = defaultdict(lambda: [0.0, 0])
    for (ts, _), (cpu, mem) in by_time.items():
        seconds[ts][0] += cpu
        seconds[ts][1] += mem
    if not seconds:
        return None, None
    return statistics.fmean(v[0] for v in seconds.values()), max(v[1] for v in seconds.values())


def spark_batches(run_dir):
    path = run_dir / "spark_metrics" / "wide.jsonl"
    if not path.exists():
        return None
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    data = [r for r in rows if r.get("event") == "progress" and r.get("numInputRows")]
    if not data:
        return None
    durations = [r["durationMs"].get("triggerExecution", 0) for r in data]
    return {"batches": len(data), "median_batch_ms": statistics.median(durations), "max_batch_ms": max(durations),
            "max_processed_rows_per_s": max(r.get("processedRowsPerSecond") or 0 for r in data)}


def producer_rate(run_dir):
    path = run_dir / "producer.log"
    if not path.exists():
        return None
    text = path.read_text()
    if "HIGH-FREQUENCY RESULT" in text:
        result = json.loads(text.split("HIGH-FREQUENCY RESULT", 1)[1])
        return {"achievement_pct": result["rate_achievement_percent"],
                "max_scheduling_lag_ms": result["max_scheduling_lag_ms"]}
    return None


def output_times(run_dir, engine):
    import pandas as pd
    outputs = pd.read_parquet(run_dir / "outputs.parquet", columns=["engine", "kind", "append_ts"])
    return outputs[outputs.engine == engine]


def engine_containers(engine):
    return {"flink": ["icu-flink-jobmanager", "icu-flink-taskmanager"], "spark": ["icu-spark"]}[engine]


def per_run(run):
    spec, timeline, run_dir = run["spec"], run["timeline"], run["dir"]
    engines = ["flink", "spark"] if spec["engine"] == "both" else [spec["engine"]]
    row = {"run": f"{run_dir.parent.name}/{run_dir.name}", "drained": timeline.get("drain", {}).get("drained"),
           "engines": {}}
    for engine in engines:
        correctness = run["correctness"]["engines"].get(engine, {})
        latency = run["latency"].get(engine, {})
        cpu, mem = resources(run_dir, engine_containers(engine), timeline.get("engine_ready"),
                             timeline.get("drained"))
        result = {
            "precision_recall": {kind: (correctness.get(kind, {}).get("precision"), correctness.get(kind, {}).get("recall"))
                                 for kind in ("telemetry", "windows")},
            "alerts_exact": (correctness.get("alerts", {}).get("exact_match"), correctness.get("alerts", {}).get("reference")),
            "dlq": correctness.get("dlq"), "duplicates": correctness.get("duplicates"),
            "dropped_by_watermark": correctness.get("dropped_by_watermark"),
            "latency": {kind: latency[kind] for kind in latency},
            "cpu_mean_pct": cpu, "mem_peak_bytes": mem,
        }
        if engine == "spark":
            result["batches"] = spark_batches(run_dir)
        times = output_times(run_dir, engine)
        if spec["experiment"] == "E3" and len(times):
            raw_chart = run["collect"]["raw_records"]
            elapsed = (times.append_ts.max() - timeline["engine_ready"]) / 1000
            result["throughput"] = {"raw_records": raw_chart, "seconds_to_last_output": elapsed,
                                    "events_per_second": raw_chart / elapsed if elapsed > 0 else None}
        if spec["experiment"] == "E4" and "restarted" in timeline:
            after = times[times.append_ts > timeline["restarted"]]
            result["recovery"] = {
                "first_output_after_restart_s": (after.append_ts.min() - timeline["restarted"]) / 1000 if len(after) else None,
                "missing_telemetry": correctness.get("telemetry", {}).get("reference", 0)
                - correctness.get("telemetry", {}).get("matched", 0),
                "missing_windows": correctness.get("windows", {}).get("reference", 0)
                - correctness.get("windows", {}).get("matched", 0)}
        row["engines"][engine] = result
    row["producer"] = producer_rate(run_dir)
    return row


def aggregate(rows_by_config):
    summary = {}
    for (experiment, engine_spec, profile, trigger), rows in sorted(rows_by_config.items()):
        for engine in (["flink", "spark"] if engine_spec == "both" else [engine_spec]):
            items = [r["engines"][engine] for r in rows if engine in r["engines"]]
            entry = {"experiment": experiment, "engine": engine, "profile": profile, "trigger": trigger,
                     "runs": [r["run"] for r in rows], "all_drained": all(r["drained"] for r in rows),
                     "latency": {}, "cpu_mean_pct": spread([i["cpu_mean_pct"] for i in items]),
                     "mem_peak_mib": spread([i["mem_peak_bytes"] / 2 ** 20 if i["mem_peak_bytes"] else None for i in items]),
                     "precision": {k: spread([i["precision_recall"][k][0] for i in items]) for k in ("telemetry", "windows")},
                     "recall": {k: spread([i["precision_recall"][k][1] for i in items]) for k in ("telemetry", "windows")},
                     "alerts_exact": [i["alerts_exact"] for i in items],
                     "duplicates_total": spread([sum((i["duplicates"] or {}).values()) for i in items]),
                     "dlq": [i["dlq"] for i in items], "dropped_by_watermark": [i.get("dropped_by_watermark") for i in items],
                     "producer": [r["producer"] for r in rows]}
            for kind in (*KINDS, "window_q_wide", "window_q_window"):
                stats = [i["latency"].get(kind) for i in items if i["latency"].get(kind)]
                if stats:
                    entry["latency"][kind] = {p: spread([s[p] for s in stats]) for p in ("p50", "p95", "p99", "max")}
            if any("batches" in i and i["batches"] for i in items):
                entry["spark_batches"] = {k: spread([i["batches"][k] for i in items if i.get("batches")])
                                          for k in ("batches", "median_batch_ms", "max_batch_ms", "max_processed_rows_per_s")}
            if any("throughput" in i for i in items):
                entry["throughput"] = {k: spread([i["throughput"][k] for i in items if "throughput" in i])
                                       for k in ("seconds_to_last_output", "events_per_second")}
            if any("recovery" in i for i in items):
                entry["recovery"] = {k: spread([i["recovery"][k] for i in items if "recovery" in i])
                                     for k in ("first_output_after_restart_s", "missing_telemetry", "missing_windows")}
            summary[f"{experiment}|{engine}|{profile}|{trigger}"] = entry
    return summary


def fmt(item, scale=1.0, digits=0):
    if not item:
        return "-"
    value = lambda v: f"{v * scale:,.{digits}f}"
    if item["n"] == 1 or item["min"] == item["max"]:
        return value(item["median"])
    return f"{value(item['median'])} ({value(item['min'])}–{value(item['max'])})"


def markdown(summary):
    lines = ["# Kết quả benchmark Spark vs Flink", "",
             "Tự sinh bởi `python bench/summarize.py`. Mỗi ô là **trung vị (min–max)** qua các lần lặp; "
             "latency tính bằng ms theo §10.1 của plan.", ""]
    entries = list(summary.values())
    e1 = [e for e in entries if e["experiment"] == "E1"]
    if e1:
        lines += ["## E1 — Correctness (kịch bản chuẩn 6 phút, hai engine chạy song song)", "",
                  "| Engine | Lần chạy | Precision / recall telemetry | Precision / recall cửa sổ | Alert khớp | DLQ (chartevents) | Bản ghi trùng |",
                  "|---|---|---|---|---|---|---|"]
        for e in e1:
            lines.append(f"| {e['engine']} | {len(e['runs'])} | {fmt(e['precision']['telemetry'], digits=4)} / "
                         f"{fmt(e['recall']['telemetry'], digits=4)} | {fmt(e['precision']['windows'], digits=4)} / "
                         f"{fmt(e['recall']['windows'], digits=4)} | "
                         f"{', '.join(f'{a}/{b}' for a, b in e['alerts_exact'])} | {e['dlq'][0]} | "
                         f"{fmt(e['duplicates_total'])} |")
        lines.append("")
    e2 = [e for e in entries if e["experiment"] == "E2"]
    if e2:
        lines += ["## E2 — Latency theo tải (ms)", "",
                  "| Tải | Engine | Trigger | Telemetry p50 | Telemetry p95 | Cửa sổ p50 | Cửa sổ p95 | Alert p50 | Alert max | CPU TB (%) | RAM đỉnh (MiB) | Drain |",
                  "|---|---|---|---|---|---|---|---|---|---|---|---|"]
        order = {"normal": 0, "hf100": 1, "hf200": 2}
        for e in sorted(e2, key=lambda e: (order.get(e["profile"], 9), e["engine"], e["trigger"])):
            lat = e["latency"]
            lines.append(f"| {e['profile']} | {e['engine']} | {e['trigger']} | {fmt(lat.get('telemetry', {}).get('p50'))} | "
                         f"{fmt(lat.get('telemetry', {}).get('p95'))} | {fmt(lat.get('window', {}).get('p50'))} | "
                         f"{fmt(lat.get('window', {}).get('p95'))} | {fmt(lat.get('alert', {}).get('p50'))} | "
                         f"{fmt(lat.get('alert', {}).get('max'))} | {fmt(e['cpu_mean_pct'])} | {fmt(e['mem_peak_mib'])} | "
                         f"{'ok' if e['all_drained'] else 'KHÔNG'} |")
        lines += ["", "### E2 — Tách latency cửa sổ của Spark (Q_wide → topic → Q_window), p50 ms", "",
                  "| Tải | Trigger | Phần Q_wide | Phần Q_window | Batch Q_wide trung vị (ms) | Số batch |", "|---|---|---|---|---|---|"]
        for e in sorted((e for e in e2 if e["engine"] == "spark"), key=lambda e: (order.get(e["profile"], 9), e["trigger"])):
            lat, batches = e["latency"], e.get("spark_batches", {})
            lines.append(f"| {e['profile']} | {e['trigger']} | {fmt(lat.get('window_q_wide', {}).get('p50'))} | "
                         f"{fmt(lat.get('window_q_window', {}).get('p50'))} | {fmt(batches.get('median_batch_ms'))} | "
                         f"{fmt(batches.get('batches'))} |")
        lines.append("")
    e3 = [e for e in entries if e["experiment"] == "E3"]
    if e3:
        lines += ["## E3 — Throughput tối đa (nạp sẵn HF200 × 10 phút)", "",
                  "| Engine | Thời gian tới output cuối (s) | Event/giây | CPU TB (%) | RAM đỉnh (MiB) | Drain |",
                  "|---|---|---|---|---|---|"]
        for e in e3:
            t = e.get("throughput", {})
            lines.append(f"| {e['engine']} | {fmt(t.get('seconds_to_last_output'), digits=1)} | "
                         f"{fmt(t.get('events_per_second'))} | {fmt(e['cpu_mean_pct'])} | {fmt(e['mem_peak_mib'])} | "
                         f"{'ok' if e['all_drained'] else 'KHÔNG'} |")
        lines.append("")
    e4 = [e for e in entries if e["experiment"] == "E4"]
    if e4:
        lines += ["## E4 — Phục hồi lỗi (HF100, kill engine ở giây 150)", "",
                  "| Engine | Output đầu tiên sau restart (s) | Bản ghi trùng | Telemetry thiếu | Cửa sổ thiếu | Recall telemetry |",
                  "|---|---|---|---|---|---|"]
        for e in e4:
            r = e.get("recovery", {})
            lines.append(f"| {e['engine']} | {fmt(r.get('first_output_after_restart_s'), digits=1)} | "
                         f"{fmt(e['duplicates_total'])} | {fmt(r.get('missing_telemetry'))} | "
                         f"{fmt(r.get('missing_windows'))} | {fmt(e['recall']['telemetry'], digits=4)} |")
        lines.append("")
    return "\n".join(lines)


def main():
    rows_by_config = defaultdict(list)
    for run in load_runs():
        rows_by_config[config_key(run["spec"])].append(per_run(run))
    summary = aggregate(rows_by_config)
    (RESULTS / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
    (RESULTS / "summary.md").write_text(markdown(summary))
    print(f"{len(summary)} configurations from {sum(len(v) for v in rows_by_config.values())} runs")


if __name__ == "__main__":
    sys.exit(main())
