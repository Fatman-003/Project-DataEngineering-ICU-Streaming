"""Fill docs/spark/EVALUATION_TEMPLATE.md with measured numbers -> docs/SPARK_VS_FLINK_EVALUATION.md.

    python bench/summarize.py && python bench/report.py

Placeholders:
  {{e1.<engine>.<telemetry|windows>.<precision|recall>}}        E1 medians
  {{e2.<engine>.<trigger>.<profile>.<kind>.<p50|p95|p99|max>}}   E2 latency (ms)
  {{e2.<engine>.<trigger>.<profile>.<cpu|mem>}}                 mean CPU % / peak MiB
  {{e2.spark.<trigger>.<profile>.<q_wide|q_window|batch_ms|batches>}}
  {{e3.<engine>.<eps|seconds|cpu|mem>}}  {{e4.<engine>.<recovery_s|duplicates|missing>}}
  {{ops.<engine>.startup_s}}  {{tables}} (the whole of bench/results/summary.md)
Write the team's analysis in the template; this file only substitutes numbers.
"""
import json
import re
import statistics
import sys
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
RESULTS = PROJECT / "bench/results"
TEMPLATE = PROJECT / "docs/spark/EVALUATION_TEMPLATE.md"
OUTPUT = PROJECT / "docs/SPARK_VS_FLINK_EVALUATION.md"


def number(value, digits=0):
    """Vietnamese style: "." groups thousands, "," marks decimals (1.340,5)."""
    if value is None:
        return "n/a"
    text = f"{value:,.{digits}f}" if digits else f"{round(value):,}"
    return text.replace(",", "\0").replace(".", ",").replace("\0", ".")


def median(item):
    return None if not item else item["median"]


def values(summary):
    out = {}
    for entry in summary.values():
        exp, engine, profile, trigger = entry["experiment"], entry["engine"], entry["profile"], entry["trigger"]
        if exp == "E1":
            for kind in ("telemetry", "windows"):
                for metric in ("precision", "recall"):
                    out[f"e1.{engine}.{kind}.{metric}"] = number(median(entry[metric][kind]), 4)
        elif exp == "E2":
            base = f"e2.{engine}.{trigger}.{profile}"
            for kind, stats in entry["latency"].items():
                for stat, item in stats.items():
                    name = {"window_q_wide": "q_wide", "window_q_window": "q_window"}.get(kind, kind)
                    out[f"{base}.{name}" if name in ("q_wide", "q_window") and stat == "p50" else f"{base}.{name}.{stat}"] \
                        = number(median(item))
            out[f"{base}.cpu"] = number(median(entry["cpu_mean_pct"]))
            out[f"{base}.mem"] = number(median(entry["mem_peak_mib"]))
            batches = entry.get("spark_batches") or {}
            out[f"{base}.batch_ms"] = number(median(batches.get("median_batch_ms")))
            out[f"{base}.batches"] = number(median(batches.get("batches")))
        elif exp == "E3":
            throughput = entry.get("throughput") or {}
            out[f"e3.{engine}.eps"] = number(median(throughput.get("events_per_second")))
            out[f"e3.{engine}.seconds"] = number(median(throughput.get("seconds_to_last_output")), 1)
            out[f"e3.{engine}.cpu"] = number(median(entry["cpu_mean_pct"]))
            out[f"e3.{engine}.mem"] = number(median(entry["mem_peak_mib"]))
        elif exp == "E4":
            recovery = entry.get("recovery") or {}
            out[f"e4.{engine}.recovery_s"] = number(median(recovery.get("first_output_after_restart_s")), 1)
            out[f"e4.{engine}.duplicates"] = number(median(entry["duplicates_total"]))
            out[f"e4.{engine}.missing"] = number((median(recovery.get("missing_telemetry")) or 0)
                                                 + (median(recovery.get("missing_windows")) or 0))
    startup = {}
    for path in RESULTS.glob("E2/*/run.json"):
        run = json.loads(path.read_text())
        timeline = run["timeline"]
        startup.setdefault(run["spec"]["engine"], []).append((timeline["engine_ready"] - timeline["topics_reset"]) / 1000)
    for engine, seconds in startup.items():
        out[f"ops.{engine}.startup_s"] = number(statistics.median(seconds), 1)
    return out


def main():
    summary = json.loads((RESULTS / "summary.json").read_text())
    table = values(summary)
    body = (RESULTS / "summary.md").read_text().split("\n", 3)[3]          # drop the title and intro line
    table["tables"] = re.sub(r"^(#+) ", lambda m: "#" + m.group(1) + " ", body, flags=re.M).strip()
    template = TEMPLATE.read_text()
    missing = sorted({key for key in re.findall(r"\{\{([^}]+)\}\}", template) if key not in table})
    rendered = re.sub(r"\{\{([^}]+)\}\}", lambda m: table.get(m.group(1), "n/a"), template)
    header = ("<!-- Tự sinh bởi `python bench/report.py` từ docs/spark/EVALUATION_TEMPLATE.md và "
              "bench/results/summary.json. Sửa template, đừng sửa file này. -->\n\n")
    OUTPUT.write_text(header + rendered)
    print(f"wrote {OUTPUT.relative_to(PROJECT)}; {len(table)} values" + (f"; MISSING: {missing}" if missing else ""))
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
