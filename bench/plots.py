"""Benchmark figures (PNG) into bench/results/plots/.

    python bench/plots.py

Colors follow a fixed categorical order (one hue per engine configuration,
never re-assigned by rank): Flink, Spark default trigger, Spark 1 s, Spark 5 s,
Spark with the 500 ms Kafka fetch wait. CPU and memory are separate charts
(never a dual axis).
"""
import csv
import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

RESULTS = Path(__file__).resolve().parent / "results"
PLOTS = RESULTS / "plots"
SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
SERIES = {"flink": "#2a78d6", "spark default": "#eb6834", "spark 1s": "#1baf7a", "spark 5s": "#eda100",
          "spark default+fetch500": "#e87ba4"}
LABELS = {"flink": "Flink", "spark default": "Spark (trigger mặc định)", "spark 1s": "Spark (trigger 1 s)",
          "spark 5s": "Spark (trigger 5 s)", "spark default+fetch500": "Spark (fetch.max.wait 500 ms)"}
PROFILES = ("normal", "speed5", "speed10")
PROFILE_LABELS = {"normal": "Normal (24 msg/s)", "speed5": "×5 (120 msg/s)", "speed10": "×10 (240 msg/s)",
                  "hf100": "HF100 (2.400 msg/s, quá tải Flink)"}
KIND_LABELS = {"telemetry": "Telemetry", "window": "Cửa sổ 5 phút", "alert": "Alert"}

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "xtick.color": INK_2, "ytick.color": INK_2,
    "text.color": INK, "axes.titlecolor": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "lines.linewidth": 2, "font.size": 10,
    "legend.frameon": False, "legend.labelcolor": INK_2,
})


def runs(experiment):
    for path in sorted(RESULTS.glob(f"{experiment}/*/run.json")):
        run = json.loads(path.read_text())
        spec = run["spec"]
        trigger = spec["trigger"] + (f"+fetch{spec['fetch_wait_ms']}" if spec.get("fetch_wait_ms") else "")
        yield run, path.parent, spec["engine"] if spec["engine"] != "spark" else f"spark {trigger}"


def latency_rows(run_dir, engine):
    path = run_dir / "latency.csv"
    values = defaultdict(list)
    if path.exists():
        with open(path) as handle:
            for row in csv.DictReader(handle):
                if row["engine"] == engine:
                    values[row["kind"]].append(float(row["latency_ms"]))
    return values


def latency_cdf():
    pooled = defaultdict(lambda: defaultdict(list))     # (profile, kind) -> config -> values
    for run, run_dir, config in runs("E2"):
        engine = run["spec"]["engine"]
        for kind, values in latency_rows(run_dir, engine).items():
            pooled[(run["spec"]["profile"], kind)][config].extend(values)
    for kind in KIND_LABELS:
        figure, axes = plt.subplots(1, 3, figsize=(13, 3.8), sharey=True)
        drawn = False
        for axis, profile in zip(axes, PROFILES):
            for config, color in SERIES.items():
                values = sorted(pooled[(profile, kind)].get(config, []))
                if values:
                    drawn = True
                    axis.plot(values, [(i + 1) / len(values) for i in range(len(values))], color=color,
                              label=LABELS[config], drawstyle="steps-post")
            axis.set_xscale("log")
            axis.set_title(PROFILE_LABELS[profile], fontsize=10)
            axis.set_xlabel("Latency (ms, thang log)")
        axes[0].set_ylabel("Tỷ lệ tích lũy")
        if drawn:
            handles, labels = axes[0].get_legend_handles_labels()
            figure.legend(handles, labels, loc="upper center", ncol=5, bbox_to_anchor=(0.5, 1.02))
            figure.suptitle(f"E2 — CDF latency {KIND_LABELS[kind].lower()} (gộp 3 lần lặp)", y=1.1, fontsize=12)
            figure.savefig(PLOTS / f"e2_latency_cdf_{kind}.png", dpi=150, bbox_inches="tight")
        plt.close(figure)


def p95_by_load():
    per_run = defaultdict(lambda: defaultdict(list))    # kind -> (profile, config) -> [p95 per repeat]
    for run, _, config in runs("E2"):
        stats = run["latency"].get(run["spec"]["engine"], {})
        for kind in KIND_LABELS:
            if kind in stats:
                per_run[kind][(run["spec"]["profile"], config)].append(stats[kind]["p95"])
    configs = [c for c in SERIES if c != "spark default+fetch500"]
    for kind, data in per_run.items():
        figure, axis = plt.subplots(figsize=(10, 4))
        width = 0.8 / len(configs)
        for index, config in enumerate(configs):
            positions = [p + (index - (len(configs) - 1) / 2) * width for p in range(len(PROFILES))]
            series = [data.get((profile, config), []) for profile in PROFILES]
            present = [(pos, values) for pos, values in zip(positions, series) if values]
            if not present:
                continue
            box = axis.boxplot([v for _, v in present], positions=[p for p, _ in present], widths=width * 0.8,
                               patch_artist=True, showfliers=False, medianprops={"color": SURFACE, "linewidth": 2})
            for patch in box["boxes"]:
                patch.set(facecolor=SERIES[config], edgecolor=SERIES[config])
            for element in ("whiskers", "caps"):
                for line in box[element]:
                    line.set(color=SERIES[config])
            axis.scatter([p for p, v in present for _ in v], [x for _, v in present for x in v], s=14,
                         color=INK_2, zorder=3)
            axis.plot([], [], color=SERIES[config], linewidth=8, label=LABELS[config])
        axis.set_xticks(range(len(PROFILES)), [PROFILE_LABELS[p] for p in PROFILES])
        axis.set_yscale("log")
        axis.set_ylabel("p95 latency (ms, thang log)")
        axis.set_title(f"E2 — p95 latency {KIND_LABELS[kind].lower()} theo tải (mỗi chấm là một lần lặp)")
        axis.legend(loc="upper left", ncol=2)
        figure.savefig(PLOTS / f"e2_p95_{kind}.png", dpi=150, bbox_inches="tight")
        plt.close(figure)


def throughput():
    path = RESULTS / "summary.json"
    summary = json.loads(path.read_text()) if path.exists() else {}
    values = {e["engine"]: e["throughput"]["events_per_second"] for e in summary.values()
              if e["experiment"] == "E3" and (e.get("throughput") or {}).get("events_per_second")}
    if not values:
        return
    figure, axis = plt.subplots(figsize=(6, 3.5))
    engines = [e for e in ("flink", "spark") if e in values]
    for index, engine in enumerate(engines):
        item = values[engine]
        color = SERIES["flink" if engine == "flink" else "spark default"]
        axis.bar(index, item["median"], width=0.55, color=color)
        axis.errorbar(index, item["median"], yerr=[[item["median"] - item["min"]], [item["max"] - item["median"]]],
                      color=INK_2, capsize=4, linewidth=1)
        axis.annotate(f"{item['median']:,.0f}", (index, item["max"]), textcoords="offset points", xytext=(0, 6),
                      ha="center", color=INK)
    axis.set_xticks(range(len(engines)), ["Flink", "Spark"])
    axis.set_ylabel("Event RAW / giây")
    axis.set_title("E3 — Throughput khi xử lý backlog HF200 × 2 phút\n(trung vị, thanh lỗi = min–max)")
    axis.grid(axis="x", visible=False)
    figure.savefig(PLOTS / "e3_throughput.png", dpi=150, bbox_inches="tight")
    plt.close(figure)


def resource_series(run_dir, containers, origin):
    seconds = defaultdict(lambda: [0.0, 0])
    with open(run_dir / "resources.csv") as handle:
        latest = {}
        for row in csv.DictReader(handle):
            if row["container"] in containers:
                latest[(int(row["ts_ms"]) // 1000, row["container"])] = (float(row["cpu_pct"]), int(row["mem_bytes"] or 0))
    for (ts, _), (cpu, mem) in latest.items():
        seconds[ts][0] += cpu
        seconds[ts][1] += mem
    points = sorted(seconds.items())
    return [ts - origin / 1000 for ts, _ in points], [v[0] for _, v in points], [v[1] / 2 ** 20 for _, v in points]


def resources():
    containers = {"flink": ["icu-flink-jobmanager", "icu-flink-taskmanager"], "spark": ["icu-spark"]}
    for profile in PROFILES:
        chosen = {config: (run, run_dir) for run, run_dir, config in runs("E2")
                  if run["spec"]["profile"] == profile and run["spec"]["repeat"] == 1 and (run_dir / "resources.csv").exists()}
        if not chosen:
            continue
        for metric, label, index in (("cpu", "CPU (% của một core, cộng các container)", 1),
                                     ("mem", "Bộ nhớ (MiB, cộng các container)", 2)):
            figure, axis = plt.subplots(figsize=(10, 3.5))
            for config, color in SERIES.items():
                if config not in chosen:
                    continue
                run, run_dir = chosen[config]
                series = resource_series(run_dir, containers[run["spec"]["engine"]], run["timeline"]["engine_ready"])
                axis.plot(series[0], series[index], color=color, label=LABELS[config])
            axis.set_xlabel("Giây kể từ khi engine sẵn sàng")
            axis.set_ylabel(label)
            axis.set_title(f"E2 {PROFILE_LABELS[profile]} — {'CPU' if metric == 'cpu' else 'Bộ nhớ'} theo thời gian (lần lặp 1)")
            axis.legend(loc="upper right", ncol=2)
            figure.savefig(PLOTS / f"e2_{metric}_{profile}.png", dpi=150, bbox_inches="tight")
            plt.close(figure)


def spark_batches():
    for profile in PROFILES:
        figure, axis = plt.subplots(figsize=(10, 3.5))
        drawn = False
        for run, run_dir, config in runs("E2"):
            path = run_dir / "spark_metrics" / "wide.jsonl"
            if run["spec"]["profile"] != profile or run["spec"]["repeat"] != 1 or not path.exists():
                continue
            rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
            rows = [r for r in rows if r.get("event") == "progress" and r.get("numInputRows")]
            if not rows:
                continue
            origin = run["timeline"]["engine_ready"] / 1000
            x = [datetime.fromisoformat(r["timestamp"].replace("Z", "+00:00")).timestamp() - origin for r in rows]
            axis.plot(x, [r["durationMs"]["triggerExecution"] for r in rows], color=SERIES[config],
                      label=LABELS[config], marker="o", markersize=3, linewidth=1.2)
            drawn = True
        if drawn:
            axis.set_xlabel("Giây kể từ khi engine sẵn sàng")
            axis.set_ylabel("Thời gian batch Q_wide (ms)")
            axis.set_title(f"E2 {PROFILE_LABELS[profile]} — thời gian micro-batch của Spark Q_wide (lần lặp 1)")
            axis.legend(loc="upper left", ncol=2)
            figure.savefig(PLOTS / f"e2_spark_batches_{profile}.png", dpi=150, bbox_inches="tight")
        plt.close(figure)


def main():
    PLOTS.mkdir(parents=True, exist_ok=True)
    for chart in (latency_cdf, p95_by_load, throughput, resources, spark_batches):
        chart()
    print("\n".join(sorted(p.name for p in PLOTS.glob("*.png"))))


if __name__ == "__main__":
    main()
