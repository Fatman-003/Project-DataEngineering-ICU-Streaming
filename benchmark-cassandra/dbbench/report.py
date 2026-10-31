"""results/*.json -> charts + results/REPORT.md (numbers only; the analysis lives in README.md).

Sections follow the methodology: write throughput, point-in-time read, time-window aggregation, read/write workloads.
Colors: TimescaleDB blue, Cassandra orange (categorical slots 1-2, validated for colour-vision deficiency); every chart
carries value labels and every number is also in a table.
"""
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter, NullFormatter  # noqa: E402
import math  # noqa: E402

RESULTS = Path(__file__).resolve().parents[1] / "results"
DB_CONTAINER = {"timescaledb": "icu-dbbench-timescaledb", "cassandra": "icu-dbbench-cassandra"}
COLORS = {"timescaledb": "#2a78d6", "cassandra": "#eb6834"}
NAMES = {"timescaledb": "TimescaleDB", "cassandra": "Cassandra"}
INK, MUTED, GRID = "#262624", "#6b6a64", "#e6e5e0"
WORKLOAD_NAMES = {"read100": "100% READ", "mixed70_30": "70% READ / 30% WRITE", "write100": "100% WRITE"}
QUERY_NAMES = {"q_point_1h": "Point-in-time read: mọi chỉ số 1 bệnh nhân, 1 giờ qua",
               "q_avg_hr_30d": "Time-window aggregation: AVG nhịp tim 1 bệnh nhân, 30 ngày"}
TARGETS = ("timescaledb", "cassandra")

plt.rcParams.update({"font.size": 9, "axes.edgecolor": MUTED, "axes.labelcolor": INK, "xtick.color": MUTED,
                     "ytick.color": MUTED, "text.color": INK, "axes.titleweight": "bold", "axes.titlesize": 10,
                     "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
                     "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
                     "legend.frameon": False, "figure.dpi": 130})


def load():
    path = RESULTS / "results.json"
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


SAMPLES = []


def stats_samples():
    samples = []
    for path in sorted((RESULTS / "raw").glob("stats-*.json")):
        samples.extend(json.loads(path.read_text(encoding="utf-8")))
    return samples


def usage(container, start, end):
    rows = [s for s in SAMPLES if s["name"] == container and start <= s["t"] <= end]
    if not rows:
        return None, None
    return sum(r["cpu"] for r in rows) / len(rows), max(r["mem"] for r in rows) / 2 ** 20


def fmt(value, digits=1):
    if value is None:
        return "–"
    if isinstance(value, (int, float)) and abs(value) >= 1000:
        return f"{value:,.0f}".replace(",", ".")
    return f"{value:.{digits}f}".replace(".", ",")


def ok(flag):
    return "✅" if flag else "❌"


def ratio_text(ts, cass, higher_better):
    """'TimescaleDB 2,3×' style verdict: who wins and by how much."""
    if not ts or not cass:
        return "–", None
    better_ts = ts > cass if higher_better else ts < cass
    factor = max(ts, cass) / min(ts, cass)
    if factor < 1.05:
        return "≈ ngang nhau", None
    return f"**{NAMES['timescaledb' if better_ts else 'cassandra']} {fmt(factor, 1)}×**", "timescaledb" if better_ts else "cassandra"


def bar_labels(ax, bars, values, unit=""):
    ax.bar_label(bars, labels=[f"{fmt(v, 2 if v < 10 else 0)}{unit}" for v in values], fontsize=7.5, padding=2,
                 color=INK)


def save_fig(fig, name):
    fig.tight_layout()
    fig.savefig(RESULTS / name)
    plt.close(fig)


NICE = [0.1, 0.2, 0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000]


def plain_log(axis):
    """Log axis with plain numbers (0,5 / 1 / 10 / 100), not 10^n; 1-2-5 steps when the range is narrow."""
    axis.set_major_formatter(FuncFormatter(lambda v, _: fmt(v, 1 if v < 1 else 0)))
    axis.set_minor_formatter(NullFormatter())
    low, high = sorted(axis.get_view_interval())
    inside = [v for v in NICE if low <= v <= high]
    if len(inside) <= 2:   # narrow range: 1-2-3-5-7 steps
        inside = [m * 10 ** e for e in range(-1, 5) for m in (1, 2, 3, 5, 7) if low <= m * 10 ** e <= high]
    if len(inside) <= 5:
        axis.set_ticks(inside or [low, high])


def plain_linear(axis):
    axis.set_major_formatter(FuncFormatter(lambda v, _: fmt(v, 0)))


def grouped_bars(ax, groups, series, values, unit="", log=False, horizontal=False):
    """values[s][g]; thin bars with a small gap, value labels on every bar."""
    width = 0.8 / len(series)
    for i, s in enumerate(series):
        pos = [g + (i - (len(series) - 1) / 2) * width for g in range(len(groups))]
        vals = [values[s][g] or 0 for g in range(len(groups))]
        draw = ax.barh if horizontal else ax.bar
        bars = draw(pos, vals, width * 0.92, label=NAMES[s], color=COLORS[s])
        bar_labels(ax, bars, vals, unit)
    if horizontal:
        ax.set_yticks(range(len(groups)), groups)
        ax.invert_yaxis()
        ax.grid(axis="y", visible=False)
        if log:
            ax.set_xscale("log")
            plain_log(ax.xaxis)
    else:
        ax.set_xticks(range(len(groups)), groups)
        ax.grid(axis="x", visible=False)
        if log:
            ax.set_yscale("log")
            plain_log(ax.yaxis)
    ax.legend(fontsize=8)


# --------------------------------------------------------------------------------------------- sections
def summary_rows(data):
    """(group, label, ts, cass, unit, higher_better) for every headline measurement."""
    rows = []
    w = data.get("write", {})
    if w:
        rows.append(("Ghi", "Insert 1.000.000 record, lô 1.000", w["timescaledb"]["records_per_s"],
                     w["cassandra"]["records_per_s"], "record/s", True))
    r = data.get("read", {})
    if "timescaledb" in r and "cassandra" in r:
        for name, short in (("q_point_1h", "Point-in-time read (1 giờ)"), ("q_avg_hr_30d", "AVG nhịp tim 30 ngày")):
            rows.append(("Đọc", short, r["timescaledb"]["queries"][name]["latency_ms"]["p50"],
                         r["cassandra"]["queries"][name]["latency_ms"]["p50"], "ms p50", False))
    by = {(x["target"], x["workload"]): x for x in data.get("workloads", [])}
    for workload in WORKLOAD_NAMES:
        if ("timescaledb", workload) in by:
            rows.append(("Workload", f"{WORKLOAD_NAMES[workload]}, 8 client", by["timescaledb", workload]["ops_per_s"],
                         by["cassandra", workload]["ops_per_s"], "ops/s", True))
    return rows


def section_summary(lines, data):
    rows = summary_rows(data)
    if not rows:
        return
    lines += ["## Tóm tắt", "", "| Nhóm | Phép đo | TimescaleDB | Cassandra | Đơn vị | Bên tốt hơn |",
              "|---|---|---:|---:|---|---|"]
    wins = {"timescaledb": 0, "cassandra": 0}
    for group, label, ts, cass, unit, higher in rows:
        verdict, winner = ratio_text(ts, cass, higher)
        if winner:
            wins[winner] += 1
        lines.append(f"| {group} | {label} | {fmt(ts, 2)} | {fmt(cass, 2)} | {unit} | {verdict} |")
    lines += ["", f"TimescaleDB tốt hơn ở **{wins['timescaledb']}** phép đo, Cassandra ở **{wins['cassandra']}** "
              f"(trên {len(rows)}). Với độ trễ, nhỏ hơn là tốt hơn.", "", "![Tóm tắt](summary.png)", ""]
    fig, ax = plt.subplots(figsize=(9, 0.42 * len(rows) + 1.3))
    labels, factors, colors = [], [], []
    for group, label, ts, cass, unit, higher in rows:
        factor = (ts / cass) if higher else (cass / ts)
        labels.append(f"{group}: {label}")
        factors.append(factor)
        colors.append(COLORS["timescaledb"] if factor >= 1 else COLORS["cassandra"])
    y = range(len(rows))
    signed = [math.log10(f) if f >= 1 else -math.log10(1 / f) for f in factors]
    bars = ax.barh(y, signed, color=colors, height=0.62)
    for bar, factor in zip(bars, factors):
        value = factor if factor >= 1 else 1 / factor
        x = bar.get_width()
        ax.text(x + (0.03 if x >= 0 else -0.03), bar.get_y() + bar.get_height() / 2, f"{fmt(value, 1)}×",
                va="center", ha="left" if x >= 0 else "right", fontsize=7.5, color=INK)
    ax.axvline(0, color=MUTED, linewidth=1)
    ax.set_yticks(list(y), labels)
    ax.invert_yaxis()
    span = max(1.0, max(abs(v) for v in signed) + 0.25)
    marks = [m for m in (1, 2, 5, 10, 20, 50, 100, 200) if math.log10(m) <= span]
    ticks = [-math.log10(m) for m in reversed(marks[1:])] + [math.log10(m) for m in marks]
    ax.set_xticks(ticks, [f"{round(10 ** abs(t))}×" for t in ticks])
    ax.set_xlim(-span, span)
    ax.set_xlabel("← Cassandra tốt hơn bao nhiêu lần        |        TimescaleDB tốt hơn bao nhiêu lần →")
    ax.grid(axis="y", visible=False)
    ax.set_title("Bên nào tốt hơn, và tốt hơn bao nhiêu lần")
    save_fig(fig, "summary.png")


def span_text(seconds):
    if seconds >= 2 * 86_400:
        return f"{fmt(seconds / 86_400, 0)} ngày"
    if seconds >= 2 * 3_600:
        return f"{fmt(seconds / 3_600, 1)} giờ"
    return f"{fmt(seconds / 60, 1)} phút"


def section_dataset(lines, data):
    d = data.get("dataset")
    if not d:
        return
    per = d["records"] // d["patients"]
    lines += ["## Dữ liệu và phương pháp", "",
              f"- **Data generator**: client Python độc lập sinh ngẫu nhiên **{fmt(d['records'])} record** telemetry "
              f"(HR, MAP, SBP, SpO2) cho **{d['patients']} bệnh nhân** ({fmt(per)} mẫu/bệnh nhân), kết thúc lúc "
              f"{d['end'][:16].replace('T', ' ')} UTC. Giá trị ngẫu nhiên quanh mức nền riêng của từng bệnh nhân, "
              "trong giới hạn CHECK của schema repo; seed cố định nên chạy lại ra đúng cùng dữ liệu.",
              "- **Cùng 1.000.000 record, 4 kịch bản thời gian** (mục 1–3 dùng kịch bản 30 ngày, mục 5 so cả 4):", "",
              "| Kịch bản | Tần số / bệnh nhân | Tổng record/giây | Trải trong |", "|---|---:|---:|---|"]
    for sc in data.get("scenarios", {}).values():
        lines.append(f"| {sc['label']} | {fmt(sc['hz'], 2 if sc['hz'] < 1 else 0)} Hz | "
                     f"{fmt(sc['hz'] * d['patients'], 1 if sc['hz'] < 1 else 0)} | {span_text(sc['span_s'])} |")
    lines += ["",
              f"- **Ghi**: {d['writers']} tiến trình, lô cố định **{fmt(d['batch'])} record**, DB trống, dữ liệu sinh "
              "xong trước khi bấm giờ.",
              "- **Đọc**: 1 client tuần tự, bệnh nhân ngẫu nhiên, 5 lần khởi động không tính. Mỗi truy vấn được so "
              "đáp án giữa hai DB.",
              "- **TimescaleDB** đúng như repo đang chạy: schema Flyway của repo, không bật nén.", ""]


def section_write(lines, data):
    w = data.get("write")
    if not w:
        return
    lines += ["## 1. Write Throughput: insert 1.000.000 record (kịch bản 30 ngày)", "",
              "| DB | record | thời gian (s) | **record/s** | p50 / lô (ms) | p95 / lô (ms) | p99 / lô (ms) | CPU DB % |",
              "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for label in ("timescaledb", "cassandra"):
        x = w[label]
        cpu, _ = usage(DB_CONTAINER[label], x["wall_start"], x["wall_end"])
        lat = x["batch_latency_ms"]
        lines.append(f"| {NAMES[label]} | {fmt(x['records'])} | {fmt(x['elapsed_s'])} | **{fmt(x['records_per_s'])}** | "
                     f"{fmt(lat['p50'])} | {fmt(lat['p95'])} | {fmt(lat['p99'])} | {fmt(cpu)} |")
    verdict, _ = ratio_text(w["timescaledb"]["records_per_s"], w["cassandra"]["records_per_s"], True)
    lines += ["", f"Bên ghi nhanh hơn: {verdict}.", "", "![Ghi](write.png)", ""]
    targets = ["timescaledb", "cassandra"]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.8))
    for ax, key, title, ylabel in ((a1, "records_per_s", "Thông lượng ghi", "record/s (cao hơn = tốt hơn)"),
                                   (a2, "elapsed_s", "Thời gian insert 1.000.000 record", "giây (thấp hơn = tốt hơn)")):
        vals = [w[t][key] for t in targets]
        bars = ax.bar([NAMES[t] for t in targets], vals, width=0.5, color=[COLORS[t] for t in targets])
        bar_labels(ax, bars, vals, "" if key == "records_per_s" else " s")
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.grid(axis="x", visible=False)
        plain_linear(ax.yaxis)
    save_fig(fig, "write.png")


def section_reads(lines, data):
    r = data.get("read")
    if not r:
        return
    targets = [t for t in TARGETS if t in r]
    for number, name in ((2, "q_point_1h"), (3, "q_avg_hr_30d")):
        lines += [f"## {number}. {QUERY_NAMES[name]} (kịch bản 30 ngày)", ""]
        first = r[targets[0]]["queries"][name]
        note = ""
        if name == "q_avg_hr_30d":
            note = f" (1 giá trị, tính trên {fmt(data['dataset']['records'] / data['dataset']['patients'])} mẫu)"
        lines += [f"Lặp {first['iterations']} lần; trung bình **{fmt(first['rows_returned_avg'], 0)} hàng** trả về{note}.",
                  "", "| DB | p50 ms | p95 ms | p99 ms | max ms | Kết quả giống TimescaleDB |",
                  "|---|---:|---:|---:|---:|---|"]
        for t in targets:
            q = r[t]["queries"][name]
            lat = q["latency_ms"]
            same = json.dumps(q["check"]) == json.dumps(r["timescaledb"]["queries"][name]["check"])
            lines.append(f"| {NAMES[t]} | **{fmt(lat['p50'], 2)}** | {fmt(lat['p95'], 2)} | {fmt(lat['p99'], 2)} | "
                         f"{fmt(lat['max'], 2)} | {ok(same)} |")
        verdict, _ = ratio_text(r["timescaledb"]["queries"][name]["latency_ms"]["p50"],
                                r["cassandra"]["queries"][name]["latency_ms"]["p50"], False)
        lines += ["", f"So p50: {verdict}.", "", f"![{name}]({name}.png)", ""]
        fig, ax = plt.subplots(figsize=(8, 3.6))
        grouped_bars(ax, ["p50", "p95", "p99"], targets,
                     {t: [r[t]["queries"][name]["latency_ms"][p] for p in ("p50", "p95", "p99")] for t in targets},
                     log=True)
        ax.set_ylabel("ms (log, thấp hơn = tốt hơn)")
        ax.set_title(QUERY_NAMES[name], fontsize=9)
        save_fig(fig, f"{name}.png")


def section_workloads(lines, data):
    runs = data.get("workloads")
    if not runs:
        return
    setup = data["workload_setup"]
    by = {(x["target"], x["workload"]): x for x in runs}
    lines += ["## 4. Workloads: 100% READ, 70% READ / 30% WRITE, 100% WRITE (kịch bản 30 ngày)", "",
              f"Trên cùng 1.000.000 record; {setup['clients']} client song song, vòng kín (gửi lệnh, chờ xong, gửi "
              f"lệnh tiếp), mỗi kịch bản {setup['duration_s']} s sau 5 s khởi động. READ = point-in-time read "
              "(1 bệnh nhân ngẫu nhiên, 1 giờ qua); WRITE = insert 1 record mới.", "",
              "| Kịch bản | DB | **ops/s** | READ ops/s | WRITE ops/s | READ p50 / p99 ms | WRITE p50 / p99 ms | CPU DB % |",
              "|---|---|---:|---:|---:|---:|---:|---:|"]
    for workload in WORKLOAD_NAMES:
        for t in ("timescaledb", "cassandra"):
            x = by.get((t, workload))
            if not x:
                continue
            cpu, _ = usage(DB_CONTAINER[t], x["wall_start"], x["wall_end"])
            rl, wl = x["read_latency_ms"], x["write_latency_ms"]
            lines.append(f"| {WORKLOAD_NAMES[workload]} | {NAMES[t]} | **{fmt(x['ops_per_s'])}** | "
                         f"{fmt(x['read_ops_per_s'])} | {fmt(x['write_ops_per_s'])} | "
                         f"{fmt(rl['p50'], 2)} / {fmt(rl['p99'], 2)} | {fmt(wl['p50'], 2)} / {fmt(wl['p99'], 2)} | "
                         f"{fmt(cpu)} |" + (" ⚠ hết dữ liệu sinh sẵn" if x["exhausted"] else ""))
    lines += ["", "![Workloads](workloads.png)", ""]
    workloads = [w for w in WORKLOAD_NAMES if ("timescaledb", w) in by]
    tg = ["timescaledb", "cassandra"]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(11, 4))
    grouped_bars(a1, [WORKLOAD_NAMES[w] for w in workloads], tg,
                 {t: [by[t, w]["ops_per_s"] for w in workloads] for t in tg})
    plain_linear(a1.yaxis)
    a1.set_ylabel("ops/s (cao hơn = tốt hơn)")
    a1.set_title(f"Thông lượng, {setup['clients']} client")
    groups, values = [], {t: [] for t in tg}
    short = {"read100": "100% READ", "mixed70_30": "70/30", "write100": "100% WRITE"}
    for w in workloads:
        for kind in ("read", "write"):
            if by["timescaledb", w][f"{kind}_latency_ms"]["n"]:
                groups.append(f"{short[w]}\n{kind.upper()} p99")
                for t in tg:
                    values[t].append(by[t, w][f"{kind}_latency_ms"]["p99"])
    grouped_bars(a2, groups, tg, values, log=True)
    a2.set_ylabel("p99 ms (log, thấp hơn = tốt hơn)")
    a2.set_title("Độ trễ p99 mỗi lệnh")
    a2.tick_params(axis="x", labelsize=7.5)
    save_fig(fig, "workloads.png")


def section_hz(lines, data):
    scenarios = [sc for sc in data.get("scenarios", {}).values() if "write" in sc and "read" in sc]
    if len(scenarios) < 2:
        return
    tg = ["timescaledb", "cassandra"]
    lines += ["## 5. Theo tần số mẫu: cùng 1.000.000 record, ép vào khoảng thời gian ngắn dần", "",
              "Mỗi kịch bản ghi lại 1.000.000 record vào DB trống rồi đo 2 truy vấn. Tần số càng cao, \"1 giờ qua\" của "
              "1 bệnh nhân càng nhiều hàng (ở 100 Hz là toàn bộ 100.000 mẫu của người đó). TimescaleDB chia chunk 1 giờ "
              "nên số chunk giảm từ 720 (30 ngày) xuống 1 (100 Hz); Cassandra chia partition theo ngày.", "",
              "| Kịch bản | Trải trong | TimescaleDB record/s | Cassandra record/s | Ghi nhanh hơn | "
              "Point read: hàng | TS p50 ms | Cass p50 ms | Đọc nhanh hơn | AVG 30 ngày: TS p50 ms | Cass p50 ms | "
              "Tổng hợp nhanh hơn | Đáp án giống |",
              "|---|---|---:|---:|---|---:|---:|---:|---|---:|---:|---|---|"]
    for sc in scenarios:
        w, r = sc["write"], sc["read"]
        q = {t: r[t]["queries"] for t in tg}
        write_verdict, _ = ratio_text(w["timescaledb"]["records_per_s"], w["cassandra"]["records_per_s"], True)
        point_verdict, _ = ratio_text(q["timescaledb"]["q_point_1h"]["latency_ms"]["p50"],
                                      q["cassandra"]["q_point_1h"]["latency_ms"]["p50"], False)
        avg_verdict, _ = ratio_text(q["timescaledb"]["q_avg_hr_30d"]["latency_ms"]["p50"],
                                    q["cassandra"]["q_avg_hr_30d"]["latency_ms"]["p50"], False)
        same = all(json.dumps(q["timescaledb"][n]["check"]) == json.dumps(q["cassandra"][n]["check"])
                   for n in ("q_point_1h", "q_avg_hr_30d"))
        lines.append(f"| **{sc['label']}** | {span_text(sc['span_s'])} | {fmt(w['timescaledb']['records_per_s'])} | "
                     f"{fmt(w['cassandra']['records_per_s'])} | {write_verdict} | "
                     f"{fmt(q['timescaledb']['q_point_1h']['rows_returned_avg'], 0)} | "
                     f"{fmt(q['timescaledb']['q_point_1h']['latency_ms']['p50'], 2)} | "
                     f"{fmt(q['cassandra']['q_point_1h']['latency_ms']['p50'], 2)} | {point_verdict} | "
                     f"{fmt(q['timescaledb']['q_avg_hr_30d']['latency_ms']['p50'], 1)} | "
                     f"{fmt(q['cassandra']['q_avg_hr_30d']['latency_ms']['p50'], 1)} | {avg_verdict} | {ok(same)} |")
    lines += ["", "![Theo tần số mẫu](hz.png)", ""]
    labels = [sc["label"] if sc["label"] == span_text(sc["span_s"]) else f"{sc['label']}\n({span_text(sc['span_s'])})"
              for sc in scenarios]
    fig, axes = plt.subplots(1, 3, figsize=(13, 4))
    panels = [("Write Throughput", "record/s (cao hơn = tốt hơn)", False,
               lambda sc, t: sc["write"][t]["records_per_s"]),
              ("Point-in-time read (1 giờ qua), p50", "ms (log, thấp hơn = tốt hơn)", True,
               lambda sc, t: sc["read"][t]["queries"]["q_point_1h"]["latency_ms"]["p50"]),
              ("AVG nhịp tim 30 ngày, p50", "ms (log, thấp hơn = tốt hơn)", True,
               lambda sc, t: sc["read"][t]["queries"]["q_avg_hr_30d"]["latency_ms"]["p50"])]
    for ax, (title, ylabel, log, value) in zip(axes, panels):
        grouped_bars(ax, labels, tg, {t: [value(sc, t) for sc in scenarios] for t in tg}, log=log)
        if not log:
            plain_linear(ax.yaxis)
        ax.set_title(title)
        ax.set_ylabel(ylabel)
        ax.tick_params(axis="x", labelsize=7.5)
    fig.suptitle("Cùng 1.000.000 record ở các tần số mẫu khác nhau", fontweight="bold", fontsize=10)
    save_fig(fig, "hz.png")


def section_setup(lines):
    lines += ["## Môi trường đo", "",
              "- Mỗi DB **2 CPU / 2 GB** (TimescaleDB 2.30.2-pg17 với schema Flyway của repo, hypertable chunk 1 giờ; "
              "Cassandra 5.0.4, heap 1 GB, 1 node, partition `(stay_id, day)`). Client Python 4 CPU, cùng mạng Docker.",
              "- CPU DB % là trung bình `docker stats` trong lúc đo (200% = 2 CPU bão hòa).",
              "- p50 / p95 / p99: 50% / 95% / 99% số lệnh xong trong thời gian này hoặc nhanh hơn.", ""]


def build():
    SAMPLES.extend(stats_samples())
    data = load()
    base = {**data, **data.get("scenarios", {}).get("30d", {})}   # sections 1-4: the 30-day scenario
    lines = ["# TimescaleDB vs Cassandra: kết quả benchmark", "",
             "Sinh tự động bởi `python tool.py report`. Phương pháp và phân tích: [README.md](../README.md).", ""]
    section_summary(lines, base)
    section_dataset(lines, data)
    section_write(lines, base)
    section_reads(lines, base)
    section_workloads(lines, data)
    section_hz(lines, data)
    section_setup(lines)
    (RESULTS / "REPORT.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    print("[saved] results/REPORT.md")
