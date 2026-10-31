"""Benchmark phases. They run inside the client container (tool.py starts them with `docker compose run`).

Methodology: a standalone client generates 1,000,000 random telemetry records for 10 patients and measures
  1. Write throughput      insert all records with a fixed batch size, records/s
  2. Point-in-time read    every vital of one patient over the last hour
  3. Time-window aggregate AVG heart rate of one patient over 30 days (ICU dashboard)
  4. Workloads             100% READ / 70% READ-30% WRITE / 100% WRITE, 8 clients, on the same dataset
Steps 1-3 run for every scenario: the same 1,000,000 records spread over 30 days, or packed at 1 / 20 / 100 Hz per
patient (100 Hz = 1,000 records/s = 16.7 minutes). The workloads run on the 30-day scenario.
"""
import json
import multiprocessing as mp
import random
import time
from array import array
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dbbench.stores import TELEMETRY, TimescaleStore, CassandraStore, make_store

RESULTS = Path(__file__).resolve().parents[1] / "results"
PROC = mp.get_context("forkserver")   # children do not inherit the parent's generated rows

# ------------------------------------------------------------------------------------------------ dataset
RECORDS, PATIENTS = 1_000_000, 10
END = datetime(2026, 10, 30, tzinfo=timezone.utc)       # "now" of the dataset: reads look back from here
BATCH, WRITERS = 1000, 4                                # fixed batch size, parallel writer processes
# Same 1,000,000 records, different time span: span = RECORDS / (PATIENTS x Hz). reads = (point, aggregation) runs,
# fewer at high Hz because one "last hour" read then returns every sample of the patient.
SCENARIOS = {
    "30d": {"label": "30 ngày", "span_s": 30 * 86_400, "reads": (300, 50)},
    "1hz": {"label": "1 Hz", "span_s": RECORDS // (PATIENTS * 1), "reads": (300, 50)},
    "20hz": {"label": "20 Hz", "span_s": RECORDS // (PATIENTS * 20), "reads": (50, 20)},
    "100hz": {"label": "100 Hz", "span_s": RECORDS // (PATIENTS * 100), "reads": (50, 20)},
}
VITALS = (("heart_rate", 0, 350), ("map", 0, 300), ("sbp", 0, 350), ("spo2", 0, 100))   # repo CHECK ranges


def patients():
    """Synthetic patients, with IDs far outside the repo's demo patients."""
    return [{"subject_id": 90_000_000 + i, "hadm_id": 90_000_000 + i, "stay_id": 90_000_000 + i, "bed_id": 1000 + i}
            for i in range(PATIENTS)]


def patient_ids():
    return [p["stay_id"] for p in patients()]


def generate(span_s, worker=0, workers=1, records=RECORDS):
    """Random telemetry rows in the repo's column order. Each patient gets records/PATIENTS samples spread evenly over
    the span ending at END, with random jitter, and random vitals around its own random baseline. Seeded, so every
    run (and every worker) produces the same records; worker w keeps every workers-th sample."""
    per_patient = records // PATIENTS
    step_us = span_s * 1_000_000 // per_patient
    start = END - timedelta(seconds=span_s)
    rows = []
    for patient, stay in enumerate(patient_ids()):
        rng = random.Random(1000 + patient)
        base = {"heart_rate": rng.uniform(65, 110), "map": rng.uniform(60, 95), "sbp": rng.uniform(100, 140),
                "spo2": rng.uniform(93, 99)}
        spread = {"heart_rate": 8, "map": 7, "sbp": 10, "spo2": 1.5}
        for j in range(per_patient):
            jitter = rng.randrange(step_us // 2)
            values = {name: min(high, max(low, round(rng.gauss(base[name], spread[name]), 1)))
                      for name, low, high in VITALS}
            if j % workers != worker:
                continue
            sample = start + timedelta(microseconds=j * step_us + jitter)
            rows.append({"sample_time": sample, "stay_id": stay, **values})
    return rows


# ---------------------------------------------------------------------------------------------- helpers
def pct(values):
    if not values:
        return {"n": 0, "p50": None, "p95": None, "p99": None, "mean": None, "max": None}
    s = sorted(values)
    n = len(s)
    pick = lambda p: s[min(n - 1, int(round(p / 100 * (n - 1))))]  # noqa: E731
    return {"n": n, "p50": pick(50), "p95": pick(95), "p99": pick(99), "mean": sum(s) / n, "max": s[-1]}


def results():
    path = RESULTS / "results.json"
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    return {"dataset": {"records": RECORDS, "patients": PATIENTS, "end": END.isoformat(), "batch": BATCH,
                        "writers": WRITERS},
            "scenarios": {name: {"label": sc["label"], "span_s": sc["span_s"],
                                 "hz": RECORDS / PATIENTS / sc["span_s"]} for name, sc in SCENARIOS.items()}}


def save(out):
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "results.json").write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")
    print("[saved] results/results.json", flush=True)


def log(*parts):
    print(time.strftime("%H:%M:%S"), *parts, flush=True)


# ----------------------------------------------------------------------------------------------- setup
def phase_setup(args):
    cass = CassandraStore()
    cass.setup()
    cass.close()
    ts = TimescaleStore()
    ts.setup(patients())
    ts.close()
    log(f"cassandra keyspace ready; {PATIENTS} patients in timescaledb icustays")


# ------------------------------------------------------------------------------- 1. write throughput
def write_worker(label, span_s, worker, barrier, queue):
    rows = generate(span_s, worker, WRITERS)
    store = make_store(label)
    barrier.wait()
    wall_start, t0 = time.time(), time.perf_counter()
    latencies = array("d")
    for done in range(0, len(rows), BATCH):
        s = time.perf_counter()
        store.write_batch(rows[done:done + BATCH])
        latencies.append((time.perf_counter() - s) * 1000)
    elapsed = time.perf_counter() - t0
    store.close()
    queue.put({"rows": len(rows), "elapsed": elapsed, "lat": latencies.tobytes(), "wall_start": wall_start,
               "wall_end": time.time()})


def phase_write(args):
    """Insert the scenario's 1,000,000 records into each empty database: WRITERS processes, fixed batch of BATCH
    rows. Rows are generated before the clock starts."""
    out = results()
    scenario = out["scenarios"][args.scenario]
    scenario["write"] = {}
    for label in ("timescaledb", "cassandra"):
        store = make_store(label)
        store.reset()
        store.close()
        barrier, queue = PROC.Barrier(WRITERS + 1), PROC.Queue()
        procs = [PROC.Process(target=write_worker, args=(label, scenario["span_s"], w, barrier, queue))
                 for w in range(WRITERS)]
        for p in procs:
            p.start()
        barrier.wait(timeout=1800)
        parts = [queue.get(timeout=3600) for _ in procs]
        for p in procs:
            p.join()
        rows, elapsed = sum(p["rows"] for p in parts), max(p["elapsed"] for p in parts)
        latencies = [v for p in parts for v in array("d", p["lat"])]
        scenario["write"][label] = {"records": rows, "elapsed_s": elapsed, "records_per_s": rows / elapsed,
                               "batch_latency_ms": pct(latencies),
                               "wall_start": min(p["wall_start"] for p in parts),
                               "wall_end": max(p["wall_end"] for p in parts)}
        log(f"write {args.scenario} {label}: {rows} records in {elapsed:.1f} s = {rows / elapsed:.0f} records/s, "
            f"batch p99 {pct(latencies)['p99']:.0f} ms")
    ts = TimescaleStore()
    ts.conn.execute(f"ANALYZE {TELEMETRY}")
    ts.close()
    save(out)


# ------------------------------------------------------------------------- 2-3. point read and aggregation
READS = [  # (method, what it answers); iterations come from the scenario
    ("q_point_1h", "Point-in-time read: every vital of 1 patient over the last hour"),
    ("q_avg_hr_30d", "Time-window aggregation: AVG heart rate of 1 patient over 30 days"),
]


def signature(name, rows):
    """Comparable summary of an answer, so both databases can be checked to return the same thing."""
    if name == "q_avg_hr_30d":
        return round(float(rows[0][0]), 4) if rows and rows[0][0] is not None else None
    return [len(rows), *(round(float(v), 4) for v in rows[0][-4:])] if rows else [0]


def phase_read(args):
    out = results()
    scenario = out["scenarios"][args.scenario]
    scenario["read"] = {}
    ctx = {"end": END, "stays": patient_ids()}
    for target in args.targets.split(","):
        store = make_store(target)
        info, wall = {}, time.time()
        for (name, description), iterations in zip(READS, SCENARIOS[args.scenario]["reads"]):
            fn = getattr(store, name)
            rng = random.Random(7)
            check = signature(name, fn(ctx["stays"][0], ctx))
            for _ in range(5):   # warm-up
                fn(rng.choice(ctx["stays"]), ctx)
            latencies, sizes = [], []
            for _ in range(iterations):
                stay = rng.choice(ctx["stays"])
                s = time.perf_counter()
                rows = fn(stay, ctx)
                latencies.append((time.perf_counter() - s) * 1000)
                sizes.append(len(rows))
            info[name] = {"description": description, "latency_ms": pct(latencies), "iterations": iterations,
                          "rows_returned_avg": sum(sizes) / len(sizes), "check": check}
            log(f"read {args.scenario:5s} {target:12s} {name:14s} p50 {info[name]['latency_ms']['p50']:9.2f} ms "
                f"p99 {info[name]['latency_ms']['p99']:9.2f} ms rows {info[name]['rows_returned_avg']:.0f}")
        scenario["read"][target] = {"queries": info, "wall_start": wall, "wall_end": time.time()}
        store.close()
    save(out)


# --------------------------------------------------------------------- 4. workloads: read / mixed / write
CLIENTS = 8
WORKLOADS = [("read100", 1.0), ("mixed70_30", 0.7), ("write100", 0.0)]   # (name, share of reads)
NEW_ROWS = 40_000   # new records pre-generated per client (more than a 20 s run can write)


def new_rows(worker, slot):
    """Random new records after the dataset end, one write = one record. (slot, worker, k) makes every key unique."""
    rng = random.Random(slot * 100 + worker)
    stays = patient_ids()
    rows = []
    for k in range(NEW_ROWS):
        sample = END + timedelta(hours=slot, milliseconds=k * CLIENTS + worker)
        values = {name: round(rng.uniform(max(low, 50), min(high, 120)), 1) if name != "spo2"
                  else round(rng.uniform(90, 100), 1) for name, low, high in VITALS}
        rows.append({"sample_time": sample, "stay_id": rng.choice(stays), **values})
    return rows


def workload_worker(label, worker, read_share, slot, duration, barrier, queue):
    """Closed loop: one operation, wait for the answer, next one. READ = the point-in-time read (1 patient, last
    hour); WRITE = insert 1 new record."""
    rows = new_rows(worker, slot) if read_share < 1 else []
    ctx = {"end": END, "stays": patient_ids()}
    rng = random.Random(slot * 1000 + worker)
    store = make_store(label)
    lat = {"read": array("d"), "write": array("d")}
    written, exhausted = 0, False
    barrier.wait()
    wall_start, t0 = time.time(), time.perf_counter()
    deadline = t0 + duration
    while time.perf_counter() < deadline:
        if rng.random() < read_share:
            stay = rng.choice(ctx["stays"])
            s = time.perf_counter()
            store.q_point_1h(stay, ctx)
            lat["read"].append((time.perf_counter() - s) * 1000)
        else:
            if written >= len(rows):
                exhausted = True
                break
            s = time.perf_counter()
            store.write_one(rows[written])
            lat["write"].append((time.perf_counter() - s) * 1000)
            written += 1
    elapsed = time.perf_counter() - t0
    store.close()
    queue.put({"elapsed": elapsed, "exhausted": exhausted, "wall_start": wall_start, "wall_end": time.time(),
               "read": lat["read"].tobytes(), "write": lat["write"].tobytes()})


def run_workload(label, name, read_share, slot, duration):
    barrier, queue = PROC.Barrier(CLIENTS + 1), PROC.Queue()
    procs = [PROC.Process(target=workload_worker, args=(label, w, read_share, slot, duration, barrier, queue))
             for w in range(CLIENTS)]
    for p in procs:
        p.start()
    barrier.wait(timeout=600)
    parts = [queue.get(timeout=duration + 600) for _ in procs]
    for p in procs:
        p.join()
    elapsed = max(p["elapsed"] for p in parts)
    lat = {kind: [v for p in parts for v in array("d", p[kind])] for kind in ("read", "write")}
    ops = len(lat["read"]) + len(lat["write"])
    return {"target": label, "workload": name, "read_share": read_share, "clients": CLIENTS, "elapsed_s": elapsed,
            "ops": ops, "ops_per_s": ops / elapsed, "read_ops_per_s": len(lat["read"]) / elapsed,
            "write_ops_per_s": len(lat["write"]) / elapsed, "read_latency_ms": pct(lat["read"]),
            "write_latency_ms": pct(lat["write"]), "exhausted": any(p["exhausted"] for p in parts),
            "wall_start": min(p["wall_start"] for p in parts), "wall_end": max(p["wall_end"] for p in parts)}


def phase_workload(args):
    """Runs on the 30-day scenario after its reads: its writes add records after the dataset end, so the reads are
    measured on exactly the 1,000,000 records."""
    out = results()
    out["workloads"] = []
    for label in ("timescaledb", "cassandra"):
        run_workload(label, "warm-up", 0.7, 0, 5)
        for slot, (name, share) in enumerate(WORKLOADS, start=1):
            result = run_workload(label, name, share, slot, args.duration)
            log(f"workload {label} {name}: {result['ops_per_s']:.0f} ops/s (read {result['read_ops_per_s']:.0f}, "
                f"write {result['write_ops_per_s']:.0f}), read p99 {result['read_latency_ms']['p99'] or 0:.2f} ms, "
                f"write p99 {result['write_latency_ms']['p99'] or 0:.2f} ms"
                f"{' (new rows exhausted)' if result['exhausted'] else ''}")
            out["workloads"].append(result)
    out["workload_setup"] = {"clients": CLIENTS, "duration_s": args.duration, "scenario": "30d"}
    save(out)


PHASES = {"setup": phase_setup, "write": phase_write, "read": phase_read, "workload": phase_workload}
