#!/usr/bin/env python3
"""TimescaleDB (main module) vs Apache Cassandra (alternative module) for the ICU streaming repo.

Methodology: a standalone Python client generates 1,000,000 random telemetry records (10 patients) and measures
write throughput (fixed batch size), point-in-time read (1 patient, last hour) and time-window aggregation (AVG heart
rate of 1 patient over 30 days) for 4 scenarios: the records spread over 30 days, or packed at 1 / 20 / 100 Hz per
patient (100 Hz = 16.7 minutes). 100% READ / 70-30 / 100% WRITE workloads run on the 30-day scenario.
Runs on its own Docker stack (compose project `icu-dbbench`: TimescaleDB, Cassandra, client; own network,
ports 25432/29042, volumes). The demo pipeline and the repo code are only read, never changed.

Host side needs only Docker and the Python standard library:

    python tool.py up                 # start timescaledb + cassandra, build the client image
    python tool.py all                # setup -> (write -> read) x 4 scenarios, workload on 30d -> report
    python tool.py write --scenario 100hz   # then: python tool.py read --scenario 100hz
    python tool.py down [--volumes]   # stop the stack (and drop its data volumes)

Single steps: setup | write | read | workload | report. Each step runs inside the `client` container (same
network as the databases, 4 CPU), while this process samples `docker stats`. Results land in
results/results.json, charts and results/REPORT.md.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
COMPOSE = ["docker", "compose", "-f", str(HERE / "compose.yaml")]
RESULTS = HERE / "results"
RAW = RESULTS / "raw"
WATCHED = ["icu-dbbench-timescaledb", "icu-dbbench-cassandra", "icu-dbbench-client"]
IN_CONTAINER = bool(os.environ.get("BENCH_IN_CONTAINER"))


# ------------------------------------------------------------------------------------------------ host
def sh(command, check=True, capture=False):
    print("$", " ".join(command), flush=True)
    return subprocess.run(command, check=check, text=True, capture_output=capture)


class StatsSampler(threading.Thread):
    """`docker stats` of the bench containers, about once a second, for CPU/RAM per phase and per run."""

    def __init__(self):
        super().__init__(daemon=True)
        self.samples, self.stopped = [], threading.Event()

    def run(self):
        while not self.stopped.is_set():
            # `docker stats` fails as a whole if one named container is absent (the client comes and goes)
            running = subprocess.run(["docker", "ps", "--format", "{{.Names}}"], text=True,
                                     capture_output=True).stdout.split()
            names = [name for name in WATCHED if name in running]
            if not names:
                time.sleep(1)
                continue
            proc = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *names],
                                  text=True, capture_output=True)
            now = time.time()
            for line in proc.stdout.splitlines():
                try:
                    row = json.loads(line)
                    self.samples.append({"t": now, "name": row["Name"], "cpu": float(row["CPUPerc"].rstrip("%")),
                                         "mem": parse_bytes(row["MemUsage"].split("/")[0])})
                except (ValueError, KeyError):
                    continue

    def stop(self):
        self.stopped.set()
        self.join(timeout=10)


def parse_bytes(text):
    text = text.strip()
    for unit, factor in (("GiB", 2 ** 30), ("MiB", 2 ** 20), ("KiB", 2 ** 10), ("kB", 1e3), ("MB", 1e6),
                         ("GB", 1e9), ("B", 1)):
        if text.endswith(unit):
            return float(text[: -len(unit)]) * factor
    return float(text)


def in_client(phase, *extra):
    """Run one phase inside the client container while sampling resource usage from the host."""
    RAW.mkdir(parents=True, exist_ok=True)
    sampler = StatsSampler()
    sampler.start()
    started = time.time()
    try:
        sh([*COMPOSE, "--profile", "client", "run", "--rm", "--name", "icu-dbbench-client", "client",
            "python", "tool.py", "_exec", phase, *extra])
    finally:
        sampler.stop()
        (RAW / f"stats-{phase}-{int(started)}.json").write_text(json.dumps(sampler.samples), encoding="utf-8")


def clear_snapshots():
    """TRUNCATE in Cassandra keeps an auto-snapshot of the old data; drop them so the disk does not fill up."""
    sh(["docker", "exec", "icu-dbbench-cassandra", "nodetool", "clearsnapshot", "--all"], check=False)


def host_up(args):
    sh([*COMPOSE, "up", "-d", "--wait", "timescaledb", "cassandra"])
    sh([*COMPOSE, "--profile", "client", "build", "client"])


def host_down(args):
    sh([*COMPOSE, "--profile", "client", "down", *(["--volumes"] if args.volumes else [])])


SCENARIOS = ("30d", "1hz", "20hz", "100hz")   # same 1,000,000 records over 30 days, or packed at 1 / 20 / 100 Hz


def host_write(args):
    """Insert the scenario's 1,000,000 records into both empty databases."""
    in_client("write", "--scenario", args.scenario)
    clear_snapshots()


def host_read(args):
    """Point-in-time read + 30-day aggregation on both databases, on the scenario loaded by `write`."""
    in_client("read", "--scenario", args.scenario, "--targets", "timescaledb,cassandra")


def host_workload(args):
    """100% READ / 70-30 / 100% WRITE on the loaded scenario (`all` runs it on the 30-day one)."""
    in_client("workload", "--duration", str(args.duration or 20))
    clear_snapshots()


def host_all(args):
    host_up(args)
    in_client("setup")
    (RESULTS / "results.json").unlink(missing_ok=True)
    for scenario in SCENARIOS:
        args.scenario = scenario
        host_write(args)
        host_read(args)
        if scenario == "30d":
            host_workload(args)
    in_client("report")


# ------------------------------------------------------------------------------------------- container
def container_exec(argv):
    parser = argparse.ArgumentParser(prog="tool.py _exec")
    parser.add_argument("phase")
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--targets", default="timescaledb,cassandra")
    parser.add_argument("--scenario", default="30d")
    args = parser.parse_args(argv)
    if args.phase == "report":
        from dbbench import report
        report.build()
        return
    from dbbench.phases import PHASES
    PHASES[args.phase](args)


def main():
    if len(sys.argv) > 1 and sys.argv[1] == "_exec":
        if not IN_CONTAINER:
            sys.exit("_exec runs inside the client container; use `python tool.py <phase>`")
        container_exec(sys.argv[2:])
        return
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=["up", "down", "all", "setup", "write", "read", "workload", "report"])
    parser.add_argument("--volumes", action="store_true", help="down: also remove the bench data volumes")
    parser.add_argument("--duration", type=int, help="workload: seconds per workload (default 20)")
    parser.add_argument("--scenario", choices=SCENARIOS, default="30d",
                        help="write/read: 1,000,000 records over 30 days, or packed at 1 / 20 / 100 Hz per patient")
    args = parser.parse_args()
    actions = {"up": host_up, "down": host_down, "all": host_all, "write": host_write, "read": host_read,
               "workload": host_workload, "setup": lambda a: in_client("setup"),
               "report": lambda a: in_client("report")}
    actions[args.command](args)


if __name__ == "__main__":
    main()
