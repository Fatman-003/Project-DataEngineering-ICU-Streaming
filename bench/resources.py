"""Sample `docker stats` about once per second for the measured engine containers.

    python bench/resources.py --out resources.csv --containers icu-spark [--docker "sudo docker"]

Runs until terminated (bench/run.py starts and stops it). Columns: wall-clock
UNIX ms, container, CPU % (100 = one core), memory bytes, memory limit bytes.
"""
import argparse
import csv
import json
import re
import shlex
import signal
import subprocess
import sys
import time

ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
UNITS = {"b": 1, "kb": 1e3, "mb": 1e6, "gb": 1e9, "kib": 1024, "mib": 1024 ** 2, "gib": 1024 ** 3, "tib": 1024 ** 4}


def to_bytes(text):
    match = re.fullmatch(r"\s*([\d.]+)\s*([A-Za-z]+)\s*", text)
    return None if not match else int(float(match.group(1)) * UNITS[match.group(2).lower()])


def parse(line):
    start = line.find("{")
    if start < 0:
        return None
    item = json.loads(line[start:])
    used, _, limit = item.get("MemUsage", "").partition("/")
    return {"container": item.get("Name"), "cpu_pct": float(item.get("CPUPerc", "0%").rstrip("%") or 0),
            "mem_bytes": to_bytes(used), "mem_limit_bytes": to_bytes(limit)}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True)
    parser.add_argument("--containers", required=True)
    parser.add_argument("--docker", default="docker")
    args = parser.parse_args()
    command = [*shlex.split(args.docker), "stats", "--format", "{{json .}}", *args.containers.split(",")]
    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True, bufsize=1)
    signal.signal(signal.SIGTERM, lambda *_: (process.terminate(), sys.exit(0)))
    with open(args.out, "w", newline="") as out:
        writer = csv.DictWriter(out, ["ts_ms", "container", "cpu_pct", "mem_bytes", "mem_limit_bytes"])
        writer.writeheader()
        for line in process.stdout:
            # Each refresh carries terminal control sequences (cursor home, erase line).
            for chunk in ANSI.sub("\n", line).splitlines():
                try:
                    row = parse(chunk)
                except (ValueError, KeyError):
                    continue
                if row and row["container"]:
                    writer.writerow({"ts_ms": int(time.time() * 1000), **row})
            out.flush()


if __name__ == "__main__":
    main()
