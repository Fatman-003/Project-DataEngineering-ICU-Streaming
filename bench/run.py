"""Run benchmark experiments E1-E4 (plan §10); resumable, one directory per run.

    python bench/run.py --plan e1|e2|e3|e4|all [--repeats 3] [--docker "sudo docker"]
    python bench/run.py --only E2/spark_hf100_1s_r1          # a single run
    python bench/run.py --plan e2 --list                     # show the matrix

Each run: stop engines and side services, recreate RAW + engine topics with
LogAppendTime, drop Spark checkpoints, start one engine (both for E1), drive the
producer, wait for outputs to settle, then collect + compare + latency into
bench/results/<experiment>/<run>/. A run with done.json is skipped, so an
interrupted plan continues where it stopped.
"""
import argparse
import json
import os
import platform
import shlex
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
RESULTS = PROJECT / "bench/results"
sys.path.insert(0, str(PROJECT / "bench"))
from collect import ENGINE_TOPICS, RAW_TOPICS, collect   # noqa: E402

START_TIME = "2026-10-16T20:00:00Z"
FLINK_JOB = "ICU business telemetry, 5m windows and 30s hypotension"
SIDE_SERVICES = ["icu-server", "icu-storage-writer", "icu-web", "icu-kafka-ui"]
ENGINE_SERVICES = {"flink": ["icu-flink-jobmanager", "icu-flink-taskmanager"], "spark": ["icu-spark"]}
ENGINE_CONTAINERS = {"flink": ["icu-flink-jobmanager", "icu-flink-taskmanager"], "spark": ["icu-spark"]}
GROUPS = ("icu-flink-v2", "icu-timescale-writer-v2", "icu-ui-gateway-v2", "icu-server-v2")
DLQ_TOPICS = {"icu.deadletter", "icu.alt.spark.deadletter"}
PROFILES = {
    "standard": ["--duration-minutes", "6", "--speed", "8"],           # E1: 6 min event time, 45 s wall
    "normal": ["--duration-minutes", "5", "--speed", "1"],             # 24 msg/s
    "hf100": ["--high-frequency", "100", "--duration-minutes", "5"],   # 2,400 msg/s
    "hf200": ["--high-frequency", "200", "--duration-minutes", "5"],   # 4,800 msg/s
}


def matrix(plan, repeats):
    runs = []
    for r in range(1, repeats + 1):
        if plan in ("e1", "all"):
            runs.append({"experiment": "E1", "engine": "both", "profile": "standard", "trigger": "default", "repeat": r})
        if plan in ("e2", "all"):
            for profile in ("normal", "hf100", "hf200"):
                runs.append({"experiment": "E2", "engine": "flink", "profile": profile, "trigger": "default", "repeat": r})
                for trigger in ("default", "1s", "5s"):
                    runs.append({"experiment": "E2", "engine": "spark", "profile": profile, "trigger": trigger, "repeat": r})
            # D10: the Kafka client default fetch wait, reported for transparency.
            runs.append({"experiment": "E2", "engine": "spark", "profile": "normal", "trigger": "default",
                         "fetch_wait_ms": 500, "repeat": r})
        if plan in ("e3", "all"):
            for engine in ("flink", "spark"):
                runs.append({"experiment": "E3", "engine": engine, "profile": "preload_hf200_10m", "trigger": "default",
                             "repeat": r})
        if plan in ("e4", "all"):
            for engine in ("flink", "spark"):
                runs.append({"experiment": "E4", "engine": engine, "profile": "hf100", "trigger": "default",
                             "kill_after_s": 150, "repeat": r})
    return runs


def run_name(spec):
    suffix = f"_fetch{spec['fetch_wait_ms']}" if spec.get("fetch_wait_ms") else ""
    return f"{spec['experiment']}/{spec['engine']}_{spec['profile']}_{spec['trigger']}{suffix}_r{spec['repeat']}"


class Bench:
    def __init__(self, docker, bootstrap="127.0.0.1:9092"):
        self.docker = shlex.split(docker)
        self.bootstrap = bootstrap
        self.compose = [*self.docker, "compose", "-f", str(PROJECT / "docker/compose.yaml"),
                        "-f", str(PROJECT / "docker/compose.bench.yaml"), "--env-file", str(PROJECT / ".env"),
                        "--profile", "spark"]

    def sh(self, command, check=True, env=None, **kwargs):
        return subprocess.run(command, check=check, env={**os.environ, **(env or {})}, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT, **kwargs)

    # ------------------------------------------------------------- lifecycle
    def stop_everything(self):
        self.sh([*self.compose, "stop", *SIDE_SERVICES, *ENGINE_SERVICES["flink"], *ENGINE_SERVICES["spark"]],
                check=False)
        self.sh([*self.compose, "rm", "-f", "icu-spark"], check=False)
        self.sh([*self.docker, "volume", "rm", "-f", "icu-spark-checkpoints", "icu-spark-metrics"], check=False)

    def reset_topics(self):
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": self.bootstrap})
        topics = [*RAW_TOPICS.values(), *[t for e in ENGINE_TOPICS.values() for t in e.values()]]
        existing = set(admin.list_topics(timeout=10).topics)
        targets = [t for t in topics if t in existing]
        for future in (admin.delete_topics(targets, operation_timeout=30).values() if targets else ()):
            future.result(60)
        deadline = time.monotonic() + 90
        while set(admin.list_topics(timeout=10).topics) & set(topics):
            if time.monotonic() > deadline:
                raise RuntimeError("Topics were not deleted")
            time.sleep(0.5)
        for group, future in admin.delete_consumer_groups(list(GROUPS)).items():
            try:
                future.result(15)
            except Exception:
                pass                                   # absent group
        config = {"message.timestamp.type": "LogAppendTime"}
        new = [NewTopic(t, num_partitions=1, replication_factor=1,
                        config=dict(config, **({"max.message.bytes": str(8 * 1024 * 1024)} if t in DLQ_TOPICS else {})))
               for t in topics]
        for future in admin.create_topics(new).values():
            future.result(30)
        while True:
            current = admin.list_topics(timeout=10).topics
            if all(t in current and not current[t].error and set(current[t].partitions) == {0} for t in topics):
                return topics
            if time.monotonic() > deadline:
                raise RuntimeError("Topics were not recreated")
            time.sleep(0.25)

    def flink_jobs(self):
        try:
            with urllib.request.urlopen("http://127.0.0.1:8081/jobs/overview", timeout=5) as response:
                return json.load(response)["jobs"]
        except OSError:
            return None

    def start_flink(self):
        self.sh([*self.compose, "up", "-d", "--no-deps", "--force-recreate", *ENGINE_SERVICES["flink"]])
        self.wait(lambda: self.flink_jobs() is not None, 180, "Flink REST")
        self.sh([*self.compose, "up", "--no-deps", "--force-recreate", "icu-flink-submit"])
        def running():
            jobs = self.flink_jobs() or []
            job = next((j for j in jobs if j["name"] == FLINK_JOB and j["state"] == "RUNNING"), None)
            if job is None:
                return False
            with urllib.request.urlopen(f"http://127.0.0.1:8081/jobs/{job['jid']}", timeout=5) as response:
                return all(v["status"] == "RUNNING" for v in json.load(response)["vertices"])
        self.wait(running, 180, "Flink job RUNNING")

    def compose_with_env(self, env):
        """`sudo` resets the environment, so compose interpolation variables go through `env`."""
        assignments = [f"{key}={value}" for key, value in env.items()]
        if self.docker[0] == "sudo":
            return ["sudo", "env", *assignments, *self.compose[1:]]
        return ["env", *assignments, *self.compose]

    def start_spark(self, spec):
        env = {"SPARK_TRIGGER": "" if spec["trigger"] == "default" else spec["trigger"].replace("s", " seconds"),
               "SPARK_KAFKA_FETCH_MAX_WAIT_MS": str(spec.get("fetch_wait_ms", 10))}
        self.sh([*self.compose_with_env(env), "up", "-d", "--no-deps", "icu-spark"])
        actual = dict(line.split("=", 1) for line in self.sh(
            [*self.docker, "inspect", "-f", "{{range .Config.Env}}{{println .}}{{end}}", "icu-spark"]
        ).stdout.splitlines() if "=" in line)
        mismatched = {key: actual.get(key) for key, value in env.items() if actual.get(key, "") != value}
        if mismatched:
            raise RuntimeError(f"icu-spark started with the wrong settings: {mismatched}, expected {env}")
        self.wait(lambda: self.sh([*self.docker, "inspect", "-f", "{{.State.Health.Status}}", "icu-spark"],
                                  check=False).stdout.strip() == "healthy", 300, "icu-spark healthy")
        return env

    def wait(self, predicate, timeout, what):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() > deadline:
                raise RuntimeError(f"Timed out waiting for {what}")
            time.sleep(1)

    def end_offsets(self, topics):
        from confluent_kafka import Consumer, TopicPartition
        consumer = Consumer({"bootstrap.servers": self.bootstrap, "group.id": "icu-bench-drain"})
        try:
            return {t: consumer.get_watermark_offsets(TopicPartition(t, 0), timeout=10)[1] for t in topics}
        finally:
            consumer.close()

    def restarts(self, containers):
        return {c: int(self.sh([*self.docker, "inspect", "-f", "{{.RestartCount}}", c], check=False).stdout.strip() or 0)
                for c in containers}

    def drain(self, topics, quiet_s, timeout_s, containers, allowed_restarts=None):
        """Wait until outputs exist and stop growing for quiet_s seconds.

        Gives up early when an engine container restarts on its own (e.g. OOM kill).
        """
        deadline, last, stable_since = time.monotonic() + timeout_s, None, time.monotonic()
        baseline = allowed_restarts or self.restarts(containers)
        while time.monotonic() < deadline:
            current = self.end_offsets(topics)
            if self.restarts(containers) != baseline:
                return {"drained": False, "reason": "engine container restarted", "end_offsets": current,
                        "restarts": self.restarts(containers)}
            if current != last:
                last, stable_since = current, time.monotonic()
            elif any(current.values()) and time.monotonic() - stable_since >= quiet_s:
                return {"drained": True, "end_offsets": current}
            time.sleep(2)
        return {"drained": False, "reason": "timeout", "end_offsets": last}

    # ------------------------------------------------------------------ run
    def produce(self, profile, out_dir):
        command = [sys.executable, str(PROJECT / "scripts/demo/run_icu_demo.py"), *PROFILES[profile],
                   "--start-time", START_TIME]
        with open(out_dir / "producer.log", "w") as log:
            return subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                    env={**os.environ, "PYTHONUNBUFFERED": "1"})

    def preload(self, out_dir):
        command = [sys.executable, str(PROJECT / "bench/preload.py"), "--frequency", "200", "--minutes", "10",
                   "--start-time", START_TIME, "--bootstrap", self.bootstrap]
        with open(out_dir / "preload.log", "w") as log:
            subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
        return json.loads((out_dir / "preload.log").read_text().strip().splitlines()[-1])

    def run(self, spec):
        out_dir = RESULTS / run_name(spec)
        if (out_dir / "done.json").exists():
            print(f"skip {run_name(spec)} (done)", flush=True)
            return
        out_dir.mkdir(parents=True, exist_ok=True)
        engines = ["flink", "spark"] if spec["engine"] == "both" else [spec["engine"]]
        timeline = {"started_at": now_ms()}
        print(f"=== {run_name(spec)}", flush=True)
        self.stop_everything()
        self.reset_topics()
        timeline["topics_reset"] = now_ms()
        if spec["experiment"] == "E3":
            timeline["preload"] = self.preload(out_dir)
            timeline["preload_done"] = now_ms()
        for engine in engines:
            if engine == "flink":
                self.start_flink()
            else:
                timeline["spark_env"] = self.start_spark(spec)
        timeline["engine_ready"] = now_ms()
        containers = [c for e in engines for c in ENGINE_CONTAINERS[e]]
        sampler = subprocess.Popen([sys.executable, str(PROJECT / "bench/resources.py"), "--out",
                                    str(out_dir / "resources.csv"), "--containers", ",".join(containers),
                                    "--docker", " ".join(self.docker)])
        try:
            if spec["experiment"] != "E3":
                producer = self.produce(spec["profile"], out_dir)
                timeline["producer_started"] = now_ms()
                if spec.get("kill_after_s"):
                    time.sleep(spec["kill_after_s"])
                    target = "icu-flink-taskmanager" if spec["engine"] == "flink" else "icu-spark"
                    timeline["killed"] = now_ms()
                    self.sh([*self.docker, "kill", target])
                    self.sh([*self.docker, "start", target])
                    timeline["restarted"] = now_ms()
                timeline["producer_exit"] = producer.wait()
                timeline["producer_done"] = now_ms()
            outputs = [t for e in engines for t in ENGINE_TOPICS[e].values()]
            quiet, limit = (60, 3 * 3600) if spec["experiment"] == "E3" else (20, 1800)
            timeline["drain"] = self.drain(outputs, quiet, limit, containers, self.restarts(containers))
            timeline["drained"] = now_ms()
        finally:
            sampler.terminate()
            sampler.wait(10)
        metadata = collect(out_dir, self.bootstrap, tuple(engines),
                           " ".join(self.docker) if "spark" in engines else None)
        for engine in engines:
            self.sh([*self.compose, "stop", *ENGINE_SERVICES[engine]], check=False)
        from compare import evaluate as correctness
        from latency import evaluate as latency
        result = {"spec": spec, "timeline": timeline, "collect": metadata,
                  "correctness": correctness(out_dir), "latency": latency(out_dir), "machine": machine()}
        (out_dir / "run.json").write_text(json.dumps(result, indent=2, default=str))
        (out_dir / "done.json").write_text(json.dumps({"finished_at": now_ms()}))
        print(f"--- done {run_name(spec)}", flush=True)


def now_ms():
    return int(time.time() * 1000)


def machine():
    def read(command):
        try:
            return subprocess.run(command, text=True, capture_output=True, check=False).stdout.strip()
        except OSError:
            return None
    model = next((line.split(":", 1)[1].strip() for line in (read(["lscpu"]) or "").splitlines()
                  if line.startswith("Model name")), None)
    memory = next((line.split()[1] for line in Path("/proc/meminfo").read_text().splitlines()
                   if line.startswith("MemTotal")), None) if Path("/proc/meminfo").exists() else None
    return {"cpu": model, "cores": os.cpu_count(), "mem_total_kb": memory, "os": platform.platform(),
            "docker": read(["sudo", "-n", "docker", "version", "--format", "{{.Server.Version}}"]),
            "recorded_at": datetime.now(timezone.utc).isoformat()}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--plan", choices=("e1", "e2", "e3", "e4", "all"))
    parser.add_argument("--only", help="Run one run name, e.g. E2/spark_hf100_1s_r1")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--docker", default="docker")
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    runs = matrix(args.plan or "all", args.repeats)
    if args.only:
        runs = [spec for spec in runs if run_name(spec) == args.only]
        if not runs:
            parser.error(f"Unknown run {args.only}")
    if args.list:
        print("\n".join(run_name(spec) + (" (done)" if (RESULTS / run_name(spec) / "done.json").exists() else "")
                        for spec in runs))
        return
    bench = Bench(args.docker)
    for spec in runs:
        try:
            bench.run(spec)
        except Exception as error:                 # keep going; the run stays incomplete and is retried later
            print(f"!!! {run_name(spec)} failed: {error}", flush=True)
            (RESULTS / run_name(spec)).mkdir(parents=True, exist_ok=True)
            (RESULTS / run_name(spec) / "error.txt").write_text(f"{now_ms()} {error}\n")


if __name__ == "__main__":
    main()
