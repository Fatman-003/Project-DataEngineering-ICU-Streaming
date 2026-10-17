"""Windows one-command ICU demo lifecycle. Mutations are restricted to this project."""
import argparse
import contextlib
import ctypes
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import json
import math
import ntpath
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "jobs/flink"))
sys.path.insert(0, str(PROJECT / "services/storage_writer"))
from registry import load_patients
from kafka_topics import reset_demo_topics, topic_offsets
from contracts import CORE_TOPICS
from run_icu_demo import run_demo

V2_JOB_NAME = "ICU V2 telemetry, 15m statistics and sustained hypotension"
TERMINAL_STATES = {"CANCELED", "FINISHED", "FAILED", "SUSPENDED"}
SERVICES = {
    "icu-flink-jobmanager": ("jobmanager", "icu-pyflink:2.3.0-kafka5.0.0-2.2-v2"),
    "icu-flink-taskmanager": ("taskmanager", "icu-pyflink:2.3.0-kafka5.0.0-2.2-v2"),
    "icu-kafka-ui": ("kafka-ui", "provectuslabs/kafka-ui:v0.7.2"),
    "icu-realtime-gateway": ("realtime-gateway", "icu-realtime-gateway:v2"),
    "icu-timescaledb": ("timescaledb", "timescale/timescaledb:2.30.2-pg17"),
    "icu-storage-writer": ("storage-writer", "icu-storage-writer:v1"),
}
PORT_OWNERS = {9092: "icu-kafka", 8081: "icu-flink-jobmanager",
               8082: "icu-kafka-ui", 8083: "icu-realtime-gateway", 5432: "icu-timescaledb"}


def normalized(path):
    return ntpath.normcase(ntpath.normpath(str(path)))


def project_container(container, project=PROJECT):
    name = container.get("Name", "").lstrip("/")
    config = container.get("Config", {})
    if name == "icu-kafka":
        return config.get("Image") == "apache/kafka:4.2.2"
    if name not in SERVICES:
        return False
    service, image = SERVICES[name]
    labels = config.get("Labels") or {}
    return (config.get("Image") == image
            and labels.get("com.docker.compose.project") == "icu-flink"
            and labels.get("com.docker.compose.service") == service
            and normalized(labels.get("com.docker.compose.project.config_files", ""))
            == normalized(project / "docker/flink/compose.yaml"))


def project_jobs(jobs):
    return [job for job in jobs if job.get("name") == V2_JOB_NAME
            and job.get("state") not in TERMINAL_STATES]


def windows_argv(command):
    if not command:
        return []
    count = ctypes.c_int()
    function = ctypes.windll.shell32.CommandLineToArgvW
    function.argtypes = [ctypes.c_wchar_p, ctypes.POINTER(ctypes.c_int)]
    function.restype = ctypes.POINTER(ctypes.c_wchar_p)
    pointer = function(command, ctypes.byref(count))
    if not pointer:
        return []
    try:
        return [pointer[index] for index in range(count.value)]
    finally:
        free = ctypes.windll.kernel32.LocalFree
        free.argtypes = [ctypes.c_void_p]
        free(pointer)


def project_process(process, project=PROJECT):
    if (process.get("Name") or "").lower() not in {"python.exe", "pythonw.exe"}:
        return False
    arguments = process.get("Arguments")
    if arguments is None:
        arguments = windows_argv(process.get("CommandLine"))
    scripts = {normalized(project / "scripts/demo/run_icu_demo.py"),
               normalized(project / "services/realtime_gateway/gateway.py")}
    # Inspect Python's executable script position, never a data argument to another app.
    index = 1
    while index < len(arguments):
        argument = arguments[index]
        if argument in {"-c", "-m"}:
            return False
        if argument == "--":
            index += 1
            break
        if argument in {"-W", "-X"}:
            index += 2
            continue
        if argument in {"-u", "-B", "-E", "-I", "-O", "-OO", "-s", "-S", "-q", "-x"} or argument.startswith(("-W", "-X")):
            index += 1
            continue
        break
    return index < len(arguments) and normalized(arguments[index]) in scripts


def docker_proxy(process, docker_path):
    name = (process.get("Name") or "").lower()
    path = normalized(process.get("Path") or "")
    if name in {"com.docker.backend.exe", "com.docker.proxy.exe", "docker-proxy.exe"}:
        return any(path.startswith(normalized(candidate.parent) + "\\")
                   for candidate in desktop_candidates(docker_path))
    wsl_root = normalized(Path(os.environ.get("ProgramFiles", r"C:\Program Files")) / "WSL")
    return name == "wslrelay.exe" and path.startswith(wsl_root + "\\")


def check_ports(containers, listeners, docker_path, project=PROJECT):
    """Return confidently owned host PIDs to stop; refuse unknown owners first."""
    mappings = {}
    for container in containers:
        if not container.get("State", {}).get("Running"):
            continue
        for bindings in container.get("NetworkSettings", {}).get("Ports", {}).values():
            for binding in bindings or ():
                mappings.setdefault(int(binding["HostPort"]), []).append(container)
    owned = set()
    for listener in listeners:
        port = int(listener["Port"])
        if port not in PORT_OWNERS:
            continue
        candidates = mappings.get(port, [])
        expected = PORT_OWNERS[port]
        if any(c.get("Name", "").lstrip("/") != expected or not project_container(c, project)
               for c in candidates):
            raise RuntimeError(f"Port {port} is published by a non-project container; PID {listener['Pid']}, process {listener.get('Name', 'unknown')}")
        if candidates and docker_proxy(listener, docker_path):
            continue
        if port == 8083 and project_process(listener, project):
            owned.add(int(listener["Pid"]))
            continue
        raise RuntimeError(f"Port {port} is occupied by non-project process: PID {listener['Pid']}, "
                           f"Process: {listener.get('Name') or 'unknown'}. Cannot safely reset this port automatically.")
    return owned


def desktop_candidates(docker_path, environment=None):
    environment = os.environ if environment is None else environment
    result = [Path(environment.get("ProgramFiles", r"C:\Program Files")) / "Docker/Docker/Docker Desktop.exe"]
    if len(Path(docker_path).parents) >= 3:
        result.insert(0, Path(docker_path).parents[2] / "Docker Desktop.exe")
    if environment.get("LOCALAPPDATA"):
        result.append(Path(environment["LOCALAPPDATA"]) / "Docker/Docker Desktop.exe")
    if os.name == "nt":
        import winreg
        for hive in (winreg.HKEY_CURRENT_USER, winreg.HKEY_LOCAL_MACHINE):
            try:
                with winreg.OpenKey(hive, r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\Docker Desktop.exe") as key:
                    result.append(Path(winreg.QueryValue(key, None).strip('"')))
            except OSError:
                pass
    return list(dict.fromkeys(result))


def fresh_snapshot(snapshot):
    patients = load_patients()
    return (set(snapshot) == {p["demo_id"] for p in patients}
            and all(all(snapshot[p["demo_id"]].get(key) == p[key] for key in p)
                    and all(snapshot[p["demo_id"]].get(key) in ({}, [])
                            for key in ("vitals", "summaries", "alerts")) for p in patients))


@contextlib.contextmanager
def demo_lock():
    """A named OS mutex disappears on exit/crash; no runtime files or stale locks."""
    name = "Local\\ICU-room-demo-" + hashlib.sha256(normalized(PROJECT).encode()).hexdigest()[:24]
    kernel = ctypes.windll.kernel32
    kernel.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p]
    kernel.CreateMutexW.restype = ctypes.c_void_p
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    kernel.ReleaseMutex.argtypes = [ctypes.c_void_p]
    handle = kernel.CreateMutexW(None, True, name)
    if not handle:
        raise RuntimeError("Could not acquire the project demo mutex")
    if kernel.GetLastError() == 183:
        kernel.CloseHandle(handle)
        raise RuntimeError("An ICU orchestrator is already running. Let it finish or press Ctrl+C in its console before starting another.")
    try:
        yield
    finally:
        kernel.ReleaseMutex(handle)
        kernel.CloseHandle(handle)


class Orchestrator:
    def __init__(self, args):
        self.args, self.step = args, "Prerequisites"
        self.docker = shutil.which("docker")
        self.python = PROJECT / ".venv/Scripts/python.exe"
        self.compose = str(PROJECT / "docker/flink/compose.yaml")
        self.report = {}
        self.step_started = time.monotonic()

    def command(self, *args, timeout=90, check=True, show=False):
        result = subprocess.run([str(arg) for arg in args], cwd=PROJECT, text=True,
                                capture_output=True, timeout=timeout, encoding="utf-8", errors="replace")
        if show:
            print(result.stdout.strip(), flush=True)
        if check and result.returncode:
            raise RuntimeError(f"Command failed ({result.returncode}): {result.stderr[-2000:] or result.stdout[-2000:]}")
        return result

    def compose_command(self, *args, **kwargs):
        return self.command(self.docker, "compose", "-f", self.compose,
                            "--env-file", PROJECT / ".env", *args, **kwargs)

    def mark(self, step):
        now = time.monotonic()
        self.report.setdefault("step_seconds", {})[self.step] = round(now - self.step_started, 3)
        self.step_started = now
        self.step = step
        print(f"[WAIT] {step}...", flush=True)

    def wait(self, predicate, timeout, failure):
        deadline = time.monotonic() + timeout
        while True:
            if predicate():
                return
            if time.monotonic() >= deadline:
                raise RuntimeError(failure)
            time.sleep(0.2 if self.args.fast_verify else 1)

    def http(self, path, base="http://127.0.0.1:8081"):
        with urllib.request.urlopen(base + path, timeout=5) as response:
            return json.load(response)

    def reachable(self, path, base):
        try:
            with urllib.request.urlopen(base + path, timeout=5) as response:
                return response.status == 200
        except (OSError, urllib.error.URLError):
            return False

    def prerequisites(self):
        if os.name != "nt":
            raise RuntimeError("run_icu_demo.ps1 requires Windows and Docker Desktop")
        if not self.docker or not self.python.is_file():
            raise RuntimeError("Install Docker Desktop and create .venv with requirements.txt first")
        for package in ("confluent_kafka", "aiohttp", "psycopg"):
            if importlib.util.find_spec(package) is None:
                raise RuntimeError(f"Missing {package}; install requirements.txt in .venv")
        load_patients()

    def ensure_docker(self):
        self.mark("Docker engine readiness")
        def available():
            try:
                return self.command(self.docker, "info", "--format", "{{.ServerVersion}}", timeout=15, check=False).returncode == 0
            except subprocess.TimeoutExpired:
                return False
        if not available():
            if self.args.skip_docker_start:
                raise RuntimeError("Docker is unavailable and -SkipDockerStart was set")
            desktop = next((path for path in desktop_candidates(self.docker) if path.is_file()), None)
            if desktop is None:
                raise RuntimeError("Docker Desktop.exe was not found. Install Docker Desktop or make its docker CLI available on PATH.")
            print(f"[WAIT] Starting Docker Desktop: {desktop}", flush=True)
            startup = subprocess.STARTUPINFO()
            startup.dwFlags |= subprocess.STARTF_USESHOWWINDOW
            startup.wShowWindow = 0
            subprocess.Popen([str(desktop)], startupinfo=startup,
                             creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP)
            print("[WAIT] Waiting for Docker engine...", flush=True)
            self.wait(available, 240, "Docker engine did not become ready within four minutes; inspect Docker Desktop's startup error.")
        self.command(self.docker, "compose", "version")
        print("[OK] Docker engine ready", flush=True)

    def containers(self):
        identifiers = self.command(self.docker, "ps", "-aq").stdout.split()
        return json.loads(self.command(self.docker, "inspect", *identifiers).stdout) if identifiers else []

    def powershell_json(self, source):
        result = self.command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
                              "$ErrorActionPreference = 'Stop'; " + source)
        return json.loads(result.stdout or "[]")

    def listeners(self):
        return self.powershell_json("""
        $rows = @(Get-NetTCPConnection -State Listen | Where-Object { $_.LocalPort -in @(9092,8081,8082,8083,5432) } | ForEach-Object {
            $proc = Get-CimInstance Win32_Process -Filter ("ProcessId = " + $_.OwningProcess) -ErrorAction SilentlyContinue
            [PSCustomObject]@{Port=$_.LocalPort; Address=$_.LocalAddress; Pid=$_.OwningProcess;
                Name=$proc.Name; Path=$proc.ExecutablePath; CommandLine=$proc.CommandLine}
        }); ConvertTo-Json -InputObject $rows -Compress
        """)

    def processes(self):
        return self.powershell_json("""
        $rows = @(Get-CimInstance Win32_Process -Filter "Name = 'python.exe' OR Name = 'pythonw.exe'" | ForEach-Object {
            [PSCustomObject]@{Pid=$_.ProcessId; Name=$_.Name; Path=$_.ExecutablePath; CommandLine=$_.CommandLine}
        }); ConvertTo-Json -InputObject $rows -Compress
        """)

    def inspect_ownership(self):
        self.mark("Project ownership and required ports")
        containers = self.containers()
        names = {c["Name"].lstrip("/"): c for c in containers}
        if "icu-kafka" not in names:
            raise RuntimeError("Existing icu-kafka container is missing. Refusing to create or replace the broker.")
        for name, container in names.items():
            if name in {*SERVICES, "icu-kafka"} and not project_container(container):
                raise RuntimeError(f"Container {name} does not match this project's image/Compose ownership")
        check_ports(containers, self.listeners(), self.docker)
        self.report["broker_id_before"] = names["icu-kafka"]["Id"]
        manager = names.get("icu-flink-jobmanager")
        jobs = []
        if manager and manager["State"]["Running"]:
            def rest_ready():
                return self.reachable("/jobs/overview", "http://127.0.0.1:8081")
            self.wait(rest_ready, 60, "Running JobManager is not inspectable; refusing to reset potentially unrelated jobs")
            jobs = self.http("/jobs/overview")["jobs"]
        foreign = [j for j in jobs if j["state"] not in TERMINAL_STATES and j["name"] != V2_JOB_NAME]
        if foreign:
            raise RuntimeError("Unrelated Flink jobs are active; they will not be canceled or displaced: "
                               + ", ".join(j["name"] for j in foreign))
        return jobs

    def stop_owned_producers(self):
        self.mark("Stop stale project producer/host gateway processes")
        for process in self.processes():
            if not project_process(process) or int(process["Pid"]) == os.getpid():
                continue
            current = next((p for p in self.processes() if p["Pid"] == process["Pid"]), None)
            if current is None:
                continue
            if current["CommandLine"] != process["CommandLine"] or not project_process(current):
                raise RuntimeError("Process identity changed during inspection; refusing to stop it")
            pid = int(process["Pid"])
            self.command("powershell.exe", "-NoProfile", "-NonInteractive", "-Command",
                         f"Stop-Process -Id {pid} -ErrorAction Stop")
            print(f"[OK] Stopped project process PID {pid}", flush=True)

    def ensure_local_env(self):
        self.mark("Local database configuration")
        target = PROJECT / ".env"
        defaults = {key: value for line in (PROJECT / ".env.example").read_text(encoding="utf-8").splitlines()
                    if line and not line.startswith("#") and "=" in line
                    for key, value in [line.split("=", 1)]}
        existing = {}
        if target.exists():
            existing = {key.strip(): value.strip() for line in target.read_text(encoding="utf-8-sig").splitlines()
                        if line and not line.lstrip().startswith("#") and "=" in line
                        for key, value in [line.split("=", 1)]}
        missing = {key: value for key, value in defaults.items() if key not in existing}
        if missing:
            with target.open("a", encoding="utf-8") as stream:
                stream.write("\n" + "\n".join(f"{key}={value}" for key, value in missing.items()) + "\n")
        if not all((existing.get(key) or missing.get(key)) for key in defaults):
            raise RuntimeError("Local .env has an empty database setting")
        self.report["database_env_created_or_extended"] = bool(missing)

    def cancel_jobs(self, jobs):
        self.mark("Cancel previous ICU V2 Flink jobs")
        selected = project_jobs(jobs)
        for job in selected:
            if not re.fullmatch(r"[0-9a-f]{32}", job["jid"]):
                raise RuntimeError("Invalid Flink job identity")
            request = urllib.request.Request(f"http://127.0.0.1:8081/jobs/{job['jid']}?mode=cancel", method="PATCH")
            with urllib.request.urlopen(request, timeout=10):
                pass
        if selected:
            self.wait(lambda: not project_jobs(self.http("/jobs/overview")["jobs"]), 90,
                      "Previous V2 jobs did not fully terminate")
        self.report["canceled_job_ids"] = [j["jid"] for j in selected]

    def ensure_infrastructure(self):
        self.mark("Preserve and start the existing Kafka broker")
        self.command(self.python, "-B", PROJECT / "docker/flink/prepare_kafka.py", show=True)
        self.mark("Flink infrastructure and images")
        missing = [image for image in (SERVICES["icu-flink-jobmanager"][1], SERVICES["icu-realtime-gateway"][1])
                   if self.command(self.docker, "image", "inspect", image, check=False).returncode]
        if missing:
            if self.args.skip_build:
                raise RuntimeError("Required images are missing and -SkipBuild was set: " + ", ".join(missing))
            self.compose_command("build", "jobmanager", "realtime-gateway", timeout=1200, show=True)
        elif self.args.fast_verify and not self.args.skip_build:
            # COPY'd gateway code must match the source under verification.
            self.compose_command("build", "realtime-gateway", timeout=1200)
        self.compose_command("up", "-d", "--no-build", "jobmanager", "taskmanager", timeout=180)
        def slots_ready():
            try:
                overview = self.http("/overview")
                return overview["taskmanagers"] >= 1 and overview["slots-available"] >= 1
            except (OSError, urllib.error.URLError):
                return False
        self.wait(slots_ready, 120, "Flink TaskManager/slot is unavailable; no simulator was started")

    def ensure_database_images(self):
        image = SERVICES["icu-timescaledb"][1]
        if self.command(self.docker, "image", "inspect", image, check=False).returncode:
            self.mark("Pulling TimescaleDB")
            self.compose_command("pull", "timescaledb", timeout=1800, show=True)
        print("[OK] TimescaleDB image ready", flush=True)
        writer_image = SERVICES["icu-storage-writer"][1]
        if self.args.skip_build:
            if self.command(self.docker, "image", "inspect", writer_image, check=False).returncode:
                raise RuntimeError("Storage Writer image missing and -SkipBuild was set")
        else:
            self.mark("Build Storage Writer image")
            self.compose_command("build", "storage-writer", timeout=1200)
        print("[OK] Storage Writer image ready", flush=True)

    def start_database(self):
        self.mark("TimescaleDB health, schema and reference seed")
        self.compose_command("up", "-d", "--no-build", "timescaledb", timeout=180)
        def db_healthy():
            result = self.command(self.docker, "inspect", "--format", "{{.State.Health.Status}}",
                                  "icu-timescaledb", check=False)
            return result.returncode == 0 and result.stdout.strip() == "healthy"
        self.wait(db_healthy, 120, "TimescaleDB did not pass pg_isready healthcheck")
        from db_admin import initialize, connect
        self.report["database"] = initialize(keep=self.args.keep_database_data)
        if self.args.keep_database_data:
            with connect() as connection:
                latest = connection.execute("SELECT max(event_time) FROM chartevents").fetchone()[0]
            if latest is not None:
                self.report["retained_event_start_after"] = latest.isoformat()

    def start_writer(self):
        self.mark("Storage Writer readiness and independent Kafka subscription")
        self.compose_command("up", "-d", "--no-deps", "--no-build", "storage-writer", timeout=180)
        def healthy():
            result = self.command(self.docker, "inspect", "--format", "{{.State.Health.Status}}",
                                  "icu-storage-writer", check=False)
            return result.returncode == 0 and result.stdout.strip() == "healthy"
        self.wait(healthy, 90, "Storage Writer did not become healthy and join its business-topic group")
        from confluent_kafka.admin import AdminClient
        from config import GROUP_ID, TOPIC_TABLE
        admin = AdminClient({"bootstrap.servers": "localhost:9092"})
        state = {"reason": "group has not finished joining"}
        def group_ready():
            try:
                group = admin.describe_consumer_groups([GROUP_ID])[GROUP_ID].result(5)
                assigned = ({p.topic for p in group.members[0].assignment.topic_partitions}
                            if len(group.members) == 1 else set())
                state["members"], state["topics"] = len(group.members), assigned
                return len(group.members) == 1 and assigned == set(TOPIC_TABLE)
            except Exception as error:
                state["reason"] = repr(error)
                return False
        self.wait(group_ready, 60, f"Storage Writer group did not become stable: {state}")
        self.report["storage_writer"] = {"consumer_group": GROUP_ID, "topics": sorted(state["topics"]),
                                         "members": state["members"], "healthy": True}

    def submit_job(self):
        self.mark("Submit exactly one fresh V2 Flink job")
        if project_jobs(self.http("/jobs/overview")["jobs"]):
            raise RuntimeError("A V2 job appeared during reset; refusing duplicate submission")
        options = ["--checkpoint-interval-ms", "2000"] if self.args.fast_verify else []
        output = self.compose_command("exec", "-T", "jobmanager", "/opt/flink/bin/flink", "run", "-d",
                                      "-py", "/opt/flink/jobs/runtime_pipeline.py", "--offset", "earliest", *options).stdout
        matched = re.search(r"JobID\s+([0-9a-f]{32})", output)
        if not matched:
            raise RuntimeError("Flink did not return a fresh job ID")
        job_id = matched.group(1)
        def running():
            jobs = project_jobs(self.http("/jobs/overview")["jobs"])
            if len(jobs) > 1:
                raise RuntimeError("More than one V2 job is active")
            details = self.http(f"/jobs/{job_id}")
            if details["state"] in TERMINAL_STATES:
                raise RuntimeError(f"Fresh Flink job terminated: {details['state']}")
            checkpoints = self.http(f"/jobs/{job_id}/checkpoints")
            return (len(jobs) == 1 and details["state"] == "RUNNING"
                    and bool(details["vertices"]) and all(v["status"] == "RUNNING" for v in details["vertices"])
                    and checkpoints["counts"]["completed"] > 0)
        self.wait(running, 180, "Fresh Flink job did not reach RUNNING with a completed checkpoint")
        self.report["job_id"] = job_id

    def start_gateway_and_ui(self):
        self.compose_command("up", "-d", "--no-deps", "--no-build", "realtime-gateway", "kafka-ui", timeout=240)

    def reset_gateway_and_ui(self, started=False):
        self.mark("Fresh Gateway and Kafka UI readiness")
        if not started:
            self.start_gateway_and_ui()
        def gateway_ready():
            try:
                return self.http("/api/health", "http://127.0.0.1:8083").get("kafka_connected", False)
            except (OSError, urllib.error.URLError):
                return False
        self.wait(gateway_ready, 90, "Gateway did not become healthy at localhost:8083")
        self.wait(lambda: self.reachable("/", "http://127.0.0.1:8082"), 90, "Kafka UI did not become reachable at localhost:8082")
        clusters = self.http("/api/clusters", "http://127.0.0.1:8082")
        if not any(c["name"] == "ICU-LOCAL" and c["status"] == "online" for c in clusters):
            raise RuntimeError("Kafka UI cluster ICU-LOCAL is not online")
        snapshot = self.http("/api/snapshot", "http://127.0.0.1:8083")
        if not fresh_snapshot(snapshot):
            raise RuntimeError("Gateway still contains previous demo values or alerts")
        self.report["initial_snapshot"] = snapshot

    def prepare(self):
        self.prerequisites()
        self.ensure_docker()
        jobs = self.inspect_ownership()
        self.ensure_local_env()
        self.stop_owned_producers()
        containers = {c["Name"].lstrip("/"): c for c in self.containers()}
        for name in ("icu-realtime-gateway", "icu-storage-writer"):
            if name in containers and containers[name]["State"]["Running"]:
                self.command(self.docker, "stop", "--time", "15", name)
        self.cancel_jobs(jobs)
        self.ensure_infrastructure()
        self.ensure_database_images()
        self.start_database()
        self.mark("Reset only the seven V2 topics and three project consumer groups")
        self.report["reset"] = reset_demo_topics()
        if self.args.fast_verify:
            # Both services start against empty topics while Flink boots. The
            # producer still waits for checkpoint, consumer and snapshot checks.
            self.start_gateway_and_ui()
        self.submit_job()
        self.reset_gateway_and_ui(started=self.args.fast_verify)
        self.start_writer()
        offsets = topic_offsets(CORE_TOPICS)
        if offsets != {topic: {"0": [0, 0]} for topic in CORE_TOPICS}:
            raise RuntimeError("Fresh topics received unexpected messages before simulator startup")
        broker = next(c for c in self.containers() if c["Name"] == "/icu-kafka")
        self.report["broker_id_after"] = broker["Id"]
        if broker["Id"] != self.report["broker_id_before"]:
            raise RuntimeError("Existing Kafka broker identity changed")
        self.report["ready"] = True
        self.save_report()
        print("\n============================================================\nICU STREAMING SYSTEM READY\n"
              "============================================================\nKafka Broker: localhost:9092 READY\n"
              "Flink UI: http://localhost:8081 READY\nKafka UI: http://localhost:8082 READY\n"
              "Realtime Gateway: http://localhost:8083 READY\n"
              "TimescaleDB: localhost:5432 READY (9 tables / 4 hypertables)\n"
              "Storage Writer: icu-timescale-writer-v2 READY\n"
              "Gateway Snapshot: http://localhost:8083/api/snapshot\nKafka V2 topics: 7 / 7 READY (empty)\n"
              "Flink V2 job: RUNNING, checkpoint completed\nPrevious demo state: "
              + ("KEPT" if self.args.keep_database_data else "RESET") + "\n"
              "Existing Kafka broker: PRESERVED\n============================================================", flush=True)

    def save_report(self):
        if self.args.state_report:
            Path(self.args.state_report).write_text(json.dumps(self.report, indent=2), encoding="utf-8")

    def run(self):
        invocation_began = time.monotonic()
        try:
            with demo_lock():
                self.prepare()
                self.report["startup_seconds"] = time.monotonic() - invocation_began
                if self.args.fast_verify:
                    from fast_verify import verify, SPEED
                    self.mark(f"Fast end-to-end verification ({SPEED}x, unchanged clinical rule)")
                    self.report["verification"] = verify()
                    from db_verify import verify_database
                    self.report["database_verification"] = verify_database()
                    self.report.update(outcome="verified", total_seconds=time.monotonic() - invocation_began)
                    self.save_report()
                    print(json.dumps({"FAST ALERT VERIFICATION": self.report["verification"],
                                      "DATABASE VERIFICATION": self.report["database_verification"],
                                      "startup_seconds": self.report["startup_seconds"],
                                      "total_seconds": self.report["total_seconds"]}, indent=2), flush=True)
                    return 0
                self.mark("Realtime simulator")
                print(f"Starting {self.args.duration_minutes:g}-minute realtime ICU simulation "
                      f"at {self.args.interval_seconds:g}s intervals...", flush=True)
                began = time.monotonic()
                start_override = None
                if self.args.keep_database_data and self.report.get("retained_event_start_after"):
                    retained = datetime.fromisoformat(self.report["retained_event_start_after"])
                    start_override = max(datetime.now(timezone.utc).replace(microsecond=0),
                                         retained + timedelta(seconds=1))
                start = run_demo(duration_minutes=self.args.duration_minutes,
                                 interval_seconds=self.args.interval_seconds, loop=self.args.loop,
                                 raw=self.args.raw, start=start_override)
                self.report.update(outcome="completed", producer_started=True,
                                   event_start=start.isoformat(), elapsed_seconds=time.monotonic() - began)
                self.save_report()
                print("[OK] Demo finished. Kafka, Flink, Kafka UI and Gateway remain available.", flush=True)
                return 0
        except KeyboardInterrupt:
            self.report["outcome"] = "interrupted"
            self.save_report()
            print("\n[INFO] Producer stopped. Infrastructure remains running for inspection.", flush=True)
            return 130
        except Exception as error:
            self.report.update(outcome="failed", failed_step=self.step, error=str(error))
            self.save_report()
            print(f"[ERROR] {self.step}: {error}", file=sys.stderr, flush=True)
            return 1


def positive(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return number


def arguments(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration-minutes", type=positive, default=6)
    parser.add_argument("--interval-seconds", type=positive, default=1)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--raw", action="store_true")
    parser.add_argument("--fast-verify", action="store_true", help="Six logical minutes at 24x with live Kafka/WebSocket assertions")
    parser.add_argument("--keep-database-data", action="store_true", help="Retain the four business tables across normal demo resets")
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument("--skip-docker-start", action="store_true")
    parser.add_argument("--state-report", help="Optional verification output; normal runs create no report files")
    args = parser.parse_args(argv)
    if args.fast_verify and (args.duration_minutes != 6 or args.interval_seconds != 1 or args.loop or args.raw or args.keep_database_data):
        parser.error("FastVerify uses a clean fixed six-minute scenario without Loop, Raw, or KeepDatabaseData")
    return args


if __name__ == "__main__":
    raise SystemExit(Orchestrator(arguments()).run())
