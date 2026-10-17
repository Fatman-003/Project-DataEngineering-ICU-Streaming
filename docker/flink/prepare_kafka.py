"""Attach the existing single Kafka broker and add a Docker-only listener.

Run with the existing host .venv. Never removes/recreates Kafka or its data.
"""
import json
import re
import subprocess
import sys
import time
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT / "scripts/demo"))
from kafka_topics import ensure_topics
from confluent_kafka.admin import AdminClient

CONTAINER = "icu-kafka"
NETWORK = "icu-streaming"
PROPERTIES = {
    "listeners": "PLAINTEXT://:9092,CONTROLLER://:9093,INTERNAL://:9094",
    "advertised.listeners": "PLAINTEXT://localhost:9092,INTERNAL://icu-kafka:9094",
    "listener.security.protocol.map": (
        "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,SSL:SSL,"
        "SASL_PLAINTEXT:SASL_PLAINTEXT,SASL_SSL:SASL_SSL,INTERNAL:PLAINTEXT"),
}


def docker(*arguments, input=None, check=True):
    return subprocess.run(["docker", *arguments], input=input, text=True,
                          capture_output=True, check=check, timeout=90)


def properties(text):
    return {key.strip(): value.strip() for line in text.splitlines()
            if line.strip() and not line.lstrip().startswith("#")
            for key, sep, value in [line.partition("=")] if sep}


def updated_properties(text):
    values = properties(text)
    if values.get("node.id") != "1" or values.get("inter.broker.listener.name") != "PLAINTEXT":
        raise ValueError("Unexpected broker identity/inter-broker listener; refusing to overwrite")
    expected = {
        "listeners": "PLAINTEXT://:9092,CONTROLLER://:9093",
        "advertised.listeners": "PLAINTEXT://localhost:9092",
    }
    for key, original in expected.items():
        if values.get(key) not in (original, PROPERTIES[key]):
            raise ValueError(f"Unexpected {key}; refusing to replace another listener setup")
    for key, desired in PROPERTIES.items():
        # This script owns only the three documented listener settings.
        original = values.get(key)
        if original is None:
            raise ValueError(f"Missing broker setting: {key}")
        text = re.sub(r"^" + re.escape(key) + r"=.*$", key + "=" + desired,
                      text, flags=re.MULTILINE)
    return text


def main():
    try:
        inspected = json.loads(docker("inspect", CONTAINER).stdout)[0]
        if inspected["Config"]["Image"] != "apache/kafka:4.2.2":
            raise ValueError("Expected the existing apache/kafka:4.2.2 broker")
        if not inspected["State"]["Running"]:
            docker("start", CONTAINER)
        if docker("network", "inspect", NETWORK, check=False).returncode:
            docker("network", "create", NETWORK)
        if NETWORK not in inspected["NetworkSettings"]["Networks"]:
            docker("network", "connect", "--alias", CONTAINER, NETWORK, CONTAINER)
        original = docker("exec", CONTAINER, "cat", "/opt/kafka/config/server.properties").stdout
        updated = updated_properties(original)
        changed = properties(original) != properties(updated)
        output = PROJECT / "data/working/operations"
        output.mkdir(parents=True, exist_ok=True)
        backup = output / "kafka_server_properties.before.txt"
        if not backup.exists():
            backup.write_text(original, encoding="utf-8")
        if changed:
            # Kafka's startup wrapper reads this existing config volume on restart.
            # Preserve every other active property, including original log.dirs.
            docker("exec", "-i", "-u", "root", CONTAINER, "sh", "-c",
                   "cat > /mnt/shared/config/server.properties", input=updated)
            print("Adding INTERNAL listener on 9094; restarting the SAME Kafka container.",
                  flush=True)
            docker("restart", "--time", "30", CONTAINER)
        admin = AdminClient({"bootstrap.servers": "localhost:9092",
                             "log_level": 0, "socket.timeout.ms": 3000})
        deadline = time.monotonic() + 60
        while True:
            try:
                metadata = admin.list_topics(timeout=3)
                if metadata.brokers.get(1) is not None:
                    break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
            time.sleep(0.5)
        after = json.loads(docker("inspect", CONTAINER).stdout)[0]
        if after["Id"] != inspected["Id"]:
            raise ValueError("Broker container identity changed unexpectedly")
        topics = ensure_topics("localhost:9092")
        print(json.dumps({"container_id_unchanged": True, "restarted": changed,
                          "network": NETWORK, "host_bootstrap": "localhost:9092",
                          "flink_bootstrap": "icu-kafka:9094", "topics": topics}, indent=2))
        return 0
    except Exception as error:
        if isinstance(error, subprocess.CalledProcessError):
            print(error.stderr, file=sys.stderr)
        print(f"Kafka network preparation failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
