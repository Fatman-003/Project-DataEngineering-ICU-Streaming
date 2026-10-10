"""Shared literal .env/environment settings; no new dependencies."""
import os
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[2]
TOPICS = {
    "vitals": "icu.v1.vitals.numeric",
    "clinical": "icu.v1.clinical.events",
    "waveform": "icu.v1.vitals.waveform",
}


def settings():
    values = {}
    path = PROJECT / ".env"
    if path.exists():
        for line in path.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            key, sep, value = line.partition("=")
            if not sep:
                raise ValueError("Malformed .env assignment")
            value = value.strip()
            if value.startswith(("'", '"')):
                if len(value) < 2 or value[-1] != value[0]:
                    raise ValueError(f"Unclosed .env quote: {key}")
                value = value[1:-1]
            else:
                value = value.split(" #", 1)[0].rstrip()
            values[key.strip()] = value
    values.update(os.environ)
    return {
        "bootstrap": values.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "reference": (PROJECT / values.get("REFERENCE_DATA_ROOT", "data/reference")).resolve(),
        "waveform": (PROJECT / values["MIMIC_WAVEFORM_ROOT"]).resolve()
        if values.get("MIMIC_WAVEFORM_ROOT") else None,
    }
