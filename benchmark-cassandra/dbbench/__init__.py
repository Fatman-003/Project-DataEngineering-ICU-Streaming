"""TimescaleDB vs Cassandra storage benchmark for the ICU streaming repo (isolated, not part of the pipeline)."""
import os
import sys
from pathlib import Path

# Inside the client container the repo is mounted read-only at /repo; on the host it is the parent folder.
REPO = Path(os.environ.get("ICU_REPO", "/repo" if os.environ.get("BENCH_IN_CONTAINER") else Path(__file__).resolve().parents[2]))
# services/storage_writer: db.insert_sql (the repo INSERT); jobs/flink: modules db.py imports.
for sub in ("jobs/flink", "services/storage_writer"):
    path = str(REPO / sub)
    if path not in sys.path:
        sys.path.insert(0, path)
