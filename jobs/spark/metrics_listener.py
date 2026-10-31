"""Append every StreamingQueryProgress to <metrics_root>/<query>.jsonl."""
import json
import os
import threading

from pyspark.sql.streaming import StreamingQueryListener

# Fields the benchmark reads (plan §5.5); the full progress JSON is kept as well.
SUMMARY_FIELDS = ("batchId", "timestamp", "numInputRows", "inputRowsPerSecond", "processedRowsPerSecond")


def summarize(progress):
    operators = progress.get("stateOperators") or []
    return {**{field: progress.get(field) for field in SUMMARY_FIELDS},
            "durationMs": progress.get("durationMs", {}),
            "watermark": (progress.get("eventTime") or {}).get("watermark"),
            "stateRowsTotal": sum(op.get("numRowsTotal", 0) for op in operators),
            "stateMemoryUsedBytes": sum(op.get("memoryUsedBytes", 0) for op in operators),
            "numRowsDroppedByWatermark": sum(op.get("numRowsDroppedByWatermark", 0) for op in operators)}


class JsonlProgressListener(StreamingQueryListener):
    def __init__(self, root):
        super().__init__()
        self.root = root
        self.lock = threading.Lock()
        os.makedirs(root, exist_ok=True)

    def write(self, name, record):
        line = json.dumps(record, separators=(",", ":"), default=str)
        with self.lock, open(os.path.join(self.root, f"{name or 'unnamed'}.jsonl"), "a", encoding="utf-8") as out:
            out.write(line + "\n")

    def onQueryStarted(self, event):
        self.write(event.name, {"event": "started", "id": str(event.id), "runId": str(event.runId),
                                "timestamp": event.timestamp})

    def onQueryProgress(self, event):
        progress = json.loads(event.progress.json)
        self.write(progress.get("name"), {"event": "progress", **summarize(progress), "progress": progress})

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        self.write("terminated", {"event": "terminated", "id": str(event.id), "runId": str(event.runId),
                                  "exception": event.exception})
