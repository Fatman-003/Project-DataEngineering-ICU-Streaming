"""Central TICKET-006 runtime settings; no host PyFlink dependency."""
import os
from dataclasses import dataclass

DLQ_TOPIC = "icu.v1.deadletter"
# Original RAW values may expand through JSON escaping plus lossless Base64.
DLQ_MAX_MESSAGE_BYTES = 8 * 1024 * 1024


@dataclass(frozen=True)
class RuntimeConfig:
    bootstrap_servers: str = "icu-kafka:9094"
    dlq_topic: str = DLQ_TOPIC
    max_out_of_orderness_seconds: int = 30
    dedup_state_ttl_hours: int = 24
    checkpoint_interval_ms: int = 10000

    @classmethod
    def from_env(cls):
        defaults = cls()
        config = cls(
            bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", defaults.bootstrap_servers),
            dlq_topic=os.environ.get("DLQ_TOPIC", defaults.dlq_topic),
            max_out_of_orderness_seconds=int(os.environ.get(
                "MAX_OUT_OF_ORDERNESS_SECONDS", defaults.max_out_of_orderness_seconds)),
            dedup_state_ttl_hours=int(os.environ.get(
                "DEDUP_STATE_TTL_HOURS", defaults.dedup_state_ttl_hours)),
            checkpoint_interval_ms=int(os.environ.get(
                "CHECKPOINT_INTERVAL_MS", defaults.checkpoint_interval_ms)),
        )
        config.validate()
        return config

    def validate(self):
        if not self.bootstrap_servers.strip():
            raise ValueError("KAFKA_BOOTSTRAP_SERVERS must be nonempty")
        if self.dlq_topic != DLQ_TOPIC:
            raise ValueError(f"TICKET-006 permits only output topic {DLQ_TOPIC}")
        if self.max_out_of_orderness_seconds < 0:
            raise ValueError("MAX_OUT_OF_ORDERNESS_SECONDS must be >= 0")
        if self.dedup_state_ttl_hours <= 0 or self.checkpoint_interval_ms <= 0:
            raise ValueError("DEDUP_STATE_TTL_HOURS and CHECKPOINT_INTERVAL_MS must be > 0")
