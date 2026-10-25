"""Central business timing settings; environment overrides concern infrastructure only."""
import os
from dataclasses import dataclass
from contracts import BUSINESS_TOPICS, DLQ_TOPIC
DLQ_MAX_MESSAGE_BYTES = 8 * 1024 * 1024
MAX_OUT_OF_ORDER_SECONDS = 2
HYPOTENSION_MAP_THRESHOLD = 65.0
HYPOTENSION_CONFIRMATION_SECONDS = 30
HYPOTENSION_MAX_SAMPLE_GAP_SECONDS = 3
HYPOTENSION_RECOVERY_WINDOW_SECONDS = 10
HYPOTENSION_RECOVERY_OBSERVATIONS = 5
MAP_BUCKET_SECONDS = 1
MAP_BUCKET_MAX_SAMPLES = 256
CONTEXT_HR_HIGH_THRESHOLD = 100
CONTEXT_RR_HIGH_THRESHOLD = 22
CONTEXT_SBP_LOW_THRESHOLD = 100
CONTEXT_SPO2_LOW_THRESHOLD = 91


@dataclass(frozen=True)
class RuntimeConfig:
    bootstrap_servers: str = "icu-kafka:9094"
    dlq_topic: str = DLQ_TOPIC
    max_out_of_orderness_seconds: int = MAX_OUT_OF_ORDER_SECONDS
    dedup_state_ttl_hours: int = 24
    checkpoint_interval_ms: int = 10000

    @classmethod
    def from_env(cls):
        config = cls(bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "icu-kafka:9094"))
        config.validate()
        return config

    def validate(self):
        if not self.bootstrap_servers.strip() or self.dlq_topic != DLQ_TOPIC:
            raise ValueError("Invalid V2 Kafka configuration")
        if min(self.dedup_state_ttl_hours, self.checkpoint_interval_ms) <= 0:
            raise ValueError("TTL and checkpoint interval must be positive")
        if self.max_out_of_orderness_seconds < 0:
            raise ValueError("Out-of-order tolerance must be nonnegative")


@dataclass(frozen=True)
class WindowAlertConfig:
    window_5m_seconds: int = 300
    window_slide_seconds: int = 60
    map_threshold: float = HYPOTENSION_MAP_THRESHOLD
    map_sustained_seconds: int = HYPOTENSION_CONFIRMATION_SECONDS
    map_max_sample_gap_seconds: int = HYPOTENSION_MAX_SAMPLE_GAP_SECONDS
    map_recovery_window_seconds: int = HYPOTENSION_RECOVERY_WINDOW_SECONDS
    map_recovery_observations: int = HYPOTENSION_RECOVERY_OBSERVATIONS

    @classmethod
    def from_env(cls):
        config = cls()
        config.validate()
        return config

    def validate(self):
        if self.window_5m_seconds != 300 or self.window_slide_seconds != 60:
            raise ValueError("Business windows are five minutes sliding every minute")
        if (self.map_threshold != HYPOTENSION_MAP_THRESHOLD
                or self.map_sustained_seconds != HYPOTENSION_CONFIRMATION_SECONDS):
            raise ValueError(f"MAP rule requires <{HYPOTENSION_MAP_THRESHOLD:g} with "
                             f">={HYPOTENSION_CONFIRMATION_SECONDS} seconds low evidence")
        if self.map_max_sample_gap_seconds <= 0:
            raise ValueError("Sample continuity gap must be positive")
        if (self.map_recovery_window_seconds != HYPOTENSION_RECOVERY_WINDOW_SECONDS
                or self.map_recovery_observations != HYPOTENSION_RECOVERY_OBSERVATIONS):
            raise ValueError("Recovery requires five distinct one-second buckets within ten seconds")
