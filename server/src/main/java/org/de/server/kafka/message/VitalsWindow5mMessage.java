package org.de.server.kafka.message;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;
import java.util.UUID;

public record VitalsWindow5mMessage(
        @JsonProperty("schema_version") String schemaVersion,
        @JsonProperty("message_id") UUID messageId,
        @JsonProperty("event_type") EventType eventType,
        @JsonProperty("produced_at") OffsetDateTime producedAt,
        @JsonProperty("producer") String producer,
        @JsonProperty("trace_id") String traceId,
        @JsonProperty("window_end") OffsetDateTime windowEnd,
        @JsonProperty("stay_id") Long stayId,
        @JsonProperty("avg_heart_rate") Double avgHeartRate,
        @JsonProperty("avg_map") Double avgMap,
        @JsonProperty("avg_sbp") Double avgSbp,
        @JsonProperty("avg_spo2") Double avgSpo2
) implements Message {
}
