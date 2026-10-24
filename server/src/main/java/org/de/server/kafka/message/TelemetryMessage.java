package org.de.server.kafka.message;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;
import java.util.UUID;

public record TelemetryMessage(
        @JsonProperty("schema_version") String schemaVersion,
        @JsonProperty("message_id") UUID messageId,
        @JsonProperty("event_type") EventType eventType,
        @JsonProperty("produced_at") OffsetDateTime producedAt,
        @JsonProperty("producer") String producer,
        @JsonProperty("trace_id") String traceId,
        @JsonProperty("sample_time") OffsetDateTime sampleTime,
        @JsonProperty("stay_id") Long stayId,
        @JsonProperty("heart_rate") Double heartRate,
        @JsonProperty("map") Double map,
        @JsonProperty("sbp") Double sbp,
        @JsonProperty("spo2") Double spo2
) implements Message {
}
