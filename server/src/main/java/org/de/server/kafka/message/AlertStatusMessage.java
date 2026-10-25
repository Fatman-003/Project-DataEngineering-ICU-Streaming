package org.de.server.kafka.message;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.de.server.enums.AlertStatus;

import java.time.OffsetDateTime;
import java.util.UUID;

public record AlertStatusMessage(
        @JsonProperty("schema_version") String schemaVersion,
        @JsonProperty("message_id") UUID messageId,
        @JsonProperty("event_type") EventType eventType,
        @JsonProperty("produced_at") OffsetDateTime producedAt,
        @JsonProperty("producer") String producer,
        @JsonProperty("trace_id") String traceId,
        @JsonProperty("alert_time") OffsetDateTime alertTime,
        @JsonProperty("alert_id") UUID alertId,
        @JsonProperty("stay_id") Long stayId,
        @JsonProperty("status") AlertStatus status
) implements Message {
}
