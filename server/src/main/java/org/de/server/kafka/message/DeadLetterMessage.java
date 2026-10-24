package org.de.server.kafka.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

public record DeadLetterMessage(
        @JsonProperty("schema_version") String schemaVersion,
        @JsonProperty("message_id") UUID messageId,
        @JsonProperty("event_type") EventType eventType,
        @JsonProperty("produced_at") OffsetDateTime producedAt,
        @JsonProperty("producer") String producer,
        @JsonProperty("trace_id") String traceId,
        @JsonProperty("dlq_id") UUID dlqId,
        @JsonProperty("failed_at") OffsetDateTime failedAt,
        @JsonProperty("source_topic") String sourceTopic,
        @JsonProperty("source_partition") Integer sourcePartition,
        @JsonProperty("source_offset") Long sourceOffset,
        @JsonProperty("kafka_key") String kafkaKey,
        @JsonProperty("consumer_group") String consumerGroup,
        @JsonProperty("error_stage") ErrorStage errorStage,
        @JsonProperty("error_code") ErrorCode errorCode,
        @JsonProperty("error_message") String errorMessage,
        @JsonProperty("retryable") Boolean retryable,
        @JsonProperty("attempt") Integer attempt,
        @JsonProperty("original_headers") Map<String, String> originalHeaders,
        @JsonProperty("original_payload") JsonNode originalPayload,
        @JsonProperty("raw_payload_base64") String rawPayloadBase64
) implements Message {
}
