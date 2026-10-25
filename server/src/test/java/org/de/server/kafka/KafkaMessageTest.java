package org.de.server.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.de.server.kafka.message.AlertMessage;
import org.de.server.kafka.message.DeadLetterMessage;
import org.de.server.kafka.message.ErrorCode;
import org.de.server.kafka.message.ErrorStage;
import org.de.server.kafka.message.EventType;
import org.de.server.kafka.message.TelemetryMessage;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaMessageTest {

    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .findAndRegisterModules();

    @Test
    void deserializeTelemetryMessage() throws Exception {
        String json = """
                {
                  "schema_version": "1.0",
                  "message_id": "019a3f4b-7c2d-7e1f-8a9b-0c1d2e3f4a5b",
                  "event_type": "TELEMETRY_SAMPLE",
                  "produced_at": "2026-10-20T23:30:01.120Z",
                  "producer": "icu-simulator",
                  "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
                  "sample_time": "2026-10-20T23:30:01Z",
                  "stay_id": 1000001,
                  "heart_rate": 88.0,
                  "map": 72.0,
                  "sbp": 118.0,
                  "spo2": 97.0
                }
                """;

        TelemetryMessage message = objectMapper.readValue(json, TelemetryMessage.class);

        assertEquals("1.0", message.schemaVersion());
        assertEquals(UUID.fromString("019a3f4b-7c2d-7e1f-8a9b-0c1d2e3f4a5b"), message.messageId());
        assertEquals(EventType.TELEMETRY_SAMPLE, message.eventType());
        assertEquals(OffsetDateTime.parse("2026-10-20T23:30:01.120Z"), message.producedAt());
        assertEquals("icu-simulator", message.producer());
        assertEquals(1000001L, message.stayId());
        assertEquals(88.0, message.heartRate());
        assertEquals(72.0, message.map());
        assertEquals(118.0, message.sbp());
        assertEquals(97.0, message.spo2());
    }

    @Test
    void deserializeAlertMessage() throws Exception {
        String json = """
                {
                  "schema_version": "1.0",
                  "message_id": "019a3f4b-9e4f-7031-ac1d-2e3f4a5b6c7d",
                  "event_type": "SEPSIS_ALERT_CREATED",
                  "produced_at": "2026-10-20T23:36:12.500Z",
                  "producer": "icu-alert-engine",
                  "alert_time": "2026-10-20T23:36:12Z",
                  "alert_id": "6f1c2a9e-3b7d-5c4a-9f2e-8d1b7a6c5e40",
                  "stay_id": 1000001,
                  "alert_type": "HYPOTENSION_SUSTAINED",
                  "severity": "CRITICAL",
                  "trigger_value": 61.0,
                  "trigger_message": "MAP < 65 mmHg sustained for >= 30 seconds",
                  "status": "ACTIVE"
                }
                """;

        AlertMessage message = objectMapper.readValue(json, AlertMessage.class);

        assertEquals(EventType.SEPSIS_ALERT_CREATED, message.eventType());
        assertEquals(UUID.fromString("6f1c2a9e-3b7d-5c4a-9f2e-8d1b7a6c5e40"), message.alertId());
        assertEquals("HYPOTENSION_SUSTAINED", message.alertType().name());
        assertEquals("CRITICAL", message.severity().name());
        assertEquals("ACTIVE", message.status().name());
        assertEquals(61.0, message.triggerValue());
    }

    @Test
    void deserializeDeadLetterMessage() throws Exception {
        String json = """
                {
                  "schema_version": "1.0",
                  "message_id": "019a3f4c-1a2b-7c3d-8e4f-5a6b7c8d9e0f",
                  "event_type": "DLQ_EVENT",
                  "produced_at": "2026-10-20T23:31:02.450Z",
                  "producer": "icu-dlq-router",
                  "dlq_id": "019a3f4c-1a2b-7c3d-8e4f-5a6b7c8d9e0f",
                  "failed_at": "2026-10-20T23:31:02.450Z",
                  "source_topic": "icu.telemetry",
                  "source_partition": 3,
                  "source_offset": 918273,
                  "kafka_key": "1000001",
                  "consumer_group": "icu-storage-writer",
                  "error_stage": "VALIDATION",
                  "error_code": "VALUE_OUT_OF_RANGE",
                  "error_message": "spo2 must be between 0 and 100",
                  "retryable": false,
                  "attempt": 1,
                  "original_headers": {
                    "content-type": "application/json; charset=utf-8"
                  },
                  "original_payload": {
                    "sample_time": "2026-10-20T23:30:01Z",
                    "stay_id": 1000001
                  },
                  "raw_payload_base64": null
                }
                """;

        DeadLetterMessage message = objectMapper.readValue(json, DeadLetterMessage.class);

        assertEquals(EventType.DLQ_EVENT, message.eventType());
        assertEquals(ErrorStage.VALIDATION, message.errorStage());
        assertEquals(ErrorCode.VALUE_OUT_OF_RANGE, message.errorCode());
        assertEquals(false, message.retryable());
        assertNotNull(message.originalPayload());
        assertNull(message.rawPayloadBase64());
    }

    @Test
    void ignoreUnknownFieldForForwardCompatibility() throws Exception {
        String json = """
                {
                  "schema_version": "1.1",
                  "message_id": "019a3f4b-7c2d-7e1f-8a9b-0c1d2e3f4a5b",
                  "event_type": "TELEMETRY_SAMPLE",
                  "produced_at": "2026-10-20T23:30:01.120Z",
                  "producer": "icu-simulator",
                  "sample_time": "2026-10-20T23:30:01Z",
                  "stay_id": 1000001,
                  "respiratory_rate": 24.0
                }
                """;

        TelemetryMessage message = objectMapper.readValue(json, TelemetryMessage.class);

        assertEquals("1.1", message.schemaVersion());
        assertEquals(1000001L, message.stayId());
    }
}
