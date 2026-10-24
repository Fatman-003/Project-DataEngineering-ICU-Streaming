package org.de.server.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.de.server.dto.response.AlertDto;
import org.de.server.dto.response.AlertStatusChangeDto;
import org.de.server.enums.AlertSeverity;
import org.de.server.enums.AlertStatus;
import org.de.server.enums.AlertType;
import org.de.server.kafka.message.AlertMessage;
import org.de.server.kafka.message.AlertStatusMessage;
import org.de.server.kafka.message.EventType;
import org.de.server.mapper.AlertMapper;
import org.de.server.mapper.AlertStatusChangeMapper;
import org.de.server.websocket.RealtimePublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.annotation.KafkaListener;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AlertConsumerTest {

    private static final UUID MESSAGE_ID = UUID.fromString("019a3f4b-7c2d-7e1f-8a9b-0c1d2e3f4a5b");
    private static final UUID ALERT_ID = UUID.fromString("6f1c2a9e-3b7d-5c4a-9f2e-8d1b7a6c5e40");
    private static final OffsetDateTime ALERT_TIME = OffsetDateTime.parse("2026-10-20T23:36:12Z");

    private static final String ALERT_CREATED_JSON = """
            {
              "schema_version": "1.0",
              "message_id": "019a3f4b-7c2d-7e1f-8a9b-0c1d2e3f4a5b",
              "event_type": "SEPSIS_ALERT_CREATED",
              "produced_at": "2026-10-20T23:36:12.120Z",
              "producer": "icu-alert-engine",
              "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
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

    private static final String ALERT_STATUS_JSON = """
            {
              "schema_version": "1.0",
              "message_id": "019a3f4b-7c2d-7e1f-8a9b-0c1d2e3f4a5b",
              "event_type": "SEPSIS_ALERT_STATUS_CHANGED",
              "produced_at": "2026-10-20T23:40:00.000Z",
              "producer": "icu-alert-engine",
              "trace_id": "4bf92f3577b34da6a3ce929d0e0e4736",
              "alert_time": "2026-10-20T23:36:12Z",
              "alert_id": "6f1c2a9e-3b7d-5c4a-9f2e-8d1b7a6c5e40",
              "stay_id": 1000001,
              "status": "ACKNOWLEDGED"
            }
            """;

    @Mock
    private AlertMapper alertMapper;

    @Mock
    private AlertStatusChangeMapper alertStatusChangeMapper;

    @Mock
    private RealtimePublisher publisher;

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .addModule(new JavaTimeModule())
            .build();

    private AlertConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new AlertConsumer(alertMapper, alertStatusChangeMapper, publisher, objectMapper);
    }

    @Test
    void publishesAlertCreated() throws JsonProcessingException {
        AlertDto dto = new AlertDto(
                ALERT_ID,
                1000001L,
                ALERT_TIME,
                AlertType.HYPOTENSION_SUSTAINED,
                AlertSeverity.CRITICAL,
                61.0,
                "MAP < 65 mmHg sustained for >= 30 seconds");
        when(alertMapper.toDto(any(AlertMessage.class))).thenReturn(dto);

        consumer.onAlertData(readTree(ALERT_CREATED_JSON));

        ArgumentCaptor<AlertMessage> captor = ArgumentCaptor.forClass(AlertMessage.class);
        verify(alertMapper).toDto(captor.capture());
        AlertMessage parsed = captor.getValue();
        assertEquals(MESSAGE_ID, parsed.messageId());
        assertEquals(EventType.SEPSIS_ALERT_CREATED, parsed.eventType());
        assertEquals(ALERT_ID, parsed.alertId());
        assertEquals(1000001L, parsed.stayId());
        assertEquals(ALERT_TIME, parsed.alertTime());
        assertEquals(AlertType.HYPOTENSION_SUSTAINED, parsed.alertType());
        assertEquals(AlertSeverity.CRITICAL, parsed.severity());
        assertEquals(61.0, parsed.triggerValue());
        assertEquals(AlertStatus.ACTIVE, parsed.status());

        verify(publisher).publishAlert(dto);
        verifyNoInteractions(alertStatusChangeMapper);
    }

    @Test
    void publishesAlertStatusChange() throws JsonProcessingException {
        AlertStatusChangeDto dto = new AlertStatusChangeDto(
                ALERT_ID, 1000001L, ALERT_TIME, AlertStatus.ACKNOWLEDGED);
        when(alertStatusChangeMapper.toDto(any(AlertStatusMessage.class))).thenReturn(dto);

        consumer.onAlertData(readTree(ALERT_STATUS_JSON));

        ArgumentCaptor<AlertStatusMessage> captor =
                ArgumentCaptor.forClass(AlertStatusMessage.class);
        verify(alertStatusChangeMapper).toDto(captor.capture());
        AlertStatusMessage parsed = captor.getValue();
        assertEquals(EventType.SEPSIS_ALERT_STATUS_CHANGED, parsed.eventType());
        assertEquals(ALERT_ID, parsed.alertId());
        assertEquals(1000001L, parsed.stayId());
        assertEquals(AlertStatus.ACKNOWLEDGED, parsed.status());

        verify(publisher).publishAlertStatus(dto);
        verifyNoInteractions(alertMapper);
    }

    @Test
    void skipsEventTypeThatDoesNotBelongToAlertsTopic() throws JsonProcessingException {
        consumer.onAlertData(readTree("{\"event_type\":\"VITALS_WINDOW_5M\"}"));

        verifyNoInteractions(alertMapper, alertStatusChangeMapper, publisher);
    }

    @Test
    void rejectsUnknownEventType() throws JsonProcessingException {
        JsonNode node = readTree("{\"event_type\":\"NOT_A_REAL_EVENT\"}");

        assertThrows(IllegalArgumentException.class, () -> consumer.onAlertData(node));
        verifyNoInteractions(alertMapper, alertStatusChangeMapper, publisher);
    }

    @Test
    void rejectsMessageWithoutEventType() throws JsonProcessingException {
        JsonNode node = readTree("{}");

        assertThrows(IllegalArgumentException.class, () -> consumer.onAlertData(node));
        verifyNoInteractions(alertMapper, alertStatusChangeMapper, publisher);
    }

    @Test
    void rejectsMalformedAlertPayload() throws JsonProcessingException {
        JsonNode node = readTree(ALERT_CREATED_JSON.replace(
                "\"alert_time\": \"2026-10-20T23:36:12Z\"",
                "\"alert_time\": \"not-a-timestamp\""));

        assertThrows(JsonProcessingException.class, () -> consumer.onAlertData(node));
        verifyNoInteractions(alertMapper, alertStatusChangeMapper, publisher);
    }

    @Test
    void listensToAlertsTopicWithDedicatedContainerFactory() throws NoSuchMethodException {
        KafkaListener listener = AlertConsumer.class
                .getMethod("onAlertData", JsonNode.class)
                .getAnnotation(KafkaListener.class);

        assertArrayEquals(new String[]{"${app.kafka.topics.alerts}"}, listener.topics());
        assertEquals("alertsContainerFactory", listener.containerFactory());
    }

    private JsonNode readTree(String json) throws JsonProcessingException {
        return objectMapper.readTree(json);
    }
}
