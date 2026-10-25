package org.de.server.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.de.server.kafka.message.AlertMessage;
import org.de.server.kafka.message.AlertStatusMessage;
import org.de.server.kafka.message.EventType;
import org.de.server.mapper.AlertMapper;
import org.de.server.mapper.AlertStatusChangeMapper;
import org.de.server.websocket.RealtimePublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AlertConsumer {

    private final AlertMapper alertMapper;
    private final AlertStatusChangeMapper alertStatusChangeMapper;
    private final RealtimePublisher publisher;
    private final ObjectMapper objectMapper;

    @KafkaListener(
        topics = "${app.kafka.topics.alerts}",
        containerFactory = "alertsContainerFactory"
    )
    public void onAlertData(JsonNode node) throws JsonProcessingException {

        String rawEventType = node.path("event_type").asText(null);
        if (rawEventType == null) {
            log.error("Alert message is missing event_type: {}", node);
            throw new IllegalArgumentException("Missing event_type in alert message");
        }

        EventType type;
        try {
            type = EventType.valueOf(rawEventType);
        } catch (IllegalArgumentException ex) {
            log.error("Unknown event_type '{}' in alert message: {}", rawEventType, node);
            throw ex;
        }

        switch (type) {
            case SEPSIS_ALERT_CREATED -> {
                AlertMessage message = objectMapper.treeToValue(node, AlertMessage.class);
                publisher.publishAlert(alertMapper.toDto(message));
                log.info("Published alert created: stayId={}, alertId={}, severity={}",
                        message.stayId(), message.alertId(), message.severity());
            }
            case SEPSIS_ALERT_STATUS_CHANGED -> {
                AlertStatusMessage message = objectMapper.treeToValue(node, AlertStatusMessage.class);
                publisher.publishAlertStatus(alertStatusChangeMapper.toDto(message));
                log.info("Published alert status change: stayId={}, alertId={}, status={}",
                        message.stayId(), message.alertId(), message.status());
            }
            default -> log.warn("Skipping unsupported event_type '{}' on alerts topic", type);
        }
    }
}
