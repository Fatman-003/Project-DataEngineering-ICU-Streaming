package org.de.server.websocket;

import org.de.server.dto.response.AlertDto;
import org.de.server.dto.response.AlertStatusChangeDto;
import org.de.server.dto.response.PatientTelemetryDto;
import org.de.server.dto.response.VitalsWindow5mDto;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Component;

import java.time.OffsetDateTime;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class RealtimePublisher {

    private static final String TELEMETRY = "/topic/stays/%d/telemetry";
    private static final String VITALS_5M = "/topic/stays/%d/vitals-5m";
    private static final String ALERTS = "/topic/stays/%d/alerts";

    private final SimpMessagingTemplate template;
    private final AtomicLong sequence = new AtomicLong();

    public RealtimePublisher(SimpMessagingTemplate template) {
        this.template = template;
    }

    public void publishTelemetry(PatientTelemetryDto dto) {
        send(TELEMETRY.formatted(dto.stayId()),
                RealtimeEventType.TELEMETRY, dto.stayId(), dto.sampleTime(), dto);
    }

    public void publishVitals5m(VitalsWindow5mDto dto) {
        send(VITALS_5M.formatted(dto.stayId()),
                RealtimeEventType.VITALS_5M, dto.stayId(), dto.windowEnd(), dto);
    }

    public void publishAlert(AlertDto dto) {
        send(ALERTS.formatted(dto.stayId()),
                RealtimeEventType.ALERT_CREATED, dto.stayId(), dto.alertTime(), dto);
    }

    public void publishAlertStatus(AlertStatusChangeDto dto) {
        send(ALERTS.formatted(dto.stayId()),
                RealtimeEventType.ALERT_STATUS_CHANGED,
                dto.stayId(), dto.alertTime(), dto);
    }

    @SuppressWarnings("java:S8688")
    private <T> void send(String destination, RealtimeEventType type,
                          long stayId, OffsetDateTime eventTime, T data) {
        template.convertAndSend(destination, new RealtimeEvent<>(
                type, stayId, eventTime, OffsetDateTime.now(), sequence.incrementAndGet(), data));
    }
}
