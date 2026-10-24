package org.de.server.mapper;

import org.de.server.dto.response.AlertDto;
import org.de.server.dto.response.AlertStatusChangeDto;
import org.de.server.dto.response.PatientTelemetryDto;
import org.de.server.dto.response.VitalsWindow5mDto;
import org.de.server.enums.AlertSeverity;
import org.de.server.enums.AlertStatus;
import org.de.server.enums.AlertType;
import org.de.server.kafka.message.AlertMessage;
import org.de.server.kafka.message.AlertStatusMessage;
import org.de.server.kafka.message.EventType;
import org.de.server.kafka.message.TelemetryMessage;
import org.de.server.kafka.message.VitalsWindow5mMessage;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class KafkaMessageMapperTest {

    private static final UUID MESSAGE_ID = UUID.fromString("019a3f4b-7c2d-7e1f-8a9b-0c1d2e3f4a5b");
    private static final UUID ALERT_ID = UUID.fromString("6f1c2a9e-3b7d-5c4a-9f2e-8d1b7a6c5e40");
    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final OffsetDateTime PRODUCED_AT = OffsetDateTime.parse("2026-10-20T23:30:01.120Z");
    private static final OffsetDateTime SAMPLE_TIME = OffsetDateTime.parse("2026-10-20T23:30:01Z");
    private static final OffsetDateTime WINDOW_END = OffsetDateTime.parse("2026-10-20T23:35:00Z");
    private static final OffsetDateTime ALERT_TIME = OffsetDateTime.parse("2026-10-20T23:36:12Z");

    private final PatientTelemetryMapper telemetryMapper = Mappers.getMapper(PatientTelemetryMapper.class);
    private final VitalsWindow5mMapper vitalsMapper = Mappers.getMapper(VitalsWindow5mMapper.class);
    private final AlertMapper alertMapper = Mappers.getMapper(AlertMapper.class);
    private final AlertStatusChangeMapper alertStatusMapper = Mappers.getMapper(AlertStatusChangeMapper.class);

    @Test
    void mapsTelemetryMessageToDto() {
        TelemetryMessage message = new TelemetryMessage(
                "1.0",
                MESSAGE_ID,
                EventType.TELEMETRY_SAMPLE,
                PRODUCED_AT,
                "icu-simulator",
                TRACE_ID,
                SAMPLE_TIME,
                1000001L,
                88.0,
                72.0,
                118.0,
                97.0);

        PatientTelemetryDto dto = telemetryMapper.toDto(message);

        assertEquals(
                new PatientTelemetryDto(1000001L, SAMPLE_TIME, 88.0, 72.0, 118.0, 97.0),
                dto);
    }

    @Test
    void keepsNullTelemetrySignals() {
        TelemetryMessage message = new TelemetryMessage(
                "1.0",
                MESSAGE_ID,
                EventType.TELEMETRY_SAMPLE,
                PRODUCED_AT,
                "icu-simulator",
                null,
                SAMPLE_TIME,
                1000001L,
                null,
                null,
                null,
                97.0);

        PatientTelemetryDto dto = telemetryMapper.toDto(message);

        assertEquals(1000001L, dto.stayId());
        assertEquals(SAMPLE_TIME, dto.sampleTime());
        assertNull(dto.heartRate());
        assertNull(dto.map());
        assertNull(dto.sbp());
        assertEquals(97.0, dto.spo2());
    }

    @Test
    void mapsVitalsWindow5mMessageToDto() {
        VitalsWindow5mMessage message = new VitalsWindow5mMessage(
                "1.0",
                MESSAGE_ID,
                EventType.VITALS_WINDOW_5M,
                PRODUCED_AT,
                "icu-flink",
                null,
                WINDOW_END,
                1000001L,
                87.4,
                71.2,
                116.8,
                96.9);

        VitalsWindow5mDto dto = vitalsMapper.toDto(message);

        assertEquals(
                new VitalsWindow5mDto(WINDOW_END, 1000001L, 87.4, 71.2, 116.8, 96.9),
                dto);
    }

    @Test
    void mapsAlertMessageToDto() {
        AlertMessage message = new AlertMessage(
                "1.0",
                MESSAGE_ID,
                EventType.SEPSIS_ALERT_CREATED,
                PRODUCED_AT,
                "icu-alert-engine",
                null,
                ALERT_TIME,
                ALERT_ID,
                1000001L,
                AlertType.HYPOTENSION_SUSTAINED,
                AlertSeverity.CRITICAL,
                61.0,
                "MAP < 65 mmHg sustained for >= 30 seconds",
                AlertStatus.ACTIVE);

        AlertDto dto = alertMapper.toDto(message);

        assertEquals(
                new AlertDto(
                        ALERT_ID,
                        1000001L,
                        ALERT_TIME,
                        AlertType.HYPOTENSION_SUSTAINED,
                        AlertSeverity.CRITICAL,
                        61.0,
                        "MAP < 65 mmHg sustained for >= 30 seconds"),
                dto);
    }

    @Test
    void mapsAlertWithNullTriggerValue() {
        AlertMessage message = new AlertMessage(
                "1.0",
                MESSAGE_ID,
                EventType.SEPSIS_ALERT_CREATED,
                PRODUCED_AT,
                "icu-alert-engine",
                null,
                ALERT_TIME,
                ALERT_ID,
                1000001L,
                AlertType.SENSOR_DISCONNECTED,
                AlertSeverity.WARNING,
                null,
                "SpO2 sensor disconnected",
                AlertStatus.FALSE_ALARM);

        AlertDto dto = alertMapper.toDto(message);

        assertNull(dto.triggerValue());
        assertEquals(ALERT_ID, dto.alertId());
        assertEquals(AlertType.SENSOR_DISCONNECTED, dto.alertType());
        assertEquals(AlertSeverity.WARNING, dto.severity());
    }

    @Test
    void mapsAlertStatusMessageToDto() {
        AlertStatusMessage message = new AlertStatusMessage(
                "1.0",
                MESSAGE_ID,
                EventType.SEPSIS_ALERT_STATUS_CHANGED,
                PRODUCED_AT,
                "icu-alert-engine",
                null,
                ALERT_TIME,
                ALERT_ID,
                1000001L,
                AlertStatus.ACKNOWLEDGED);

        AlertStatusChangeDto dto = alertStatusMapper.toDto(message);

        assertEquals(
                new AlertStatusChangeDto(
                        ALERT_ID,
                        1000001L,
                        ALERT_TIME,
                        AlertStatus.ACKNOWLEDGED),
                dto);
    }

    @Test
    void returnsNullForNullMessage() {
        assertNull(telemetryMapper.toDto(null));
        assertNull(vitalsMapper.toDto(null));
        assertNull(alertMapper.toDto(null));
        assertNull(alertStatusMapper.toDto(null));
    }
}
