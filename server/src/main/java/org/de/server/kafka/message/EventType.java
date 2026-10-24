package org.de.server.kafka.message;

public enum EventType {
    TELEMETRY_SAMPLE,
    VITALS_WINDOW_5M,
    SEPSIS_ALERT_CREATED,
    SEPSIS_ALERT_STATUS_CHANGED,
    DLQ_EVENT
}
