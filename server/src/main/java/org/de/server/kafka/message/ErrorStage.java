package org.de.server.kafka.message;

public enum ErrorStage {
    DESERIALIZATION,
    VALIDATION,
    PROCESSING,
    STORAGE
}
