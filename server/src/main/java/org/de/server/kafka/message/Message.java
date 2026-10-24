package org.de.server.kafka.message;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.OffsetDateTime;
import java.util.UUID;

@JsonIgnoreProperties(ignoreUnknown = true)
public interface Message {
    String schemaVersion();
    UUID messageId();
    EventType eventType();
    OffsetDateTime producedAt();
    String producer();
    String traceId();
}
