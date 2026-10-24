package org.de.server.websocket;

import java.time.OffsetDateTime;

public record RealtimeEvent<T>(
        RealtimeEventType type,
        long stayId,
        OffsetDateTime eventTime,
        OffsetDateTime sentAt,
        long seq,
        T data) {
}
