package org.de.server.websocket;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.List;

@ConfigurationProperties(prefix = "app.ws")
public record RealtimeProperties(
        String endpoint,
        List<String> allowedOrigins,
        Duration heartbeat,
        Duration telemetryFlush,
        DataSize messageSizeLimit,
        DataSize sendBufferLimit,
        Duration sendTimeout) {
}