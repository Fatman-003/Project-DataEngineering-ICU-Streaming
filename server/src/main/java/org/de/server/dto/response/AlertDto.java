package org.de.server.dto.response;

import org.de.server.enums.AlertSeverity;
import org.de.server.enums.AlertType;

import java.time.OffsetDateTime;
import java.util.UUID;

public record AlertDto(
        UUID alertId,
        Long stayId,
        OffsetDateTime alertTime,
        AlertType alertType,
        AlertSeverity severity,
        Double triggerValue,
        String triggerMessage
) {
}
