package org.de.server.dto.response;

import org.de.server.enums.AlertSeverity;
import org.de.server.enums.AlertStatus;
import org.de.server.enums.AlertType;

import java.time.OffsetDateTime;
import java.util.UUID;

public record AlertSnapshotDto(
        UUID alertId,
        Long stayId,
        OffsetDateTime alertTime,
        AlertType alertType,
        AlertSeverity severity,
        Double triggerValue,
        String triggerMessage,
        AlertStatus status
) {
}
