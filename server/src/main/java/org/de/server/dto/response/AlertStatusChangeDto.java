package org.de.server.dto.response;

import org.de.server.enums.AlertStatus;

import java.time.OffsetDateTime;
import java.util.UUID;

public record AlertStatusChangeDto(
        UUID alertId,
        Long stayId,
        OffsetDateTime alertTime,
        AlertStatus status
) {
}
