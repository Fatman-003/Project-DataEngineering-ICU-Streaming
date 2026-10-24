package org.de.server.dto.request;

import java.time.OffsetDateTime;

public record TelemetrySearchCriteria(
        Long stayId,
        OffsetDateTime from,
        OffsetDateTime to,
        Integer rangeMinutes,
        Integer bucketSeconds,
        Integer maxPoints
) {
}
