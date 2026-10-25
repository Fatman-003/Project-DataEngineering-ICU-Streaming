package org.de.server.dto.response;

import java.time.OffsetDateTime;
import java.util.List;

public record TelemetryHistoryResponse(
        Long stayId,
        OffsetDateTime from,
        OffsetDateTime to,
        Integer rangeMinutes,
        int bucketSeconds,
        boolean aggregated,
        List<PatientTelemetryDto> points
) {
}
