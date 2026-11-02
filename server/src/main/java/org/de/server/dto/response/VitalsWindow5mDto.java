package org.de.server.dto.response;

import java.time.OffsetDateTime;

public record VitalsWindow5mDto(
        OffsetDateTime windowEnd,
        Long stayId,
        Double avgHeartRate,
        Double avgMap,
        Double avgSbp,
        Double avgSpo2,
        Double varHeartRate,
        Double varMap,
        Double varSbp,
        Double varSpo2
) {
}
