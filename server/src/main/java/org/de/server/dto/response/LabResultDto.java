package org.de.server.dto.response;

import org.de.server.enums.LabFlag;

import java.time.LocalDateTime;

public record LabResultDto(
        String labId,
        Long stayId,
        LocalDateTime collectedAt,
        Integer itemId,
        String name,
        String category,
        Double value,
        String rawValue,
        String unit,
        Double referenceLow,
        Double referenceHigh,
        LabFlag flag
) {
}
