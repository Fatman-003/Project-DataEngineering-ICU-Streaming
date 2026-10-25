package org.de.server.dto.response;

public record LabHistoryResponse(
        Long stayId,
        PageDto<LabResultDto> results
) {
}
