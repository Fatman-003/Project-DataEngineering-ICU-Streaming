package org.de.server.dto.response;

public record AlertHistoryResponse(
        Long stayId,
        PageDto<AlertSnapshotDto> alerts
) {
}
