package org.de.server.dto.response;

import java.util.List;

public record Vitals5mHistoryResponse(
        Long stayId,
        List<VitalsWindow5mDto> windows
) {
}
