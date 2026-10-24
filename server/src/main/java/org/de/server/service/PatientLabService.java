package org.de.server.service;

import org.de.server.dto.response.LabHistoryResponse;

import java.time.OffsetDateTime;

public interface PatientLabService {
    LabHistoryResponse getLabHistory(
            Long stayId,
            OffsetDateTime from,
            OffsetDateTime to,
            boolean abnormalOnly,
            Integer limit
    );
}
