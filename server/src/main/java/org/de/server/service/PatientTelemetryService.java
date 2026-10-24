package org.de.server.service;

import org.de.server.dto.request.TelemetrySearchCriteria;
import org.de.server.dto.response.TelemetryHistoryResponse;

public interface PatientTelemetryService {
    TelemetryHistoryResponse getTelemetryHistory(TelemetrySearchCriteria criteria);
}
