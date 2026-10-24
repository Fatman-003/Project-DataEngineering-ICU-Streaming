package org.de.server.service;

import org.de.server.dto.response.Vitals5mHistoryResponse;

public interface PatientVitalsService {
    Vitals5mHistoryResponse getVitals5mHistory(Long stayId, Integer limit);
}
