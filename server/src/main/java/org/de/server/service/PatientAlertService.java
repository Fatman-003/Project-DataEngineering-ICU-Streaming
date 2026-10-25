package org.de.server.service;

import org.de.server.dto.response.AlertHistoryResponse;
import org.de.server.enums.AlertStatus;

public interface PatientAlertService {
    AlertHistoryResponse getAlertHistory(Long stayId, AlertStatus status, Integer limit);
}
