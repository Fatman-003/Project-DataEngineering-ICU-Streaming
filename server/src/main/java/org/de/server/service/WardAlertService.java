package org.de.server.service;

import org.de.server.dto.response.PageDto;
import org.de.server.dto.response.AlertSnapshotDto;
import org.de.server.enums.AlertStatus;

/**
 * Ward-level alert queries that are not scoped to a single ICU stay,
 * so every connected console sees alerts for all patients.
 */
public interface WardAlertService {

    PageDto<AlertSnapshotDto> getAlertFeed(AlertStatus status, Integer limit);
}
