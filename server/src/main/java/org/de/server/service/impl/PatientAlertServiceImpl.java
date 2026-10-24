package org.de.server.service.impl;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.response.AlertHistoryResponse;
import org.de.server.dto.response.PageDto;
import org.de.server.enums.AlertStatus;
import org.de.server.exception.IcuStayNotFoundException;
import org.de.server.mapper.AlertMapper;
import org.de.server.repository.IcuStayRepository;
import org.de.server.repository.SepsisAlertEventRepository;
import org.de.server.service.PatientAlertService;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class PatientAlertServiceImpl implements PatientAlertService {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 200;

    private final SepsisAlertEventRepository alertRepository;
    private final IcuStayRepository icuStayRepository;
    private final AlertMapper mapper;

    @Override
    public AlertHistoryResponse getAlertHistory(Long stayId, AlertStatus status, Integer limit) {
        if (!icuStayRepository.existsById(stayId)) {
            throw new IcuStayNotFoundException(stayId);
        }

        Pageable pageable = PageRequest.of(
                0,
                limit(limit),
                Sort.by(Sort.Direction.DESC, "id.alertTime")
        );

        var page = alertRepository.findAlertHistory(stayId, status, pageable);
        return new AlertHistoryResponse(stayId, PageDto.from(page, mapper::toSnapshotDto));
    }

    private int limit(Integer limit) {
        if (limit == null || limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }
}
