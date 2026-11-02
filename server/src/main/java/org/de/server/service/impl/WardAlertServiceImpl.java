package org.de.server.service.impl;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.response.AlertSnapshotDto;
import org.de.server.dto.response.PageDto;
import org.de.server.enums.AlertStatus;
import org.de.server.mapper.AlertMapper;
import org.de.server.repository.SepsisAlertEventRepository;
import org.de.server.service.WardAlertService;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class WardAlertServiceImpl implements WardAlertService {

    private static final int DEFAULT_LIMIT = 100;
    private static final int MAX_LIMIT = 200;

    private final SepsisAlertEventRepository alertRepository;
    private final AlertMapper mapper;

    @Override
    public PageDto<AlertSnapshotDto> getAlertFeed(AlertStatus status, Integer limit) {
        Pageable pageable = PageRequest.of(
                0,
                limit(limit),
                Sort.by(Sort.Direction.DESC, "id.alertTime")
        );

        var page = alertRepository.findAlertFeed(status, pageable);
        return PageDto.from(page, mapper::toSnapshotDto);
    }

    private int limit(Integer limit) {
        if (limit == null || limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }
}
