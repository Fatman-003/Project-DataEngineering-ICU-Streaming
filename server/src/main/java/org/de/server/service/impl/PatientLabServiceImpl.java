package org.de.server.service.impl;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.response.LabHistoryResponse;
import org.de.server.dto.response.PageDto;
import org.de.server.exception.IcuStayNotFoundException;
import org.de.server.repository.IcuStayRepository;
import org.de.server.repository.LabEventRepository;
import org.de.server.service.PatientLabService;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;

@Service
@RequiredArgsConstructor
public class PatientLabServiceImpl implements PatientLabService {

    private static final int DEFAULT_LIMIT = 50;
    private static final int MAX_LIMIT = 200;

    private final LabEventRepository labEventRepository;
    private final IcuStayRepository icuStayRepository;

    @Override
    public LabHistoryResponse getLabHistory(
            Long stayId,
            OffsetDateTime from,
            OffsetDateTime to,
            boolean abnormalOnly,
            Integer limit
    ) {
        if (!icuStayRepository.existsById(stayId)) {
            throw new IcuStayNotFoundException(stayId);
        }

        int pageSize = limit(limit);
        var result = labEventRepository.findLabHistory(stayId, from, to, abnormalOnly, pageSize);
        var page = new PageDto<>(
                result.content(),
                0,
                pageSize,
                result.totalElements(),
                (int) Math.ceil((double) result.totalElements() / pageSize),
                true,
                result.totalElements() <= pageSize
        );

        return new LabHistoryResponse(stayId, page);
    }

    private int limit(Integer limit) {
        if (limit == null || limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }
}
