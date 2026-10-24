package org.de.server.service.impl;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.response.Vitals5mHistoryResponse;
import org.de.server.dto.response.VitalsWindow5mDto;
import org.de.server.exception.IcuStayNotFoundException;
import org.de.server.mapper.VitalsWindow5mMapper;
import org.de.server.repository.IcuStayRepository;
import org.de.server.repository.VitalsSlidingWindow5mRepository;
import org.de.server.service.PatientVitalsService;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;

import java.util.Comparator;

@Service
@RequiredArgsConstructor
public class PatientVitalsServiceImpl implements PatientVitalsService {

    private static final int DEFAULT_LIMIT = 5;
    private static final int MAX_LIMIT = 100;

    private final VitalsSlidingWindow5mRepository vitalsRepository;
    private final IcuStayRepository icuStayRepository;
    private final VitalsWindow5mMapper mapper;

    @Override
    public Vitals5mHistoryResponse getVitals5mHistory(Long stayId, Integer limit) {
        if (!icuStayRepository.existsById(stayId)) {
            throw new IcuStayNotFoundException(stayId);
        }

        Pageable pageable = PageRequest.of(0, limit(limit));
        var windows = vitalsRepository.findByIdStayIdOrderByIdWindowEndDesc(stayId, pageable)
                .stream()
                .map(mapper::toDtoFromEntity)
                .sorted(Comparator.comparing(VitalsWindow5mDto::windowEnd))
                .toList();

        return new Vitals5mHistoryResponse(stayId, windows);
    }

    private int limit(Integer limit) {
        if (limit == null || limit <= 0) {
            return DEFAULT_LIMIT;
        }
        return Math.min(limit, MAX_LIMIT);
    }
}
