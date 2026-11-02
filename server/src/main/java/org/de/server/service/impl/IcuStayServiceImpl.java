package org.de.server.service.impl;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.response.PageDto;
import org.de.server.dto.response.StaySummaryDto;
import org.de.server.exception.IcuStayNotFoundException;
import org.de.server.mapper.StayMapper;
import org.de.server.repository.IcuStayRepository;
import org.de.server.service.IcuStayService;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class IcuStayServiceImpl implements IcuStayService {

    private final IcuStayRepository icuStayRepository;
    private final StayMapper mapper;

    @Override
    public StaySummaryDto getStaySummaryById(Long stayId) {
        return icuStayRepository.findByStayId(stayId)
                .map(mapper::toDto)
                .orElseThrow(() -> new IcuStayNotFoundException(stayId));
    }

    @Override
    public PageDto<StaySummaryDto> getAllStays(Integer page, Integer size) {
        // Ward console order: beds are walked in physical order; the stay id
        // only breaks ties. Unassigned beds (NULL) sort last in PostgreSQL ASC.
        var pageable = PageDto.createPageable(page, size,
                Sort.by(Sort.Direction.ASC, "bedId", "stayId"));
        var staysPage = icuStayRepository.findAll(pageable);
        return PageDto.from(staysPage, mapper::toDto);
    }
}
