package org.de.server.service;

import org.de.server.dto.response.PageDto;
import org.de.server.dto.response.StaySummaryDto;

public interface IcuStayService {
    StaySummaryDto getStaySummaryById(Long stayId);
    PageDto<StaySummaryDto> getAllStays(Integer page, Integer size);
}
