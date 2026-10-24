package org.de.server.controller;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.response.ApiResponse;
import org.de.server.dto.response.PageDto;
import org.de.server.dto.response.StaySummaryDto;
import org.de.server.service.IcuStayService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/icu-stays")
@RequiredArgsConstructor
public class IcuStayController {

    private final IcuStayService icuStayService;

    @GetMapping
    public ResponseEntity<ApiResponse<PageDto<StaySummaryDto>>> getStaySummaries(
            @RequestParam(value = "page", defaultValue = "0") Integer page,
            @RequestParam(value = "size", defaultValue = "10") Integer size
    ) {
        var staySummaries = icuStayService.getAllStays(page, size);
        return ResponseEntity.ok(ApiResponse.success(staySummaries));
    }

    @GetMapping("/{stayId}")
    public ResponseEntity<ApiResponse<StaySummaryDto>> getStaySummaryById(
            @PathVariable Long stayId
    ) {
        var staySummary = icuStayService.getStaySummaryById(stayId);
        return ResponseEntity.ok(ApiResponse.success(staySummary));
    }
}
