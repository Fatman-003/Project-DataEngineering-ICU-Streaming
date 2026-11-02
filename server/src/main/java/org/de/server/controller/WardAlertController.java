package org.de.server.controller;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.response.AlertSnapshotDto;
import org.de.server.dto.response.ApiResponse;
import org.de.server.dto.response.PageDto;
import org.de.server.enums.AlertStatus;
import org.de.server.service.WardAlertService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * Ward-wide alert feed: alerts for every patient, regardless of which
 * patient console the requester is currently viewing.
 */
@RestController
@RequestMapping("/api/v1/alerts")
@RequiredArgsConstructor
public class WardAlertController {

    private final WardAlertService wardAlertService;

    @GetMapping
    public ResponseEntity<ApiResponse<PageDto<AlertSnapshotDto>>> getAlertFeed(
            @RequestParam(required = false) AlertStatus status,
            @RequestParam(required = false) Integer limit
    ) {
        var alerts = wardAlertService.getAlertFeed(status, limit);
        return ResponseEntity.ok(ApiResponse.success(alerts));
    }
}
