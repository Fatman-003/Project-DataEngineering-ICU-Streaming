package org.de.server.controller;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.request.TelemetrySearchCriteria;
import org.de.server.dto.response.ApiResponse;
import org.de.server.dto.response.TelemetryHistoryResponse;
import org.de.server.service.PatientTelemetryService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;

@RestController
@RequestMapping("/api/v1/icu-stays/{stayId}/telemetry")
@RequiredArgsConstructor
public class PatientTelemetryController {

    private final PatientTelemetryService patientTelemetryService;

    @GetMapping
    public ResponseEntity<ApiResponse<TelemetryHistoryResponse>> getTelemetryHistory(
            @PathVariable Long stayId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
            @RequestParam(required = false) Integer rangeMinutes,
            @RequestParam(required = false) Integer bucketSeconds,
            @RequestParam(required = false) Integer maxPoints
    ) {
        var criteria = new TelemetrySearchCriteria(
                stayId,
                from,
                to,
                rangeMinutes,
                bucketSeconds,
                maxPoints
        );
        var history = patientTelemetryService.getTelemetryHistory(criteria);
        return ResponseEntity.ok(ApiResponse.success(history));
    }
}
