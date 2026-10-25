package org.de.server.controller;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.response.AlertHistoryResponse;
import org.de.server.dto.response.ApiResponse;
import org.de.server.dto.response.LabHistoryResponse;
import org.de.server.dto.response.Vitals5mHistoryResponse;
import org.de.server.enums.AlertStatus;
import org.de.server.service.PatientAlertService;
import org.de.server.service.PatientLabService;
import org.de.server.service.PatientVitalsService;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;

@RestController
@RequestMapping("/api/v1/icu-stays/{stayId}")
@RequiredArgsConstructor
public class PatientMonitoringController {

    private final PatientLabService patientLabService;
    private final PatientAlertService patientAlertService;
    private final PatientVitalsService patientVitalsService;

    @GetMapping("/labs")
    public ResponseEntity<ApiResponse<LabHistoryResponse>> getLabHistory(
            @PathVariable Long stayId,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime from,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime to,
            @RequestParam(defaultValue = "false") boolean abnormalOnly,
            @RequestParam(required = false) Integer limit
    ) {
        var labs = patientLabService.getLabHistory(stayId, from, to, abnormalOnly, limit);
        return ResponseEntity.ok(ApiResponse.success(labs));
    }

    @GetMapping("/alerts")
    public ResponseEntity<ApiResponse<AlertHistoryResponse>> getAlertHistory(
            @PathVariable Long stayId,
            @RequestParam(required = false) AlertStatus status,
            @RequestParam(required = false) Integer limit
    ) {
        var alerts = patientAlertService.getAlertHistory(stayId, status, limit);
        return ResponseEntity.ok(ApiResponse.success(alerts));
    }

    @GetMapping("/vitals-5m")
    public ResponseEntity<ApiResponse<Vitals5mHistoryResponse>> getVitals5mHistory(
            @PathVariable Long stayId,
            @RequestParam(required = false) Integer limit
    ) {
        var vitals = patientVitalsService.getVitals5mHistory(stayId, limit);
        return ResponseEntity.ok(ApiResponse.success(vitals));
    }
}
