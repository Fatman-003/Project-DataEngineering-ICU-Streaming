package org.de.server.controller;

import org.de.server.dto.response.AlertHistoryResponse;
import org.de.server.dto.response.AlertSnapshotDto;
import org.de.server.dto.response.LabHistoryResponse;
import org.de.server.dto.response.LabResultDto;
import org.de.server.dto.response.PageDto;
import org.de.server.dto.response.Vitals5mHistoryResponse;
import org.de.server.dto.response.VitalsWindow5mDto;
import org.de.server.enums.AlertSeverity;
import org.de.server.enums.AlertStatus;
import org.de.server.enums.AlertType;
import org.de.server.enums.LabFlag;
import org.de.server.exception.GlobalExceptionHandler;
import org.de.server.service.PatientAlertService;
import org.de.server.service.PatientLabService;
import org.de.server.service.PatientVitalsService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class PatientMonitoringControllerTest {

    private static final String BASE_URL = "/api/v1/icu-stays/1000001";

    @Mock
    private PatientLabService patientLabService;

    @Mock
    private PatientAlertService patientAlertService;

    @Mock
    private PatientVitalsService patientVitalsService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new PatientMonitoringController(
                        patientLabService,
                        patientAlertService,
                        patientVitalsService
                ))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void getLabHistoryReturnsOkResponse() throws Exception {
        LabResultDto lab = new LabResultDto(
                "lab-1",
                1000001L,
                LocalDateTime.parse("2026-10-23T10:00:00"),
                50813,
                "Lactate",
                "Blood Gas",
                4.8,
                "4.8",
                "mmol/L",
                0.5,
                2.2,
                LabFlag.CRITICAL
        );
        LabHistoryResponse response = new LabHistoryResponse(
                1000001L,
                new PageDto<>(List.of(lab), 0, 50, 1L, 1, true, true)
        );

        when(patientLabService.getLabHistory(1000001L, null, null, false, null))
                .thenReturn(response);

        mockMvc.perform(get(BASE_URL + "/labs"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.stayId").value(1000001))
                .andExpect(jsonPath("$.data.results.content[0].labId").value("lab-1"))
                .andExpect(jsonPath("$.data.results.content[0].name").value("Lactate"))
                .andExpect(jsonPath("$.data.results.content[0].flag").value("critical"));

        verify(patientLabService).getLabHistory(1000001L, null, null, false, null);
        verifyNoMoreInteractions(patientLabService);
    }

    @Test
    void getAlertHistoryReturnsOkResponse() throws Exception {
        AlertSnapshotDto alert = new AlertSnapshotDto(
                UUID.fromString("6f1c2a9e-3b7d-5c4a-9f2e-8d1b7a6c5e40"),
                1000001L,
                OffsetDateTime.parse("2026-10-23T10:00:00Z"),
                AlertType.HYPOTENSION_SUSTAINED,
                AlertSeverity.CRITICAL,
                61.0,
                "MAP < 65 mmHg sustained for >= 30 seconds",
                AlertStatus.ACTIVE
        );
        AlertHistoryResponse response = new AlertHistoryResponse(
                1000001L,
                new PageDto<>(List.of(alert), 0, 100, 1L, 1, true, true)
        );

        when(patientAlertService.getAlertHistory(1000001L, AlertStatus.ACTIVE, 100))
                .thenReturn(response);

        mockMvc.perform(get(BASE_URL + "/alerts")
                        .param("status", "ACTIVE")
                        .param("limit", "100"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.stayId").value(1000001))
                .andExpect(jsonPath("$.data.alerts.content[0].alertId")
                        .value("6f1c2a9e-3b7d-5c4a-9f2e-8d1b7a6c5e40"))
                .andExpect(jsonPath("$.data.alerts.content[0].status").value("ACTIVE"));

        verify(patientAlertService).getAlertHistory(1000001L, AlertStatus.ACTIVE, 100);
        verifyNoMoreInteractions(patientAlertService);
    }

    @Test
    void getVitals5mHistoryReturnsOkResponse() throws Exception {
        VitalsWindow5mDto window = new VitalsWindow5mDto(
                OffsetDateTime.parse("2026-10-23T10:05:00Z"),
                1000001L,
                80.0,
                62.0,
                120.0,
                98.0
        );
        Vitals5mHistoryResponse response = new Vitals5mHistoryResponse(1000001L, List.of(window));

        when(patientVitalsService.getVitals5mHistory(1000001L, null)).thenReturn(response);

        mockMvc.perform(get(BASE_URL + "/vitals-5m"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.stayId").value(1000001))
                .andExpect(jsonPath("$.data.windows[0].avgHeartRate").value(80.0));

        verify(patientVitalsService).getVitals5mHistory(1000001L, null);
        verifyNoMoreInteractions(patientVitalsService);
    }
}
