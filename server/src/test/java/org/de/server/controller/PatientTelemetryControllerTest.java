package org.de.server.controller;

import org.de.server.dto.request.TelemetrySearchCriteria;
import org.de.server.dto.response.PatientTelemetryDto;
import org.de.server.dto.response.TelemetryHistoryResponse;
import org.de.server.exception.GlobalExceptionHandler;
import org.de.server.exception.IcuStayNotFoundException;
import org.de.server.service.PatientTelemetryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.OffsetDateTime;
import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class PatientTelemetryControllerTest {

    private static final String BASE_URL = "/api/v1/icu-stays/1000001/telemetry";

    @Mock
    private PatientTelemetryService patientTelemetryService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new PatientTelemetryController(patientTelemetryService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void getTelemetryHistoryReturnsOkResponse() throws Exception {
        OffsetDateTime sampleTime = OffsetDateTime.parse("2026-10-23T10:01:00Z");
        TelemetryHistoryResponse response = new TelemetryHistoryResponse(
                1000001L,
                OffsetDateTime.parse("2026-10-23T10:00:00Z"),
                OffsetDateTime.parse("2026-10-23T10:10:00Z"),
                null,
                60,
                true,
                List.of(new PatientTelemetryDto(
                        1000001L,
                        sampleTime,
                        80.0,
                        62.0,
                        120.0,
                        98.0
                ))
        );

        when(patientTelemetryService.getTelemetryHistory(
                new TelemetrySearchCriteria(
                        1000001L,
                        OffsetDateTime.parse("2026-10-23T10:00:00Z"),
                        OffsetDateTime.parse("2026-10-23T10:10:00Z"),
                        null,
                        60,
                        100
                )
        )).thenReturn(response);

        mockMvc.perform(get(BASE_URL)
                        .param("from", "2026-10-23T10:00:00Z")
                        .param("to", "2026-10-23T10:10:00Z")
                        .param("bucketSeconds", "60")
                        .param("maxPoints", "100"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.message").value("Success"))
                .andExpect(jsonPath("$.data.stayId").value(1000001))
                .andExpect(jsonPath("$.data.bucketSeconds").value(60))
                .andExpect(jsonPath("$.data.aggregated").value(true))
                .andExpect(jsonPath("$.data.points[0].heartRate").value(80.0));

        verify(patientTelemetryService).getTelemetryHistory(
                new TelemetrySearchCriteria(
                        1000001L,
                        OffsetDateTime.parse("2026-10-23T10:00:00Z"),
                        OffsetDateTime.parse("2026-10-23T10:10:00Z"),
                        null,
                        60,
                        100
                )
        );
        verifyNoMoreInteractions(patientTelemetryService);
    }

    @Test
    void getTelemetryHistoryReturnsNotFoundWhenStayIsMissing() throws Exception {
        when(patientTelemetryService.getTelemetryHistory(
                new TelemetrySearchCriteria(
                        9999999L, null, null, null, null, null)
        )).thenThrow(new IcuStayNotFoundException(9999999L));

        mockMvc.perform(get("/api/v1/icu-stays/9999999/telemetry"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(404))
                .andExpect(jsonPath("$.message").value("ICU stay not found: 9999999"));

        verify(patientTelemetryService).getTelemetryHistory(
                new TelemetrySearchCriteria(
                        9999999L, null, null, null, null, null)
        );
        verifyNoMoreInteractions(patientTelemetryService);
    }
}
