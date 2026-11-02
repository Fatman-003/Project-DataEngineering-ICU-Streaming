package org.de.server.controller;

import org.de.server.dto.response.AlertSnapshotDto;
import org.de.server.dto.response.ApiResponse;
import org.de.server.dto.response.PageDto;
import org.de.server.enums.AlertSeverity;
import org.de.server.enums.AlertStatus;
import org.de.server.enums.AlertType;
import org.de.server.exception.GlobalExceptionHandler;
import org.de.server.service.WardAlertService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

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
class WardAlertControllerTest {

    @Mock
    private WardAlertService wardAlertService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new WardAlertController(wardAlertService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void getAlertFeedReturnsAlertsForEveryStay() throws Exception {
        AlertSnapshotDto alert = new AlertSnapshotDto(
                UUID.fromString("6f1c2a9e-3b7d-5c4a-9f2e-8d1b7a6c5e40"),
                1000002L,
                OffsetDateTime.parse("2026-10-23T10:00:00Z"),
                AlertType.HYPOTENSION_SUSTAINED,
                AlertSeverity.CRITICAL,
                61.0,
                "MAP 61 mmHg (< 65): >= 30s accumulated low evidence",
                AlertStatus.ACTIVE
        );
        var response = new PageDto<>(List.of(alert), 0, 100, 1L, 1, true, true);

        when(wardAlertService.getAlertFeed(AlertStatus.ACTIVE, 100)).thenReturn(response);

        mockMvc.perform(get("/api/v1/alerts")
                        .param("status", "ACTIVE")
                        .param("limit", "100"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.content[0].alertId")
                        .value("6f1c2a9e-3b7d-5c4a-9f2e-8d1b7a6c5e40"))
                .andExpect(jsonPath("$.data.content[0].stayId").value(1000002))
                .andExpect(jsonPath("$.data.content[0].status").value("ACTIVE"));

        verify(wardAlertService).getAlertFeed(AlertStatus.ACTIVE, 100);
        verifyNoMoreInteractions(wardAlertService);
    }

    @Test
    void getAlertFeedSupportsDefaultFilters() throws Exception {
        when(wardAlertService.getAlertFeed(null, null))
                .thenReturn(new PageDto<>(List.of(), 0, 100, 0L, 0, true, true));

        mockMvc.perform(get("/api/v1/alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.totalElements").value(0));

        verify(wardAlertService).getAlertFeed(null, null);
        verifyNoMoreInteractions(wardAlertService);
    }
}
