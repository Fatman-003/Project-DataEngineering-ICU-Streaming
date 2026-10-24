package org.de.server.controller;

import org.de.server.dto.response.PageDto;
import org.de.server.dto.response.StaySummaryDto;
import org.de.server.exception.GlobalExceptionHandler;
import org.de.server.exception.IcuStayNotFoundException;
import org.de.server.service.IcuStayService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.List;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class IcuStayControllerTest {

    private static final String BASE_URL = "/api/v1/icu-stays";

    @Mock
    private IcuStayService icuStayService;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(new IcuStayController(icuStayService))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void getStaySummariesReturnsOkResponse() throws Exception {
        StaySummaryDto dto = createStaySummary(1000001L);
        PageDto<StaySummaryDto> page = new PageDto<>(
                List.of(dto), 0, 10, 1L, 1, true, true);

        when(icuStayService.getAllStays(0, 10)).thenReturn(page);

        mockMvc.perform(get(BASE_URL))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.message").value("Success"))
                .andExpect(jsonPath("$.data.content[0].stayId").value(1000001))
                .andExpect(jsonPath("$.data.page").value(0))
                .andExpect(jsonPath("$.data.size").value(10))
                .andExpect(jsonPath("$.data.totalElements").value(1))
                .andExpect(jsonPath("$.data.totalPages").value(1))
                .andExpect(jsonPath("$.data.first").value(true))
                .andExpect(jsonPath("$.data.last").value(true));

        verify(icuStayService).getAllStays(0, 10);
        verifyNoMoreInteractions(icuStayService);
    }

    @Test
    void getStaySummariesPassesExplicitPagination() throws Exception {
        PageDto<StaySummaryDto> page = new PageDto<>(
                List.of(), 1, 5, 0L, 1, true, true);

        when(icuStayService.getAllStays(1, 5)).thenReturn(page);

        mockMvc.perform(get(BASE_URL)
                        .param("page", "1")
                        .param("size", "5"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.data.content").isEmpty());

        verify(icuStayService).getAllStays(1, 5);
        verifyNoMoreInteractions(icuStayService);
    }

    @Test
    void getStaySummaryByIdReturnsOkResponse() throws Exception {
        StaySummaryDto dto = createStaySummary(1000001L);

        when(icuStayService.getStaySummaryById(1000001L)).thenReturn(dto);

        mockMvc.perform(get(BASE_URL + "/1000001"))
                .andExpect(status().isOk())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(200))
                .andExpect(jsonPath("$.message").value("Success"))
                .andExpect(jsonPath("$.data.stayId").value(1000001))
                .andExpect(jsonPath("$.data.subjectId").value(1))
                .andExpect(jsonPath("$.data.hadmId").value(2))
                .andExpect(jsonPath("$.data.gender").value("F"))
                .andExpect(jsonPath("$.data.age").value(67));

        verify(icuStayService).getStaySummaryById(1000001L);
        verifyNoMoreInteractions(icuStayService);
    }

    @Test
    void getStaySummaryByIdReturnsNotFoundResponse() throws Exception {
        when(icuStayService.getStaySummaryById(9999999L))
                .thenThrow(new IcuStayNotFoundException(9999999L));

        mockMvc.perform(get(BASE_URL + "/9999999"))
                .andExpect(status().isNotFound())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(404))
                .andExpect(jsonPath("$.message").value("ICU stay not found: 9999999"));

        verify(icuStayService).getStaySummaryById(9999999L);
        verifyNoMoreInteractions(icuStayService);
    }

    @Test
    void getStaySummariesReturnsBadRequestForInvalidParameter() throws Exception {
        mockMvc.perform(get(BASE_URL)
                        .param("size", "abc"))
                .andExpect(status().isBadRequest())
                .andExpect(content().contentType("application/json"))
                .andExpect(jsonPath("$.code").value(400))
                .andExpect(jsonPath("$.message").value("Invalid parameter: size"));

        verifyNoInteractions(icuStayService);
    }

    private StaySummaryDto createStaySummary(Long stayId) {
        return new StaySummaryDto(
                stayId,
                1L,
                2L,
                "F",
                67,
                "1",
                "MICU",
                "MICU",
                null,
                null,
                null,
                null,
                null,
                null,
                null
        );
    }
}
