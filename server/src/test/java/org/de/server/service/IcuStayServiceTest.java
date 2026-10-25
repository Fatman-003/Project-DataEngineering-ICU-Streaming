package org.de.server.service;

import org.de.server.dto.response.StaySummaryDto;
import org.de.server.entity.IcuStayEntity;
import org.de.server.exception.IcuStayNotFoundException;
import org.de.server.mapper.StayMapper;
import org.de.server.repository.IcuStayRepository;
import org.de.server.service.impl.IcuStayServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IcuStayServiceTest {

    @Mock
    private IcuStayRepository repository;

    @Mock
    private StayMapper mapper;

    @InjectMocks
    private IcuStayServiceImpl service;

    @Test
    void returnsMappedStaySummary() {
        IcuStayEntity entity = new IcuStayEntity();
        StaySummaryDto dto = new StaySummaryDto(
                1000001L, 1L, 2L, "F", 67, "1", "MICU", "MICU",
                null, null, null, null, null, null, null);

        when(repository.findByStayId(1000001L)).thenReturn(Optional.of(entity));
        when(mapper.toDto(entity)).thenReturn(dto);

        assertEquals(dto, service.getStaySummaryById(1000001L));

        verify(repository).findByStayId(1000001L);
        verify(mapper).toDto(entity);
        verifyNoMoreInteractions(repository, mapper);
    }

    @Test
    void throwsIcuStayNotFoundExceptionWhenStayIsMissing() {
        when(repository.findByStayId(9999999L)).thenReturn(Optional.empty());

        assertThrows(
                IcuStayNotFoundException.class,
                () -> service.getStaySummaryById(9999999L)
        );

        verify(repository).findByStayId(9999999L);
        verifyNoMoreInteractions(repository, mapper);
    }

    @Test
    void returnsMappedStayPage() {
        IcuStayEntity entity = new IcuStayEntity();
        StaySummaryDto dto = new StaySummaryDto(
                1000001L, 1L, 2L, "F", 67, "1", "MICU", "MICU",
                null, null, null, null, null, null, null);
        Pageable pageable = PageRequest.of(1, 20, Sort.by(Sort.Direction.ASC, "stayId"));
        PageImpl<IcuStayEntity> page = new PageImpl<>(List.of(entity), pageable, 21L);

        when(repository.findAll(any(Pageable.class))).thenReturn(page);
        when(mapper.toDto(entity)).thenReturn(dto);

        var result = service.getAllStays(1, 20);

        assertEquals(List.of(dto), result.content());
        assertEquals(1, result.page());
        assertEquals(20, result.size());
        assertEquals(21L, result.totalElements());
        assertEquals(2, result.totalPages());
        assertFalse(result.first());
        assertTrue(result.last());

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(repository).findAll(pageableCaptor.capture());
        verify(mapper).toDto(entity);

        Pageable usedPageable = pageableCaptor.getValue();
        assertEquals(1, usedPageable.getPageNumber());
        assertEquals(20, usedPageable.getPageSize());
        assertEquals(Sort.by(Sort.Direction.ASC, "stayId"), usedPageable.getSort());
    }

    @Test
    void usesDefaultPageAndSizeWhenInputIsInvalid() {
        Pageable pageable = PageRequest.of(0, 10, Sort.by(Sort.Direction.ASC, "stayId"));
        PageImpl<IcuStayEntity> page = new PageImpl<>(List.of(), pageable, 0L);

        when(repository.findAll(any(Pageable.class))).thenReturn(page);

        var result = service.getAllStays(-1, 0);

        assertEquals(0, result.page());
        assertEquals(10, result.size());
        assertEquals(0L, result.totalElements());

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(repository).findAll(pageableCaptor.capture());
        assertEquals(pageable, pageableCaptor.getValue());
        verifyNoMoreInteractions(repository, mapper);
    }

    @Test
    void capsPageSizeAtMaximumLimit() {
        Pageable pageable = PageRequest.of(2, 100, Sort.by(Sort.Direction.ASC, "stayId"));
        PageImpl<IcuStayEntity> page = new PageImpl<>(List.of(), pageable, 201L);

        when(repository.findAll(any(Pageable.class))).thenReturn(page);

        var result = service.getAllStays(2, 250);

        assertEquals(2, result.page());
        assertEquals(100, result.size());
        assertEquals(201L, result.totalElements());
        assertEquals(3, result.totalPages());
        assertFalse(result.first());
        assertTrue(result.last());

        ArgumentCaptor<Pageable> pageableCaptor = ArgumentCaptor.forClass(Pageable.class);
        verify(repository).findAll(pageableCaptor.capture());
        assertEquals(pageable, pageableCaptor.getValue());
        verifyNoMoreInteractions(repository, mapper);
    }
}

