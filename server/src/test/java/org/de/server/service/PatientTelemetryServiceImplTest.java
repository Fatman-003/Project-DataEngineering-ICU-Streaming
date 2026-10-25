package org.de.server.service;

import org.de.server.dto.request.TelemetrySearchCriteria;
import org.de.server.dto.response.PatientTelemetryDto;
import org.de.server.dto.response.TelemetryHistoryResponse;
import org.de.server.exception.IcuStayNotFoundException;
import org.de.server.repository.IcuStayRepository;
import org.de.server.repository.PatientTelemetryStreamRepository;
import org.de.server.repository.projection.PatientTelemetryBucketProjection;
import org.de.server.service.impl.PatientTelemetryServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PatientTelemetryServiceImplTest {

    private static final OffsetDateTime FROM = OffsetDateTime.parse("2026-10-23T10:00:00Z");
    private static final OffsetDateTime TO = OffsetDateTime.parse("2026-10-23T10:10:00Z");

    @Mock
    private PatientTelemetryStreamRepository telemetryRepository;

    @Mock
    private IcuStayRepository icuStayRepository;

    @InjectMocks
    private PatientTelemetryServiceImpl service;

    @Test
    void returnsAggregatedTelemetryForExplicitWindow() {
        PatientTelemetryBucketProjection projection = projection(
                Instant.parse("2026-10-23T10:01:00Z"),
                80.0,
                62.0,
                120.0,
                98.0);
        PatientTelemetryDto dto = new PatientTelemetryDto(
                1000001L,
                OffsetDateTime.parse("2026-10-23T10:01:00Z"),
                80.0,
                62.0,
                120.0,
                98.0);
        TelemetrySearchCriteria criteria = new TelemetrySearchCriteria(
                1000001L, FROM, TO, null, 60, 10);

        when(icuStayRepository.existsById(1000001L)).thenReturn(true);
        when(telemetryRepository.aggregateByTimeBucket(1000001L, FROM, TO, 60, 10))
                .thenReturn(List.of(projection));

        TelemetryHistoryResponse response = service.getTelemetryHistory(criteria);

        assertEquals(1000001L, response.stayId());
        assertEquals(FROM, response.from());
        assertEquals(TO, response.to());
        assertEquals(60, response.bucketSeconds());
        assertTrue(response.aggregated());
        assertEquals(List.of(dto), response.points());

        verify(icuStayRepository).existsById(1000001L);
        verify(telemetryRepository).aggregateByTimeBucket(1000001L, FROM, TO, 60, 10);
        verifyNoMoreInteractions(icuStayRepository, telemetryRepository);
    }

    @Test
    void derivesWindowFromLatestSampleAndCalculatesBucket() {
        OffsetDateTime latest = OffsetDateTime.parse("2026-10-23T10:05:00Z");
        OffsetDateTime expectedFrom = latest.minusSeconds(300);
        TelemetrySearchCriteria criteria = new TelemetrySearchCriteria(
                1000001L, null, null, null, null, 10);

        when(icuStayRepository.existsById(1000001L)).thenReturn(true);
        when(telemetryRepository.findLatestSampleTime(1000001L)).thenReturn(Optional.of(latest));
        when(telemetryRepository.aggregateByTimeBucket(1000001L, expectedFrom, latest, 30, 10))
                .thenReturn(List.of());

        TelemetryHistoryResponse response = service.getTelemetryHistory(criteria);

        assertEquals(expectedFrom, response.from());
        assertEquals(latest, response.to());
        assertEquals(30, response.bucketSeconds());
        assertTrue(response.aggregated());
        assertTrue(response.points().isEmpty());

        verify(icuStayRepository).existsById(1000001L);
        verify(telemetryRepository).findLatestSampleTime(1000001L);
        verify(telemetryRepository).aggregateByTimeBucket(1000001L, expectedFrom, latest, 30, 10);
        verifyNoMoreInteractions(icuStayRepository, telemetryRepository);
    }

    @Test
    void derivesWindowFromRangeMinutesAndLatestSample() {
        OffsetDateTime latest = OffsetDateTime.parse("2026-10-23T10:05:00Z");
        OffsetDateTime expectedFrom = latest.minusMinutes(60);
        TelemetrySearchCriteria criteria = new TelemetrySearchCriteria(
                1000001L, null, null, 60, null, 100);

        when(icuStayRepository.existsById(1000001L)).thenReturn(true);
        when(telemetryRepository.findLatestSampleTime(1000001L)).thenReturn(Optional.of(latest));
        when(telemetryRepository.aggregateByTimeBucket(1000001L, expectedFrom, latest, 36, 100))
                .thenReturn(List.of());

        TelemetryHistoryResponse response = service.getTelemetryHistory(criteria);

        assertEquals(expectedFrom, response.from());
        assertEquals(latest, response.to());
        assertEquals(60, response.rangeMinutes());
        assertEquals(36, response.bucketSeconds());

        verify(icuStayRepository).existsById(1000001L);
        verify(telemetryRepository).findLatestSampleTime(1000001L);
        verify(telemetryRepository).aggregateByTimeBucket(1000001L, expectedFrom, latest, 36, 100);
        verifyNoMoreInteractions(icuStayRepository, telemetryRepository);
    }

    @Test
    void returnsEmptyHistoryWhenStayHasNoTelemetry() {
        TelemetrySearchCriteria criteria = new TelemetrySearchCriteria(
                1000001L, null, null, null, null, null);

        when(icuStayRepository.existsById(1000001L)).thenReturn(true);
        when(telemetryRepository.findLatestSampleTime(1000001L)).thenReturn(Optional.empty());

        TelemetryHistoryResponse response = service.getTelemetryHistory(criteria);

        assertEquals(1000001L, response.stayId());
        assertEquals(0, response.bucketSeconds());
        assertTrue(!response.aggregated());
        assertTrue(response.points().isEmpty());

        verify(icuStayRepository).existsById(1000001L);
        verify(telemetryRepository).findLatestSampleTime(1000001L);
        verifyNoMoreInteractions(icuStayRepository, telemetryRepository);
    }

    @Test
    void throwsNotFoundWhenStayDoesNotExist() {
        TelemetrySearchCriteria criteria = new TelemetrySearchCriteria(
                9999999L, null, null, null, null, null);

        when(icuStayRepository.existsById(9999999L)).thenReturn(false);

        assertThrows(
                IcuStayNotFoundException.class,
                () -> service.getTelemetryHistory(criteria)
        );

        verify(icuStayRepository).existsById(9999999L);
        verifyNoMoreInteractions(icuStayRepository);
        verifyNoInteractions(telemetryRepository);
    }

    private PatientTelemetryBucketProjection projection(
            Instant sampleTime,
            Double heartRate,
            Double map,
            Double sbp,
            Double spo2
    ) {
        return new PatientTelemetryBucketProjection() {
            @Override
            public Instant getSampleTime() {
                return sampleTime;
            }

            @Override
            public Double getHeartRate() {
                return heartRate;
            }

            @Override
            public Double getMap() {
                return map;
            }

            @Override
            public Double getSbp() {
                return sbp;
            }

            @Override
            public Double getSpo2() {
                return spo2;
            }

            @Override
            public Double getMinHeartRate() {
                return null;
            }

            @Override
            public Double getMaxHeartRate() {
                return null;
            }

            @Override
            public Double getMinMap() {
                return null;
            }

            @Override
            public Double getMaxMap() {
                return null;
            }

            @Override
            public Double getMinSbp() {
                return null;
            }

            @Override
            public Double getMaxSbp() {
                return null;
            }

            @Override
            public Double getMinSpo2() {
                return null;
            }

            @Override
            public Double getMaxSpo2() {
                return null;
            }
        };
    }
}
