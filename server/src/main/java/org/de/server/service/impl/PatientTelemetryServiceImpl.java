package org.de.server.service.impl;

import lombok.RequiredArgsConstructor;
import org.de.server.dto.request.TelemetrySearchCriteria;
import org.de.server.dto.response.PatientTelemetryDto;
import org.de.server.dto.response.TelemetryHistoryResponse;
import org.de.server.exception.IcuStayNotFoundException;
import org.de.server.repository.IcuStayRepository;
import org.de.server.repository.PatientTelemetryStreamRepository;
import org.de.server.repository.projection.PatientTelemetryBucketProjection;
import org.de.server.service.PatientTelemetryService;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Service
@RequiredArgsConstructor
public class PatientTelemetryServiceImpl implements PatientTelemetryService {

    private static final Duration DEFAULT_RANGE = Duration.ofMinutes(5);
    private static final int DEFAULT_MAX_POINTS = 600;
    private static final int MAX_POINTS_LIMIT = 2000;

    private final PatientTelemetryStreamRepository telemetryRepository;
    private final IcuStayRepository icuStayRepository;

    @Override
    public TelemetryHistoryResponse getTelemetryHistory(TelemetrySearchCriteria criteria) {
        Long stayId = criteria.stayId();
        if (stayId == null) {
            throw new IllegalArgumentException("stayId is required");
        }
        if (!icuStayRepository.existsById(stayId)) {
            throw new IcuStayNotFoundException(stayId);
        }

        OffsetDateTime requestedTo = criteria.to();
        OffsetDateTime requestedFrom = criteria.from();
        Duration range = resolveRange(criteria.rangeMinutes());

        if (requestedTo == null) {
            requestedTo = telemetryRepository.findLatestSampleTime(stayId).orElse(null);
        }

        if (requestedTo == null) {
            return new TelemetryHistoryResponse(
                    stayId,
                    requestedFrom,
                    null,
                    criteria.rangeMinutes(),
                    0,
                    false,
                    List.of()
            );
        }

        if (requestedFrom == null) {
            requestedFrom = requestedTo.minus(range);
        }

        if (!requestedFrom.isBefore(requestedTo)) {
            throw new IllegalArgumentException("from must be before to");
        }

        int maxPoints = normalizeMaxPoints(criteria.maxPoints());
        int bucketSeconds = resolveBucketSeconds(criteria.bucketSeconds(), requestedFrom, requestedTo, maxPoints);
        List<PatientTelemetryDto> points = telemetryRepository
                .aggregateByTimeBucket(stayId, requestedFrom, requestedTo, bucketSeconds, maxPoints)
                .stream()
                .map(point -> toDto(stayId, point))
                .toList();

        return new TelemetryHistoryResponse(
                stayId,
                requestedFrom,
                requestedTo,
                criteria.rangeMinutes(),
                bucketSeconds,
                true,
                points
        );
    }

    private PatientTelemetryDto toDto(Long stayId, PatientTelemetryBucketProjection projection) {
        return new PatientTelemetryDto(
                stayId,
                projection.getSampleTime().atOffset(ZoneOffset.UTC),
                projection.getHeartRate(),
                projection.getMap(),
                projection.getSbp(),
                projection.getSpo2(),
                projection.getMinHeartRate(),
                projection.getMaxHeartRate(),
                projection.getMinMap(),
                projection.getMaxMap(),
                projection.getMinSbp(),
                projection.getMaxSbp(),
                projection.getMinSpo2(),
                projection.getMaxSpo2()
        );
    }

    private Duration resolveRange(Integer rangeMinutes) {
        if (rangeMinutes == null) {
            return DEFAULT_RANGE;
        }
        if (rangeMinutes <= 0) {
            throw new IllegalArgumentException("rangeMinutes must be greater than 0");
        }
        return Duration.ofMinutes(rangeMinutes);
    }

    private int normalizeMaxPoints(Integer maxPoints) {
        if (maxPoints == null || maxPoints <= 0) {
            return DEFAULT_MAX_POINTS;
        }
        return Math.min(maxPoints, MAX_POINTS_LIMIT);
    }

    private int resolveBucketSeconds(
            Integer bucketSeconds,
            OffsetDateTime from,
            OffsetDateTime to,
            int maxPoints
    ) {
        if (bucketSeconds != null) {
            if (bucketSeconds <= 0) {
                throw new IllegalArgumentException("bucketSeconds must be greater than 0");
            }
            return bucketSeconds;
        }

        long rangeSeconds = Duration.between(from, to).getSeconds();
        return (int) Math.max(1, (rangeSeconds + maxPoints - 1) / maxPoints);
    }
}
