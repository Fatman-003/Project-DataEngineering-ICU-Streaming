package org.de.server.repository;

import org.de.server.entity.PatientTelemetryStreamEntity;
import org.de.server.entity.id.PatientTelemetryId;
import org.de.server.repository.projection.PatientTelemetryBucketProjection;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface PatientTelemetryStreamRepository extends
        JpaRepository<PatientTelemetryStreamEntity, PatientTelemetryId>,
        JpaSpecificationExecutor<PatientTelemetryStreamEntity> {

    @Query(
            "SELECT max(p.id.sampleTime) FROM PatientTelemetryStreamEntity p " +
            "WHERE p.id.stayId = :stayId"
    )
    Optional<OffsetDateTime> findLatestSampleTime(@Param("stayId") Long stayId);

    @Query(
            value = """
                    SELECT time_bucket(make_interval(secs => :bucketSeconds), sample_time) AS sampleTime,
                           avg(heart_rate) AS heartRate,
                           avg(map) AS map,
                           avg(sbp) AS sbp,
                           avg(spo2) AS spo2,
                           min(heart_rate) AS minHeartRate,
                           max(heart_rate) AS maxHeartRate,
                           min(map) AS minMap,
                           max(map) AS maxMap,
                           min(sbp) AS minSbp,
                           max(sbp) AS maxSbp,
                           min(spo2) AS minSpo2,
                           max(spo2) AS maxSpo2
                    FROM patient_telemetry_stream
                    WHERE stay_id = :stayId
                      AND sample_time >= :from
                      AND sample_time < :to
                    GROUP BY sampleTime
                    ORDER BY sampleTime ASC
                    LIMIT :maxPoints
                    """,
            nativeQuery = true
    )
    List<PatientTelemetryBucketProjection> aggregateByTimeBucket(
            @Param("stayId") Long stayId,
            @Param("from") OffsetDateTime from,
            @Param("to") OffsetDateTime to,
            @Param("bucketSeconds") int bucketSeconds,
            @Param("maxPoints") int maxPoints
    );
}
