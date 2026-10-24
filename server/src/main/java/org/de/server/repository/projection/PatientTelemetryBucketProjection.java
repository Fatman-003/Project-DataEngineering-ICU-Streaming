package org.de.server.repository.projection;

import java.time.Instant;

public interface PatientTelemetryBucketProjection {

    Instant getSampleTime();

    Double getHeartRate();

    Double getMap();

    Double getSbp();

    Double getSpo2();

    Double getMinHeartRate();

    Double getMaxHeartRate();

    Double getMinMap();

    Double getMaxMap();

    Double getMinSbp();

    Double getMaxSbp();

    Double getMinSpo2();

    Double getMaxSpo2();
}
