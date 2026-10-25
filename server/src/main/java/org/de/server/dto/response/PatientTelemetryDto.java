package org.de.server.dto.response;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.OffsetDateTime;

// The record doubles as the WebSocket realtime payload at thousands of
// messages per second; bucket min/max fields stay out of the wire unless set.
@JsonInclude(JsonInclude.Include.NON_NULL)
public record PatientTelemetryDto(
        Long stayId,
        OffsetDateTime sampleTime,
        Double heartRate,
        Double map,
        Double sbp,
        Double spo2,
        Double minHeartRate,
        Double maxHeartRate,
        Double minMap,
        Double maxMap,
        Double minSbp,
        Double maxSbp,
        Double minSpo2,
        Double maxSpo2
) {

    public PatientTelemetryDto(
            Long stayId,
            OffsetDateTime sampleTime,
            Double heartRate,
            Double map,
            Double sbp,
            Double spo2
    ) {
        this(stayId, sampleTime, heartRate, map, sbp, spo2,
                null, null, null, null, null, null, null, null);
    }
}
