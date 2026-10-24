package org.de.server.entity;

import jakarta.persistence.Column;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Entity;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.de.server.entity.id.PatientTelemetryId;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "patient_telemetry_stream")
public class PatientTelemetryStreamEntity {

    @EmbeddedId
    private PatientTelemetryId id;

    @Column(name = "heart_rate")
    private Double heartRate;

    @Column(name = "map")
    private Double map;

    @Column(name = "sbp")
    private Double sbp;

    @Column(name = "spo2")
    private Double spo2;
}
