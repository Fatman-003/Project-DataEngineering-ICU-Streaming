package org.de.server.entity.id;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.time.OffsetDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
@EqualsAndHashCode
public class PatientTelemetryId implements Serializable {

    @Column(name = "sample_time", nullable = false)
    private OffsetDateTime sampleTime;

    @Column(name = "stay_id", nullable = false)
    private Long stayId;
}
