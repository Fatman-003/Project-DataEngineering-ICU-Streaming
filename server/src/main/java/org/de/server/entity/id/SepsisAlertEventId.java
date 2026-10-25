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
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Embeddable
@EqualsAndHashCode
public class SepsisAlertEventId implements Serializable {

    @Column(name = "alert_time", nullable = false)
    private OffsetDateTime alertTime;

    @Column(name = "alert_id", nullable = false)
    private UUID alertId;

}
