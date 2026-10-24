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
import org.de.server.entity.id.VitalsSlidingWindow5mId;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "vitals_sliding_window_5m")
public class VitalsSlidingWindow5mEntity {

    @EmbeddedId
    private VitalsSlidingWindow5mId id;

    @Column(name = "avg_heart_rate")
    private Double avgHeartRate;

    @Column(name = "avg_map")
    private Double avgMap;

    @Column(name = "avg_sbp")
    private Double avgSbp;

    @Column(name = "avg_spo2")
    private Double avgSpo2;
}
