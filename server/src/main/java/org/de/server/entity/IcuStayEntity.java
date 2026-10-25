package org.de.server.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "icustays")
public class IcuStayEntity {

    @Id
    @Column(name = "stay_id")
    private Long stayId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "subject_id", nullable = false)
    private PatientEntity patient;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "hadm_id", nullable = false)
    private AdmissionEntity admission;

    @Column(name = "bed_id")
    private Integer bedId;

    @Column(name = "first_careunit")
    private String firstCareunit;

    @Column(name = "last_careunit")
    private String lastCareunit;

    @Column(name = "intime")
    private LocalDateTime intime;

    @Column(name = "outtime")
    private LocalDateTime outtime;

    @Column(name = "los")
    private Double los;
}
