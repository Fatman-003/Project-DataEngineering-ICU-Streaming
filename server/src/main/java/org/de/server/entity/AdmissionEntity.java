package org.de.server.entity;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;
import java.util.Set;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "admissions")
public class AdmissionEntity {

    @Id
    @Column(name = "hadm_id")
    private Long hadmId;

    private LocalDateTime admittime;
    private LocalDateTime dischtime;
    private LocalDateTime deathtime;

    @Column(name = "admission_type")
    private String admissionType;

    @Column(name = "admit_provider_id")
    private String admitProviderId;

    @Column(name = "admission_location")
    private String admissionLocation;

    @Column(name = "discharge_location")
    private String dischargeLocation;

    private String insurance;
    private String language;

    @Column(name = "marital_status")
    private String maritalStatus;

    private String race;

    private LocalDateTime edregtime;
    private LocalDateTime edouttime;

    @Column(name = "hospital_expire_flag")
    private Short hospitalExpireFlag;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "subject_id")
    private PatientEntity patient;

    @OneToMany(mappedBy = "admission", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<IcuStayEntity> icuStays;
}
