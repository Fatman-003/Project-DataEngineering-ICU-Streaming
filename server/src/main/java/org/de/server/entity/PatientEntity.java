package org.de.server.entity;

import jakarta.persistence.CascadeType;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.OneToMany;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDate;
import java.util.Set;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "patients")
public class PatientEntity {

    @Id
    @Column(name = "subject_id")
    private Long subjectId;

    private String gender;

    @Column(name = "anchor_age")
    private Integer anchorAge;

    @Column(name = "anchor_year")
    private Integer anchorYear;

    @Column(name = "anchor_year_group")
    private String anchorYearGroup;

    private LocalDate dod;

    @OneToMany(mappedBy = "patient", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<AdmissionEntity> admissions;

    @OneToMany(mappedBy = "patient", cascade = CascadeType.ALL, orphanRemoval = true)
    private Set<IcuStayEntity> icuStays;
}
