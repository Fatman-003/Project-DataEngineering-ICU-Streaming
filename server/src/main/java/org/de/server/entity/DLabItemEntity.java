package org.de.server.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "d_labitems")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DLabItemEntity {

    @Id
    @Column(name = "itemid")
    private Integer itemId;

    @Column(name = "label")
    private String label;

    @Column(name = "fluid")
    private String fluid;

    @Column(name = "category")
    private String category;
}