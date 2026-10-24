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

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Entity
@Table(name = "d_items")
public class DItemEntity {

    @Id
    @Column(name = "itemid")
    private Integer itemId;

    @Column(name = "label")
    private String label;

    @Column(name = "abbreviation")
    private String abbreviation;

    @Column(name = "linksto")
    private String linksto;

    @Column(name = "category")
    private String category;

    @Column(name = "unitname")
    private String unitName;

    @Column(name = "param_type")
    private String paramType;

    @Column(name = "lownormalvalue")
    private Double lowNormalValue;

    @Column(name = "highnormalvalue")
    private Double highNormalValue;
}
