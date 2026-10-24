package org.de.server.mapper;

import org.de.server.dto.response.StaySummaryDto;
import org.de.server.entity.IcuStayEntity;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;

import java.util.List;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface StayMapper {

    @Mapping(source = "patient.subjectId", target = "subjectId")
    @Mapping(source = "patient.gender", target = "gender")
    @Mapping(source = "patient.anchorAge", target = "age")
    @Mapping(source = "admission.hadmId", target = "hadmId")
    @Mapping(source = "intime", target = "admissionTime")
    @Mapping(source = "outtime", target = "dischargeTime")
    @Mapping(source = "admission.admittime", target = "hospitalAdmissionTime")
    @Mapping(source = "admission.dischtime", target = "hospitalDischargeTime")
    @Mapping(source = "los", target = "lengthOfStay")
    @Mapping(source = "admission.admissionType", target = "admissionType")
    @Mapping(source = "admission.admissionLocation", target = "admissionLocation")
    StaySummaryDto toDto(IcuStayEntity entity);

    List<StaySummaryDto> toDtoList(List<IcuStayEntity> entities);
}
