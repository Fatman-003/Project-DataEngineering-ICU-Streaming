package org.de.server.mapper;

import org.de.server.dto.response.PatientTelemetryDto;
import org.de.server.entity.PatientTelemetryStreamEntity;
import org.de.server.kafka.message.TelemetryMessage;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;

import java.util.List;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface PatientTelemetryMapper {

    PatientTelemetryDto toDto(TelemetryMessage message);

    @Mapping(source = "id.stayId", target = "stayId")
    @Mapping(source = "id.sampleTime", target = "sampleTime")
    PatientTelemetryDto toDtoFromEntity(PatientTelemetryStreamEntity entity);

    List<PatientTelemetryDto> toDtoList(List<PatientTelemetryStreamEntity> entities);
}
