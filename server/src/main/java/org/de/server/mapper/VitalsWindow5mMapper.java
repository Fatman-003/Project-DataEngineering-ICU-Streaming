package org.de.server.mapper;

import org.de.server.dto.response.VitalsWindow5mDto;
import org.de.server.entity.VitalsSlidingWindow5mEntity;
import org.de.server.kafka.message.VitalsWindow5mMessage;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;

import java.util.List;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface VitalsWindow5mMapper {

    VitalsWindow5mDto toDto(VitalsWindow5mMessage message);

    @Mapping(source = "id.windowEnd", target = "windowEnd")
    @Mapping(source = "id.stayId", target = "stayId")
    VitalsWindow5mDto toDtoFromEntity(VitalsSlidingWindow5mEntity entity);

    List<VitalsWindow5mDto> toDtoList(List<VitalsSlidingWindow5mEntity> entities);
}
