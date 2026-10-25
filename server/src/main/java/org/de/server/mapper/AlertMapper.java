package org.de.server.mapper;

import org.de.server.dto.response.AlertDto;
import org.de.server.dto.response.AlertSnapshotDto;
import org.de.server.entity.SepsisAlertEventEntity;
import org.de.server.kafka.message.AlertMessage;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingConstants;

import java.util.List;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface AlertMapper {

    AlertDto toDto(AlertMessage message);

    @Mapping(source = "id.alertId", target = "alertId")
    @Mapping(source = "id.alertTime", target = "alertTime")
    AlertSnapshotDto toSnapshotDto(SepsisAlertEventEntity entity);

    List<AlertSnapshotDto> toSnapshotDtoList(List<SepsisAlertEventEntity> entities);
}
