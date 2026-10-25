package org.de.server.mapper;

import org.de.server.dto.response.AlertStatusChangeDto;
import org.de.server.kafka.message.AlertStatusMessage;
import org.mapstruct.Mapper;
import org.mapstruct.MappingConstants;

@Mapper(componentModel = MappingConstants.ComponentModel.SPRING)
public interface AlertStatusChangeMapper {

    AlertStatusChangeDto toDto(AlertStatusMessage message);
}
