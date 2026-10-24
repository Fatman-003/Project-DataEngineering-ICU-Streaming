package org.de.server.exception;

import lombok.Getter;

@Getter
public class IcuStayNotFoundException extends ResourceNotFoundException {

    private final Long stayId;

    public IcuStayNotFoundException(Long stayId) {
        super("ICU stay not found: " + stayId);
        this.stayId = stayId;
    }

}
