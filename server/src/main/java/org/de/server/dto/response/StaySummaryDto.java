package org.de.server.dto.response;

import java.time.LocalDateTime;

public record StaySummaryDto(
        Long stayId,
        Long subjectId,
        Long hadmId,
        String gender,
        Integer age,
        String bedId,
        String firstCareunit,
        String lastCareunit,
        LocalDateTime admissionTime,
        LocalDateTime dischargeTime,
        LocalDateTime hospitalAdmissionTime,
        LocalDateTime hospitalDischargeTime,
        Double lengthOfStay,
        String admissionType,
        String admissionLocation
) {
}
