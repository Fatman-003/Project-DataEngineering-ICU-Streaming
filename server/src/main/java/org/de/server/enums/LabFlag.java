package org.de.server.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum LabFlag {
    NORMAL("normal"),
    HIGH("high"),
    LOW("low"),
    CRITICAL("critical"),
    UNKNOWN("unknown");

    private final String value;

    LabFlag(String value) {
        this.value = value;
    }

    @JsonValue
    public String value() {
        return value;
    }

    @JsonCreator
    public static LabFlag fromValue(String value) {
        for (LabFlag flag : values()) {
            if (flag.value.equalsIgnoreCase(value) || flag.name().equalsIgnoreCase(value)) {
                return flag;
            }
        }
        throw new IllegalArgumentException("Unknown lab flag: " + value);
    }
}
