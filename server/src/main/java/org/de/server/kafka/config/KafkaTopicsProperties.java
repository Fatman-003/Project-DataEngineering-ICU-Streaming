package org.de.server.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.kafka.topics")
public record KafkaTopicsProperties(
        String telemetry,
        String vitals5m,
        String alerts,
        String deadLetter) {
}
