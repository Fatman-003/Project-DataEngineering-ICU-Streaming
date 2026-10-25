package org.de.server.kafka.config;

import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.databind.JsonNode;

import org.de.server.kafka.message.TelemetryMessage;
import org.de.server.kafka.message.VitalsWindow5mMessage;

@Configuration
@EnableConfigurationProperties(KafkaTopicsProperties.class)
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;
    private final SslBundles sslBundles;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties, SslBundles sslBundles) {
        this.kafkaProperties = kafkaProperties;
        this.sslBundles = sslBundles;
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler(
            KafkaOperations<Object, Object> kafkaTemplate,
            KafkaTopicsProperties topics) {

        var recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (rc, ex) -> new TopicPartition(topics.deadLetter(), rc.partition()));

        return new DefaultErrorHandler(recoverer, new FixedBackOff(1_000L, 3));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, TelemetryMessage> telemetryContainerFactory(
            DefaultErrorHandler kafkaErrorHandler) {
        return buildContainerFactory(TelemetryMessage.class, kafkaErrorHandler);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VitalsWindow5mMessage> vitalsContainerFactory(
            DefaultErrorHandler kafkaErrorHandler) {
        return buildContainerFactory(VitalsWindow5mMessage.class, kafkaErrorHandler);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, JsonNode> alertsContainerFactory(
            DefaultErrorHandler kafkaErrorHandler) {
        return buildContainerFactory(JsonNode.class, kafkaErrorHandler);
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> buildContainerFactory(
            Class<T> messageType, DefaultErrorHandler errorHandler) {

        var factory = new ConcurrentKafkaListenerContainerFactory<String, T>();
        factory.setConsumerFactory(buildConsumerFactory(messageType));
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

    @SuppressWarnings("java:S2095")
    private <T> DefaultKafkaConsumerFactory<String, T> buildConsumerFactory(Class<T> messageType) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(sslBundles);

        var jsonDeserializer = new JsonDeserializer<>(messageType)
                .ignoreTypeHeaders()
                .trustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(
                props,
                new StringDeserializer(),
                new ErrorHandlingDeserializer<>(jsonDeserializer));
    }
}
