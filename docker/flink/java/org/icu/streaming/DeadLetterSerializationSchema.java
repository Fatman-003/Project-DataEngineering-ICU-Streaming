package org.icu.streaming;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

/** Serialize the complete DLQ JSON unchanged, with the documented recoverable audit key. */
public final class DeadLetterSerializationSchema implements KafkaRecordSerializationSchema<String> {
    private static final long serialVersionUID = 1L;
    private final String topic;
    private transient ObjectMapper mapper;

    public DeadLetterSerializationSchema(String topic) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("DLQ topic must be nonempty");
        }
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            String value, KafkaSinkContext context, Long timestamp) {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            JsonNode envelope = mapper.readTree(value);
            JsonNode stayId = envelope.get("stay_id");
            JsonNode originalKey = envelope.get("original_key_base64");
            byte[] key;
            if (stayId != null && stayId.isTextual() && !stayId.asText().isEmpty()) {
                key = stayId.asText().getBytes(StandardCharsets.UTF_8);
            } else if (originalKey != null && originalKey.isTextual()) {
                key = Base64.getDecoder().decode(originalKey.asText());
            } else {
                JsonNode dlqId = envelope.get("dlq_id");
                if (dlqId == null || !dlqId.isTextual() || dlqId.asText().isEmpty()) {
                    throw new IllegalArgumentException("DLQ envelope has no stable fallback key");
                }
                key = dlqId.asText().getBytes(StandardCharsets.UTF_8);
            }
            // Kafka uses send time. The original logical event-time stays inside the DLQ JSON.
            return new ProducerRecord<>(topic, key, value.getBytes(StandardCharsets.UTF_8));
        } catch (IOException error) {
            throw new IllegalArgumentException("Invalid internally generated DLQ envelope", error);
        }
    }
}
