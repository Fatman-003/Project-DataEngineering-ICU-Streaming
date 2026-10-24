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
            if (stayId == null || stayId.isNull()) {
                stayId = envelope.get("mapped_stay_id");
            }
            JsonNode originalKey = envelope.get("kafka_key");
            byte[] key;
            if (stayId != null && (stayId.isIntegralNumber() || stayId.isTextual()) && !stayId.asText().isEmpty()) {
                key = stayId.asText().getBytes(StandardCharsets.UTF_8);
            } else if (originalKey != null && originalKey.isTextual()) {
                key = originalKey.asText().getBytes(StandardCharsets.UTF_8);
            } else {
                JsonNode dlqId = envelope.get("dlq_id");
                if (dlqId == null || !dlqId.isTextual() || dlqId.asText().isEmpty()) {
                    throw new IllegalArgumentException("DLQ envelope has no stable fallback key");
                }
                key = dlqId.asText().getBytes(StandardCharsets.UTF_8);
            }
            // Kafka uses send time. The original logical event-time stays inside the DLQ JSON.
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, key, value.getBytes(StandardCharsets.UTF_8));
            record.headers().add("content-type", "application/json; charset=utf-8".getBytes(StandardCharsets.UTF_8));
            String[][] fields = {{"schema-version", "schema_version"}, {"message-id", "message_id"},
                {"event-type", "event_type"}, {"producer", "producer"}, {"trace-id", "trace_id"},
                {"dlq-source-topic", "source_topic"}, {"dlq-error-code", "error_code"}};
            for (String[] field : fields) {
                JsonNode item = envelope.get(field[1]);
                if (item != null && !item.isNull()) {
                    record.headers().add(field[0], item.asText().getBytes(StandardCharsets.UTF_8));
                }
            }
            return record;
        } catch (IOException error) {
            throw new IllegalArgumentException("Invalid internally generated DLQ envelope", error);
        }
    }
}
