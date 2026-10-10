package org.icu.streaming;

import java.io.IOException;
import java.util.Base64;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

/** Preserve the Kafka record metadata and original bytes before Python JSON parsing. */
public final class KafkaMetadataDeserializer implements KafkaRecordDeserializationSchema<String> {
    private static final long serialVersionUID = 1L;
    private transient ObjectMapper mapper;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out)
            throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        ObjectNode envelope = mapper.createObjectNode();
        envelope.put("topic", record.topic());
        envelope.put("partition", record.partition());
        envelope.put("offset", record.offset());
        if (record.key() == null) {
            envelope.putNull("key_base64");
        } else {
            envelope.put("key_base64", Base64.getEncoder().encodeToString(record.key()));
        }
        if (record.value() == null) {
            envelope.putNull("payload_base64");
        } else {
            envelope.put("payload_base64", Base64.getEncoder().encodeToString(record.value()));
        }
        out.collect(mapper.writeValueAsString(envelope));
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return Types.STRING;
    }
}
