package org.de.server.kafka.consumer;

import org.de.server.dto.response.PatientTelemetryDto;
import org.de.server.kafka.message.EventType;
import org.de.server.kafka.message.TelemetryMessage;
import org.de.server.mapper.PatientTelemetryMapper;
import org.de.server.websocket.RealtimePublisher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.annotation.KafkaListener;

import java.time.OffsetDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PatientTelemetryStreamConsumerTest {

    private static final UUID MESSAGE_ID = UUID.fromString("019a3f4b-7c2d-7e1f-8a9b-0c1d2e3f4a5b");
    private static final OffsetDateTime PRODUCED_AT = OffsetDateTime.parse("2026-10-20T23:30:01.120Z");
    private static final OffsetDateTime SAMPLE_TIME = OffsetDateTime.parse("2026-10-20T23:30:01Z");

    @Mock
    private PatientTelemetryMapper mapper;

    @Mock
    private RealtimePublisher publisher;

    @InjectMocks
    private PatientTelemetryStreamConsumer consumer;

    @Test
    void publishesMappedTelemetry() {
        TelemetryMessage message = new TelemetryMessage(
                "1.0",
                MESSAGE_ID,
                EventType.TELEMETRY_SAMPLE,
                PRODUCED_AT,
                "icu-simulator",
                "4bf92f3577b34da6a3ce929d0e0e4736",
                SAMPLE_TIME,
                1000001L,
                88.0,
                72.0,
                118.0,
                97.0);
        PatientTelemetryDto dto = new PatientTelemetryDto(
                1000001L, SAMPLE_TIME, 88.0, 72.0, 118.0, 97.0);

        when(mapper.toDto(message)).thenReturn(dto);

        consumer.onTelemetryData(message);

        verify(mapper).toDto(message);
        verify(publisher).publishTelemetry(dto);
        verifyNoMoreInteractions(mapper, publisher);
    }

    @Test
    void listensToTelemetryTopicWithDedicatedContainerFactory() throws NoSuchMethodException {
        KafkaListener listener = PatientTelemetryStreamConsumer.class
                .getMethod("onTelemetryData", TelemetryMessage.class)
                .getAnnotation(KafkaListener.class);

        assertArrayEquals(new String[]{"${app.kafka.topics.telemetry}"}, listener.topics());
        assertEquals("telemetryContainerFactory", listener.containerFactory());
    }
}
