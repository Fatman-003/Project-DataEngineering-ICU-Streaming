package org.de.server.kafka.consumer;

import org.de.server.dto.response.VitalsWindow5mDto;
import org.de.server.kafka.message.EventType;
import org.de.server.kafka.message.VitalsWindow5mMessage;
import org.de.server.mapper.VitalsWindow5mMapper;
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
class VitalsWindow5mConsumerTest {

    private static final UUID MESSAGE_ID = UUID.fromString("019a3f4b-7c2d-7e1f-8a9b-0c1d2e3f4a5b");
    private static final OffsetDateTime PRODUCED_AT = OffsetDateTime.parse("2026-10-20T23:35:01.120Z");
    private static final OffsetDateTime WINDOW_END = OffsetDateTime.parse("2026-10-20T23:35:00Z");

    @Mock
    private VitalsWindow5mMapper mapper;

    @Mock
    private RealtimePublisher publisher;

    @InjectMocks
    private VitalsWindow5mConsumer consumer;

    @Test
    void publishesMappedVitalsWindow() {
        VitalsWindow5mMessage message = new VitalsWindow5mMessage(
                "1.0",
                MESSAGE_ID,
                EventType.VITALS_WINDOW_5M,
                PRODUCED_AT,
                "icu-flink",
                null,
                WINDOW_END,
                1000001L,
                87.4,
                71.2,
                116.8,
                96.9);
        VitalsWindow5mDto dto = new VitalsWindow5mDto(
                WINDOW_END, 1000001L, 87.4, 71.2, 116.8, 96.9);

        when(mapper.toDto(message)).thenReturn(dto);

        consumer.onVitalsWindow5mData(message);

        verify(mapper).toDto(message);
        verify(publisher).publishVitals5m(dto);
        verifyNoMoreInteractions(mapper, publisher);
    }

    @Test
    void listensToVitalsTopicWithDedicatedContainerFactory() throws NoSuchMethodException {
        KafkaListener listener = VitalsWindow5mConsumer.class
                .getMethod("onVitalsWindow5mData", VitalsWindow5mMessage.class)
                .getAnnotation(KafkaListener.class);

        assertArrayEquals(new String[]{"${app.kafka.topics.vitals-5m}"}, listener.topics());
        assertEquals("vitalsContainerFactory", listener.containerFactory());
    }
}
