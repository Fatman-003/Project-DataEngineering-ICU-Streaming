package org.de.server.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.de.server.kafka.message.VitalsWindow5mMessage;
import org.de.server.mapper.VitalsWindow5mMapper;
import org.de.server.websocket.RealtimePublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class VitalsWindow5mConsumer {

    private final VitalsWindow5mMapper mapper;
    private final RealtimePublisher publisher;

    @KafkaListener(
        topics = "${app.kafka.topics.vitals-5m}",
        containerFactory = "vitalsContainerFactory"
    )
    public void onVitalsWindow5mData(VitalsWindow5mMessage message) {
        publisher.publishVitals5m(mapper.toDto(message));
        log.debug("Published 5m vitals window: stayId={}, windowEnd={}, avgHr={}, avgMap={}, avgSbp={}, avgSpo2={}",
                message.stayId(), message.windowEnd(), message.avgHeartRate(),
                message.avgMap(), message.avgSbp(), message.avgSpo2());
    }
}
