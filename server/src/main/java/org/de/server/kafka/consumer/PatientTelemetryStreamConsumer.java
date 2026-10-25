package org.de.server.kafka.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.de.server.kafka.message.TelemetryMessage;
import org.de.server.mapper.PatientTelemetryMapper;
import org.de.server.websocket.RealtimePublisher;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PatientTelemetryStreamConsumer {

    private final PatientTelemetryMapper mapper;
    private final RealtimePublisher publisher;

    @KafkaListener(
        topics = "${app.kafka.topics.telemetry}",
        containerFactory = "telemetryContainerFactory"
    )
    public void onTelemetryData(TelemetryMessage message) {
        publisher.publishTelemetry(mapper.toDto(message));
        log.debug("Published telemetry: stayId={}, sampleTime={}, hr={}, map={}, sbp={}, spo2={}",
                message.stayId(), message.sampleTime(), message.heartRate(),
                message.map(), message.sbp(), message.spo2());
    }
}
