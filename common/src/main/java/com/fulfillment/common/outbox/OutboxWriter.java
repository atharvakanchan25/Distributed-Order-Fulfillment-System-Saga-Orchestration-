package com.fulfillment.common.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OutboxWriter {

    private final OutboxEventRepository repository;
    private final ObjectMapper          objectMapper;

    /**
     * Serialize {@code payload} and persist an outbox row.
     * Must be called inside an active transaction so it shares the business TX.
     */
    public void write(String aggregateId, String topic, Object payload) {
        try {
            String json = objectMapper.writeValueAsString(payload);
            repository.save(OutboxEvent.of(
                    aggregateId,
                    topic,
                    payload.getClass().getName(),
                    json));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox payload", e);
        }
    }
}
