package com.fulfillment.common.outbox;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

/**
 * Polls the outbox table every 500 ms and forwards unpublished rows to Kafka.
 *
 * Uses a String-valued KafkaTemplate so the pre-serialized JSON payload stored
 * in the outbox is sent as-is without double-serialization.
 *
 * WHY POLLING OUTBOX OVER DEBEZIUM CDC?
 * Debezium reads the Postgres WAL and streams changes to Kafka with near-zero
 * latency and no polling overhead — ideal for very high throughput. However it
 * requires running a Kafka Connect cluster, a connector config per service, and
 * careful WAL retention tuning. For this system (5 services, moderate load) the
 * operational cost outweighs the benefit. A 500 ms polling interval adds at most
 * half a second of end-to-end latency, which is acceptable. If throughput grows
 * to tens of thousands of orders/second, migrating to Debezium is the right call.
 */
@Slf4j
@Component
public class OutboxPoller {

    private final OutboxEventRepository         repository;
    private final KafkaTemplate<String, String> stringKafkaTemplate;

    public OutboxPoller(OutboxEventRepository repository,
                        @Qualifier("outboxStringKafkaTemplate") KafkaTemplate<String, String> stringKafkaTemplate) {
        this.repository          = repository;
        this.stringKafkaTemplate = stringKafkaTemplate;
    }

    @Scheduled(fixedDelay = 500)
    @Transactional
    public void poll() {
        var pending = repository.findUnpublished();
        if (pending.isEmpty()) return;

        for (OutboxEvent event : pending) {
            try {
                var record = new ProducerRecord<>(
                        event.getTopic(),
                        null,
                        event.getAggregateId(),
                        event.getPayload());
                // Carry the type header so consumers can deserialize correctly
                record.headers().add(new RecordHeader(
                        "__TypeId__",
                        event.getPayloadType().getBytes(StandardCharsets.UTF_8)));

                stringKafkaTemplate.send(record).get(); // sync — we want the exception if it fails
                event.markPublished();
                repository.save(event);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                log.error("[outbox] Interrupted while publishing event {}", event.getId(), ex);
            } catch (Exception ex) {
                log.error("[outbox] Failed to publish event {} to {}: {}",
                        event.getId(), event.getTopic(), ex.getMessage(), ex);
                // Leave publishedAt null — will retry on next poll
            }
        }
    }
}
