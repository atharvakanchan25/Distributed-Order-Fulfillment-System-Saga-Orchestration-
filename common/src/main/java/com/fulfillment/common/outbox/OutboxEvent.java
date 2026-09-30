package com.fulfillment.common.outbox;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * One row per message that must reach Kafka.
 * Written atomically with the business change; the OutboxPoller publishes it
 * and marks it done. This eliminates the dual-write race where the DB commit
 * succeeds but kafkaTemplate.send() fails (or vice-versa).
 */
@Entity
@Table(name = "outbox_events",
       indexes = @Index(name = "idx_outbox_unpublished", columnList = "published_at"))
@Getter
@NoArgsConstructor
public class OutboxEvent {

    @Id
    private UUID id;

    /** orderId — used as the Kafka partition key so all events for one order are ordered. */
    @Column(nullable = false)
    private String aggregateId;

    @Column(nullable = false)
    private String topic;

    /** Fully-qualified class name written as the Spring Kafka type header. */
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payloadType;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    /** null = pending; non-null = published. Poller filters on IS NULL. */
    @Column
    private Instant publishedAt;

    public static OutboxEvent of(String aggregateId, String topic,
                                 String payloadType, String payload) {
        var e = new OutboxEvent();
        e.id          = UUID.randomUUID();
        e.aggregateId = aggregateId;
        e.topic       = topic;
        e.payloadType = payloadType;
        e.payload     = payload;
        e.createdAt   = Instant.now();
        return e;
    }

    public void markPublished() {
        this.publishedAt = Instant.now();
    }
}
