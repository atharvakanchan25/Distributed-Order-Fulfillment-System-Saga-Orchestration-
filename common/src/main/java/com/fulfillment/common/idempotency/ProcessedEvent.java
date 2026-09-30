package com.fulfillment.common.idempotency;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

/**
 * Records every event/command ID that this service has successfully processed.
 * Before acting on a message, the handler checks this table.
 * If the ID is already present, the message is a duplicate and is skipped.
 */
@Entity
@Table(name = "processed_events")
@Getter
@NoArgsConstructor
public class ProcessedEvent {

    @Id
    private UUID eventId;

    @Column(nullable = false)
    private String handlerName;

    @Column(nullable = false, updatable = false)
    private Instant processedAt;

    public static ProcessedEvent of(UUID eventId, String handlerName) {
        var e = new ProcessedEvent();
        e.eventId     = eventId;
        e.handlerName = handlerName;
        e.processedAt = Instant.now();
        return e;
    }
}
