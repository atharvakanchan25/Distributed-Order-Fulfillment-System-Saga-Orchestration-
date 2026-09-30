package com.fulfillment.common.idempotency;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class IdempotencyGuard {

    private final ProcessedEventRepository repository;

    /**
     * Returns {@code true} if this event should be processed (first time seen).
     * Returns {@code false} if it is a duplicate — caller must skip processing.
     *
     * Uses INSERT-on-conflict: if two threads race on the same eventId the DB
     * unique PK constraint ensures only one wins; the other gets a
     * DataIntegrityViolationException which we catch and treat as "already done".
     */
    public boolean tryProcess(UUID eventId, String handlerName) {
        try {
            repository.saveAndFlush(ProcessedEvent.of(eventId, handlerName));
            return true;
        } catch (DataIntegrityViolationException ex) {
            log.warn("[idempotency] Duplicate event {} for handler {} — skipping", eventId, handlerName, ex);
            return false;
        }
    }
}
