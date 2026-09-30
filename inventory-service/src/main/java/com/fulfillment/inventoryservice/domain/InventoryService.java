package com.fulfillment.inventoryservice.domain;

import com.fulfillment.common.events.*;
import com.fulfillment.common.idempotency.IdempotencyGuard;
import com.fulfillment.common.outbox.OutboxWriter;
import jakarta.persistence.OptimisticLockException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class InventoryService {

    private final StockItemRepository stockItemRepository;
    private final OutboxWriter         outboxWriter;
    private final IdempotencyGuard     idempotencyGuard;

    // Deterministic idempotency keys — survive retries, unique per operation
    private static UUID reserveKey(UUID orderId) {
        return UUID.nameUUIDFromBytes(("reserve:" + orderId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
    private static UUID releaseKey(UUID orderId) {
        return UUID.nameUUIDFromBytes(("release:" + orderId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * @Retryable catches OptimisticLockException thrown when two concurrent
     * transactions both read the same @Version and one loses the race.
     * The loser retries the whole transaction — re-reads the updated stock,
     * re-checks quantity, and either succeeds or publishes a failure event.
     * This replaces SELECT FOR UPDATE and avoids holding a row lock across
     * the full Kafka round-trip.
     */
    @Retryable(retryFor = OptimisticLockException.class,
               maxAttempts = 5, backoff = @Backoff(delay = 50, multiplier = 2))
    @Transactional
    public void handleReserveInventory(ReserveInventoryCommand cmd) {
        if (!idempotencyGuard.tryProcess(reserveKey(cmd.orderId()), "reserve-inventory")) return;

        log.info("[inventory] ReserveInventory orderId={} qty={}", cmd.orderId(), cmd.quantity());
        var stockOpt = stockItemRepository.findByItemId(cmd.itemId());

        if (stockOpt.isEmpty()) {
            outboxWriter.write(cmd.orderId().toString(), KafkaTopics.INVENTORY_EVENTS,
                    new InventoryReservationFailedEvent(cmd.orderId(),
                            "Item not found: " + cmd.itemId(), Instant.now()));
            return;
        }

        StockItem stock = stockOpt.get();
        if (!stock.reserve(cmd.quantity())) {
            log.warn("[inventory] Insufficient stock for item {}", cmd.itemId());
            outboxWriter.write(cmd.orderId().toString(), KafkaTopics.INVENTORY_EVENTS,
                    new InventoryReservationFailedEvent(cmd.orderId(), "Insufficient stock", Instant.now()));
            return;
        }

        // @Version incremented here — throws OptimisticLockException on concurrent conflict
        stockItemRepository.save(stock);
        outboxWriter.write(cmd.orderId().toString(), KafkaTopics.INVENTORY_EVENTS,
                new InventoryReservedEvent(cmd.orderId(), cmd.customerId(), cmd.itemId(),
                        cmd.quantity(), cmd.totalAmount(), Instant.now()));
    }

    @Transactional
    public void handleReleaseInventory(ReleaseInventoryCommand cmd) {
        if (!idempotencyGuard.tryProcess(releaseKey(cmd.orderId()), "release-inventory")) return;

        log.info("[inventory] ReleaseInventory orderId={} qty={}", cmd.orderId(), cmd.quantity());
        stockItemRepository.findByItemId(cmd.itemId()).ifPresent(stock -> {
            stock.release(cmd.quantity());
            stockItemRepository.save(stock);
        });

        outboxWriter.write(cmd.orderId().toString(), KafkaTopics.INVENTORY_EVENTS,
                new InventoryReleasedEvent(cmd.orderId(), Instant.now()));
    }
}
