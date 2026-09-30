package com.fulfillment.inventoryservice.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "stock_items")
@Getter
@NoArgsConstructor
public class StockItem {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false, unique = true)
    private UUID itemId;

    @Column(nullable = false)
    private int quantity;

    /**
     * Optimistic locking version. JPA increments this on every UPDATE.
     * If two concurrent transactions both read version=5 and try to decrement,
     * the second UPDATE will find version=6 (set by the first) and throw
     * OptimisticLockException — preventing an oversell without a SELECT FOR UPDATE.
     *
     * Relationship to the Redis reservation lock from the spec:
     * Redis gives us a fast pre-check ("is there likely stock?") before we even
     * hit the DB, reducing contention under high load. The @Version column is the
     * authoritative safety net — it guarantees correctness even if Redis is stale
     * or unavailable. Use both: Redis as a performance optimisation, @Version as
     * the correctness guarantee.
     */
    @Version
    private long version;

    @Column(nullable = false)
    private Instant updatedAt;

    /** Returns true and decrements if sufficient stock exists, false otherwise. */
    public boolean reserve(int qty) {
        if (this.quantity < qty) return false;
        this.quantity -= qty;
        this.updatedAt = Instant.now();
        return true;
    }

    public void release(int qty) {
        this.quantity += qty;
        this.updatedAt = Instant.now();
    }
}
