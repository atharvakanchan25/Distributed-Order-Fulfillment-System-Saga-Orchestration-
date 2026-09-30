package com.fulfillment.orderservice.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor
public class Order {

    public enum Status { PENDING, CONFIRMED, FAILED }

    @Id
    private UUID id;

    @Column(nullable = false)
    private UUID customerId;

    @Column(nullable = false)
    private UUID itemId;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal totalPrice;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Status status;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    public static Order create(UUID customerId, UUID itemId, int quantity, BigDecimal totalPrice) {
        var o = new Order();
        o.id         = UUID.randomUUID();
        o.customerId = customerId;
        o.itemId     = itemId;
        o.quantity   = quantity;
        o.totalPrice = totalPrice;
        o.status     = Status.PENDING;
        o.createdAt  = Instant.now();
        o.updatedAt  = o.createdAt;
        return o;
    }

    /**
     * Allowed transitions:
     *   PENDING → CONFIRMED
     *   PENDING → FAILED
     * Everything else is illegal — terminal states are final.
     */
    public void transitionTo(Status next) {
        if (this.status == Status.PENDING
                && (next == Status.CONFIRMED || next == Status.FAILED)) {
            this.status    = next;
            this.updatedAt = Instant.now();
            return;
        }
        throw new IllegalStateException(
                "Cannot transition order from %s to %s".formatted(this.status, next));
    }
}
