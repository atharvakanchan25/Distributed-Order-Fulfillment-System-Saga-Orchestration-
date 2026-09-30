package com.fulfillment.sagaorchestrator.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "order_saga_state")
@Getter
@NoArgsConstructor
public class OrderSagaState {

    @Id
    private UUID orderId;

    @Column(nullable = false)
    private UUID customerId;

    @Column(nullable = false)
    private UUID itemId;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Setter
    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 40)
    private SagaState state;

    @Setter
    @Column
    private String failureReason;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Setter
    @Column(nullable = false)
    private Instant updatedAt;

    /** Updated on every state transition — used by the timeout checker. */
    @Setter
    @Column(nullable = false)
    private Instant lastStepAt;

    /** True once this saga has been sent to the DLQ topic for manual intervention. */
    @Setter
    @Column(nullable = false)
    private boolean dlqSent;

    public static OrderSagaState create(UUID orderId, UUID customerId, UUID itemId,
                                        int quantity, BigDecimal totalAmount) {
        var s = new OrderSagaState();
        s.orderId     = orderId;
        s.customerId  = customerId;
        s.itemId      = itemId;
        s.quantity    = quantity;
        s.totalAmount = totalAmount;
        s.state       = SagaState.CREATED;
        s.createdAt   = Instant.now();
        s.updatedAt   = s.createdAt;
        s.lastStepAt  = s.createdAt;
        s.dlqSent     = false;
        return s;
    }
}
