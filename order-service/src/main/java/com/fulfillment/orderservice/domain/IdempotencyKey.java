package com.fulfillment.orderservice.domain;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "idempotency_keys")
@Getter
@NoArgsConstructor
@AllArgsConstructor
public class IdempotencyKey {

    @Id
    private String idempotencyKey;

    @Column(nullable = false)
    private UUID orderId;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    public IdempotencyKey(String idempotencyKey, UUID orderId) {
        this.idempotencyKey = idempotencyKey;
        this.orderId        = orderId;
        this.createdAt      = Instant.now();
    }
}
