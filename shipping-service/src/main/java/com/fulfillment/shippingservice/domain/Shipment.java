package com.fulfillment.shippingservice.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "shipments")
@Getter
@NoArgsConstructor
public class Shipment {

    public enum Status { CREATED, FAILED }

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false, unique = true)
    private UUID orderId;

    @Column(nullable = false)
    private UUID customerId;

    @Column(nullable = false)
    private UUID itemId;

    @Column(nullable = false)
    private int quantity;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Status status;

    @Column(nullable = false)
    private Instant createdAt;

    public static Shipment of(UUID orderId, UUID customerId, UUID itemId, int quantity, Status status) {
        var s = new Shipment();
        s.orderId    = orderId;
        s.customerId = customerId;
        s.itemId     = itemId;
        s.quantity   = quantity;
        s.status     = status;
        s.createdAt  = Instant.now();
        return s;
    }
}
