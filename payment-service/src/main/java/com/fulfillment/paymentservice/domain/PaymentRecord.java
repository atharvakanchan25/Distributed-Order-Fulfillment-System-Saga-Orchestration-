package com.fulfillment.paymentservice.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payment_records")
@Getter
@NoArgsConstructor
public class PaymentRecord {

    public enum Status { CHARGED, FAILED, REFUNDED }

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false, unique = true)
    private UUID orderId;

    @Column(nullable = false)
    private UUID customerId;

    @Column(nullable = false, precision = 12, scale = 2)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private Status status;

    @Column(nullable = false)
    private Instant createdAt;

    public static PaymentRecord of(UUID orderId, UUID customerId, BigDecimal amount, Status status) {
        var r = new PaymentRecord();
        r.orderId    = orderId;
        r.customerId = customerId;
        r.amount     = amount;
        r.status     = status;
        r.createdAt  = Instant.now();
        return r;
    }

    public void markRefunded() {
        this.status = Status.REFUNDED;
    }
}
