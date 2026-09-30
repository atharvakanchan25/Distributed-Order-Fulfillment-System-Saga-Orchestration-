package com.fulfillment.notificationservice.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notifications")
@Getter
@NoArgsConstructor
public class Notification {

    @Id
    @GeneratedValue
    private UUID id;

    @Column(nullable = false)
    private UUID orderId;

    @Column(nullable = false, length = 50)
    private String eventType;

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    @Column(nullable = false)
    private Instant sentAt;

    public static Notification of(UUID orderId, String eventType, String payload) {
        var n = new Notification();
        n.orderId   = orderId;
        n.eventType = eventType;
        n.payload   = payload;
        n.sentAt    = Instant.now();
        return n;
    }
}
