package com.fulfillment.common.events;

import java.time.Instant;
import java.util.UUID;

public record PaymentFailedEvent(
        UUID orderId,
        UUID customerId,
        String reason,
        Instant occurredAt
) {}
