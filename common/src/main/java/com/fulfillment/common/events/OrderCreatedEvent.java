package com.fulfillment.common.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID orderId,
        UUID customerId,
        UUID itemId,
        int quantity,
        BigDecimal totalAmount,
        Instant occurredAt
) {}
