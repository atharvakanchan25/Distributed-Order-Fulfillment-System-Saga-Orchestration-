package com.fulfillment.common.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentChargedEvent(
        UUID orderId,
        UUID customerId,
        BigDecimal amount,
        Instant occurredAt
) {}
