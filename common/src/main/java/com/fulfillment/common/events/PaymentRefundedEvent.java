package com.fulfillment.common.events;

import java.time.Instant;
import java.util.UUID;

public record PaymentRefundedEvent(
        UUID orderId,
        Instant occurredAt
) {}
