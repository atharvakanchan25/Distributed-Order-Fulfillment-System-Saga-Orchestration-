package com.fulfillment.common.events;

import java.time.Instant;
import java.util.UUID;

public record SagaDeadLetterEvent(
        UUID   orderId,
        String stuckState,
        String reason,
        Instant occurredAt
) {}
