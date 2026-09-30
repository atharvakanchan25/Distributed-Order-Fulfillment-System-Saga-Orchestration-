package com.fulfillment.common.events;

import java.time.Instant;
import java.util.UUID;

public record ShipmentFailedEvent(
        UUID orderId,
        String reason,
        Instant occurredAt
) {}
