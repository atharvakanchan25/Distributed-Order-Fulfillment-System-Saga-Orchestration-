package com.fulfillment.common.events;

import java.time.Instant;
import java.util.UUID;

public record InventoryReleasedEvent(
        UUID orderId,
        Instant occurredAt
) {}
