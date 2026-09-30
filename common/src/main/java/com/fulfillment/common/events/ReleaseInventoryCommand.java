package com.fulfillment.common.events;

import java.util.UUID;

public record ReleaseInventoryCommand(
        UUID orderId,
        UUID itemId,
        int quantity
) {}
