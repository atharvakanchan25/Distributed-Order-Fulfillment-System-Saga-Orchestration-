package com.fulfillment.common.events;

import java.math.BigDecimal;
import java.util.UUID;

public record ReserveInventoryCommand(
        UUID orderId,
        UUID customerId,
        UUID itemId,
        int quantity,
        BigDecimal totalAmount
) {}
