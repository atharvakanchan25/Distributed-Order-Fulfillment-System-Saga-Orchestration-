package com.fulfillment.common.events;

import java.math.BigDecimal;
import java.util.UUID;

public record CreateShipmentCommand(
        UUID orderId,
        UUID customerId,
        UUID itemId,
        int quantity,
        BigDecimal amount
) {}
