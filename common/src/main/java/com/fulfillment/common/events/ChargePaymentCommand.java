package com.fulfillment.common.events;

import java.math.BigDecimal;
import java.util.UUID;

public record ChargePaymentCommand(
        UUID orderId,
        UUID customerId,
        BigDecimal amount
) {}
