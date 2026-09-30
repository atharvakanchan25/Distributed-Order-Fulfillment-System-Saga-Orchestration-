package com.fulfillment.common.events;

import java.math.BigDecimal;
import java.util.UUID;

public record RefundPaymentCommand(
        UUID orderId,
        UUID customerId,
        BigDecimal amount
) {}
