package com.fulfillment.common.dto;

import java.util.UUID;

public record OrderDto(
        UUID orderId,
        UUID customerId,
        double totalAmount,
        String status
) {}
