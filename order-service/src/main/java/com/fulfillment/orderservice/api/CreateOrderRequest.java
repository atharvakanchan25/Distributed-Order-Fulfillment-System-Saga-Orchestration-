package com.fulfillment.orderservice.api;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;
import java.util.UUID;

public record CreateOrderRequest(
        @NotNull(message = "customerId is required") UUID customerId,
        @NotNull(message = "itemId is required")     UUID itemId,
        @Min(value = 1, message = "quantity must be at least 1") int quantity,
        @NotNull @Positive(message = "totalPrice must be positive") BigDecimal totalPrice
) {}
