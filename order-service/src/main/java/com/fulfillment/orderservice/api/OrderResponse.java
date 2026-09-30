package com.fulfillment.orderservice.api;

import com.fulfillment.orderservice.domain.Order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderResponse(
        UUID       id,
        UUID       customerId,
        UUID       itemId,
        int        quantity,
        BigDecimal totalPrice,
        String     status,
        Instant    createdAt,
        Instant    updatedAt
) {
    public static OrderResponse from(Order o) {
        return new OrderResponse(
                o.getId(), o.getCustomerId(), o.getItemId(),
                o.getQuantity(), o.getTotalPrice(),
                o.getStatus().name(),
                o.getCreatedAt(), o.getUpdatedAt()
        );
    }
}
