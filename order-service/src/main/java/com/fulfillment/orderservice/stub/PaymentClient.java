package com.fulfillment.orderservice.stub;

import java.math.BigDecimal;
import java.util.UUID;

/** Replaced by a Kafka command in v2. */
public interface PaymentClient {
    boolean charge(UUID customerId, BigDecimal amount);
    void refund(UUID customerId, BigDecimal amount);
}
