package com.fulfillment.orderservice.stub;

import java.math.BigDecimal;
import java.util.UUID;

/**
 * Kept for unit-test instantiation only — no longer a Spring bean in v2.
 */
public class StubPaymentClient implements PaymentClient {

    @Override
    public boolean charge(UUID customerId, BigDecimal amount) {
        return !customerId.toString().startsWith("00000000-0000-0000-0000-");
    }

    @Override
    public void refund(UUID customerId, BigDecimal amount) {
        // no-op for stub
    }
}
