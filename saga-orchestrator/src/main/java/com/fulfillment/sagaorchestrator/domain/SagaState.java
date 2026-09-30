package com.fulfillment.sagaorchestrator.domain;

/** All states the saga state machine can occupy. */
public enum SagaState {
    CREATED,
    INVENTORY_RESERVED,
    PAYMENT_CHARGED,
    CONFIRMED,

    // Compensation path
    RELEASING_INVENTORY,
    REFUNDING_PAYMENT,
    COMPENSATED,

    FAILED
}
