package com.fulfillment.sagaorchestrator.domain;

/** Events that drive state machine transitions. */
public enum SagaEvent {
    ORDER_CREATED,
    INVENTORY_RESERVED,
    INVENTORY_RESERVATION_FAILED,
    PAYMENT_CHARGED,
    PAYMENT_FAILED,
    SHIPMENT_CREATED,
    SHIPMENT_FAILED,
    INVENTORY_RELEASED,
    PAYMENT_REFUNDED
}
