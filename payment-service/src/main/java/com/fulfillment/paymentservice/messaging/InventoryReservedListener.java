package com.fulfillment.paymentservice.messaging;

/**
 * Removed — payment-service now receives explicit ChargePaymentCommand / RefundPaymentCommand
 * from the saga orchestrator via PaymentCommandListener.
 * InventoryReservedEvent is consumed only by the orchestrator.
 * @deprecated replaced by {@link PaymentCommandListener}
 */
@Deprecated
public class InventoryReservedListener {}
