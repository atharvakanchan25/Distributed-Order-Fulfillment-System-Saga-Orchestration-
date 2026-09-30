package com.fulfillment.common.events;

public final class KafkaTopics {
    // ── Result / event topics (services publish outcomes here) ───────────────
    public static final String ORDER_EVENTS     = "order-events";
    public static final String INVENTORY_EVENTS = "inventory-events";
    public static final String PAYMENT_EVENTS   = "payment-events";
    public static final String SHIPPING_EVENTS  = "shipping-events";

    // ── Command topics (orchestrator publishes, services consume) ────────────
    public static final String CMD_RESERVE_INVENTORY  = "cmd-reserve-inventory";
    public static final String CMD_RELEASE_INVENTORY  = "cmd-release-inventory";
    public static final String CMD_CHARGE_PAYMENT     = "cmd-charge-payment";
    public static final String CMD_REFUND_PAYMENT     = "cmd-refund-payment";
    public static final String CMD_CREATE_SHIPMENT    = "cmd-create-shipment";

    // ── Dead Letter Queue ────────────────────────────────────────────────────
    public static final String SAGA_DLQ = "saga-dlq";

    private KafkaTopics() {}
}
