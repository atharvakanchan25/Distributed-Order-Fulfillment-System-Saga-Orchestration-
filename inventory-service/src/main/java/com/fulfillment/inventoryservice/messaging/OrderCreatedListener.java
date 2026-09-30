package com.fulfillment.inventoryservice.messaging;

/**
 * Removed in v3 — inventory-service now listens to explicit orchestrator commands
 * (ReserveInventoryCommand / ReleaseInventoryCommand) via InventoryCommandListener.
 * This file is kept as a placeholder to avoid breaking the build if referenced elsewhere.
 * @deprecated replaced by {@link InventoryCommandListener}
 */
@Deprecated
public class OrderCreatedListener {}
