package com.fulfillment.orderservice.stub;

import java.util.UUID;

/** Replaced by a Kafka command in v2. */
public interface InventoryClient {
    boolean reserve(UUID itemId, int quantity);
    void release(UUID itemId, int quantity);
}
