package com.fulfillment.orderservice.stub;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Kept for unit-test instantiation only — no longer a Spring bean in v2.
 */
public class StubInventoryClient implements InventoryClient {

    private final ConcurrentHashMap<UUID, Integer> reserved = new ConcurrentHashMap<>();

    @Override
    public boolean reserve(UUID itemId, int quantity) {
        if (itemId.toString().startsWith("00000000-0000-0000-0000-")) return false;
        reserved.merge(itemId, quantity, Integer::sum);
        return true;
    }

    @Override
    public void release(UUID itemId, int quantity) {
        reserved.computeIfPresent(itemId, (k, v) -> Math.max(0, v - quantity));
    }
}
