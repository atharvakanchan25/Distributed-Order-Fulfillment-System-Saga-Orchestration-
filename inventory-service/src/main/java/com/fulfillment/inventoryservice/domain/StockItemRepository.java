package com.fulfillment.inventoryservice.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

public interface StockItemRepository extends JpaRepository<StockItem, UUID> {
    Optional<StockItem> findByItemId(UUID itemId);
}
