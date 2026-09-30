package com.fulfillment.inventoryservice.messaging;

import com.fulfillment.common.events.KafkaTopics;
import com.fulfillment.common.events.ReleaseInventoryCommand;
import com.fulfillment.common.events.ReserveInventoryCommand;
import com.fulfillment.inventoryservice.domain.InventoryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryCommandListener {

    private final InventoryService inventoryService;

    @KafkaListener(topics = KafkaTopics.CMD_RESERVE_INVENTORY, groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onReserveInventory(ReserveInventoryCommand cmd) {
        log.info("[inventory-service] Received ReserveInventoryCommand orderId={}", cmd.orderId());
        inventoryService.handleReserveInventory(cmd);
    }

    @KafkaListener(topics = KafkaTopics.CMD_RELEASE_INVENTORY, groupId = "inventory-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onReleaseInventory(ReleaseInventoryCommand cmd) {
        log.info("[inventory-service] Received ReleaseInventoryCommand orderId={}", cmd.orderId());
        inventoryService.handleReleaseInventory(cmd);
    }
}
