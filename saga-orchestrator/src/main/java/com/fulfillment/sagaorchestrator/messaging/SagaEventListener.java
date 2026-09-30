package com.fulfillment.sagaorchestrator.messaging;

import com.fulfillment.common.events.*;
import com.fulfillment.sagaorchestrator.domain.SagaOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Single entry point for all inbound events the orchestrator cares about.
 * Each topic gets its own listener method for clarity.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SagaEventListener {

    private final SagaOrchestrator orchestrator;

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId = "saga-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onOrderEvent(Object raw) {
        if (raw instanceof OrderCreatedEvent e) orchestrator.onOrderCreated(e);
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS, groupId = "saga-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onInventoryEvent(Object raw) {
        if (raw instanceof InventoryReservedEvent e)            orchestrator.onInventoryReserved(e);
        else if (raw instanceof InventoryReservationFailedEvent e) orchestrator.onInventoryReservationFailed(e);
        else if (raw instanceof InventoryReleasedEvent e)       orchestrator.onInventoryReleased(e);
    }

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = "saga-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onPaymentEvent(Object raw) {
        if (raw instanceof PaymentChargedEvent e)  orchestrator.onPaymentCharged(e);
        else if (raw instanceof PaymentFailedEvent e)   orchestrator.onPaymentFailed(e);
        else if (raw instanceof PaymentRefundedEvent e) orchestrator.onPaymentRefunded(e);
    }

    @KafkaListener(topics = KafkaTopics.SHIPPING_EVENTS, groupId = "saga-orchestrator-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onShippingEvent(Object raw) {
        if (raw instanceof ShipmentCreatedEvent e) orchestrator.onShipmentCreated(e);
        else if (raw instanceof ShipmentFailedEvent e)  orchestrator.onShipmentFailed(e);
    }
}
