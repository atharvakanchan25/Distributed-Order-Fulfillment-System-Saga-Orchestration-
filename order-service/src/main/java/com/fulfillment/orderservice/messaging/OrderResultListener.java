package com.fulfillment.orderservice.messaging;

import com.fulfillment.common.events.*;
import com.fulfillment.orderservice.domain.Order;
import com.fulfillment.orderservice.domain.OrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderResultListener {

    private final OrderRepository orderRepository;

    @KafkaListener(topics = KafkaTopics.PAYMENT_EVENTS, groupId = "order-service-payment-group",
            containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onPaymentEvent(Object raw) {
        if (raw instanceof PaymentChargedEvent e) {
            log.info("[order-service] PaymentCharged received orderId={}", e.orderId());
            updateStatus(e.orderId(), Order.Status.CONFIRMED);
        } else if (raw instanceof PaymentFailedEvent e) {
            log.warn("[order-service] PaymentFailed received orderId={} reason={}", e.orderId(), e.reason());
            updateStatus(e.orderId(), Order.Status.FAILED);
        }
    }

    @KafkaListener(topics = KafkaTopics.INVENTORY_EVENTS, groupId = "order-service-inventory-group",
            containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onInventoryEvent(Object raw) {
        if (raw instanceof InventoryReservationFailedEvent e) {
            log.warn("[order-service] InventoryReservationFailed orderId={} reason={}", e.orderId(), e.reason());
            updateStatus(e.orderId(), Order.Status.FAILED);
        }
        // InventoryReserved is handled by payment-service; order-service waits for payment outcome
    }

    private void updateStatus(java.util.UUID orderId, Order.Status status) {
        orderRepository.findById(orderId).ifPresentOrElse(
                order -> {
                    order.transitionTo(status);
                    orderRepository.save(order);
                    log.info("[order-service] Order {} transitioned to {}", orderId, status);
                },
                () -> log.warn("[order-service] Order {} not found for status update", orderId)
        );
    }
}
