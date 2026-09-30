package com.fulfillment.notificationservice.messaging;

import com.fulfillment.common.events.*;
import com.fulfillment.notificationservice.domain.Notification;
import com.fulfillment.notificationservice.domain.NotificationRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final NotificationRepository notificationRepository;

    @KafkaListener(topics = KafkaTopics.ORDER_EVENTS, groupId = "notification-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    @Transactional
    public void onOrderEvent(Object raw) {
        if (raw instanceof OrderConfirmedEvent e) {
            log.info("[notification] Order {} CONFIRMED — sending confirmation notification", e.orderId());
            notificationRepository.save(
                    Notification.of(e.orderId(), "ORDER_CONFIRMED",
                            "Order " + e.orderId() + " has been confirmed at " + e.occurredAt()));
        } else if (raw instanceof OrderCompensatedEvent e) {
            log.warn("[notification] Order {} COMPENSATED reason={} — sending failure notification",
                    e.orderId(), e.reason());
            notificationRepository.save(
                    Notification.of(e.orderId(), "ORDER_COMPENSATED",
                            "Order " + e.orderId() + " failed: " + e.reason()));
        }
    }
}
