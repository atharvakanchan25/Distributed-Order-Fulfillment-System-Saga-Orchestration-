package com.fulfillment.orderservice.messaging;

import com.fulfillment.common.events.KafkaTopics;
import com.fulfillment.common.events.OrderCreatedEvent;
import com.fulfillment.orderservice.domain.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishOrderCreated(Order order) {
        var event = new OrderCreatedEvent(
                order.getId(),
                order.getCustomerId(),
                order.getItemId(),
                order.getQuantity(),
                order.getTotalPrice(),
                Instant.now()
        );
        kafkaTemplate.send(KafkaTopics.ORDER_EVENTS, order.getId().toString(), event)
                .whenComplete((r, ex) -> {
                    if (ex != null) log.error("[order-service] Failed to publish OrderCreated for {}", order.getId(), ex);
                    else log.info("[order-service] Published OrderCreated orderId={} topic=order-events", order.getId());
                });
    }
}
