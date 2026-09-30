package com.fulfillment.orderservice.domain;

import com.fulfillment.common.events.KafkaTopics;
import com.fulfillment.common.events.OrderCreatedEvent;
import com.fulfillment.common.outbox.OutboxWriter;
import com.fulfillment.orderservice.exception.OrderNotFoundException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository     orderRepository;
    private final IdempotencyKeyRepository idempotencyKeyRepository;
    private final OutboxWriter        outboxWriter;

    @Transactional
    public Order createOrder(String idempotencyKey, UUID customerId, UUID itemId,
                             int quantity, BigDecimal totalPrice) {
        // Return existing order if this key was already processed
        var existing = idempotencyKeyRepository.findById(idempotencyKey);
        if (existing.isPresent()) {
            log.info("[order] Idempotent replay for key={}", idempotencyKey);
            return orderRepository.findById(existing.get().getOrderId()).orElseThrow();
        }

        Order order = Order.create(customerId, itemId, quantity, totalPrice);
        orderRepository.save(order);
        idempotencyKeyRepository.save(new IdempotencyKey(idempotencyKey, order.getId()));

        // Write to outbox in the same transaction — no dual-write risk
        outboxWriter.write(order.getId().toString(), KafkaTopics.ORDER_EVENTS,
                new OrderCreatedEvent(order.getId(), customerId, itemId, quantity, totalPrice, Instant.now()));

        log.info("[order] Order {} created, outbox entry written", order.getId());
        return order;
    }

    @Transactional(readOnly = true)
    public Order getOrder(UUID id) {
        return orderRepository.findById(id).orElseThrow(() -> new OrderNotFoundException(id));
    }

    @Transactional
    public Order cancelOrder(UUID id) {
        Order order = getOrder(id);
        if (order.getStatus() != Order.Status.PENDING) {
            throw new IllegalStateException("Cannot cancel order in status " + order.getStatus());
        }
        order.transitionTo(Order.Status.FAILED);
        return orderRepository.save(order);
    }
}
