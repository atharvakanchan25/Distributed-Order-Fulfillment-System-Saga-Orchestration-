package com.fulfillment.sagaorchestrator.domain;

import com.fulfillment.common.events.*;
import com.fulfillment.common.idempotency.IdempotencyGuard;
import com.fulfillment.common.outbox.OutboxWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.statemachine.StateMachine;
import org.springframework.statemachine.config.StateMachineFactory;
import org.springframework.statemachine.support.DefaultStateMachineContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class SagaOrchestrator {

    private final OrderSagaStateRepository                  repository;
    private final OutboxWriter                              outboxWriter;
    private final IdempotencyGuard                          idempotencyGuard;
    private final StateMachineFactory<SagaState, SagaEvent> stateMachineFactory;

    // ── Entry point ──────────────────────────────────────────────────────────

    @Transactional
    public void onOrderCreated(OrderCreatedEvent event) {
        if (!idempotencyGuard.tryProcess(event.orderId(), "saga-order-created")) return;
        log.info("[orchestrator] OrderCreated orderId={}", event.orderId());

        var saga = OrderSagaState.create(event.orderId(), event.customerId(),
                event.itemId(), event.quantity(), event.totalAmount());
        repository.save(saga);

        outboxWriter.write(event.orderId().toString(), KafkaTopics.CMD_RESERVE_INVENTORY,
                new ReserveInventoryCommand(event.orderId(), event.customerId(),
                        event.itemId(), event.quantity(), event.totalAmount()));
    }

    // ── Inventory results ────────────────────────────────────────────────────

    @Transactional
    public void onInventoryReserved(InventoryReservedEvent event) {
        if (!idempotencyGuard.tryProcess(
                UUID.nameUUIDFromBytes(("inv-reserved:" + event.orderId()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "saga-inventory-reserved")) return;

        var saga = load(event.orderId());
        transition(saga, SagaEvent.INVENTORY_RESERVED);

        outboxWriter.write(event.orderId().toString(), KafkaTopics.CMD_CHARGE_PAYMENT,
                new ChargePaymentCommand(event.orderId(), event.customerId(), event.totalAmount()));
    }

    @Transactional
    public void onInventoryReservationFailed(InventoryReservationFailedEvent event) {
        if (!idempotencyGuard.tryProcess(
                UUID.nameUUIDFromBytes(("inv-failed:" + event.orderId()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "saga-inventory-failed")) return;

        var saga = load(event.orderId());
        saga.setFailureReason(event.reason());
        transition(saga, SagaEvent.INVENTORY_RESERVATION_FAILED);

        outboxWriter.write(saga.getOrderId().toString(), KafkaTopics.ORDER_EVENTS,
                new OrderCompensatedEvent(saga.getOrderId(), event.reason(), Instant.now()));
    }

    @Transactional
    public void onInventoryReleased(InventoryReleasedEvent event) {
        if (!idempotencyGuard.tryProcess(
                UUID.nameUUIDFromBytes(("inv-released:" + event.orderId()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "saga-inventory-released")) return;

        var saga = load(event.orderId());
        transition(saga, SagaEvent.INVENTORY_RELEASED);

        outboxWriter.write(saga.getOrderId().toString(), KafkaTopics.ORDER_EVENTS,
                new OrderCompensatedEvent(saga.getOrderId(), saga.getFailureReason(), Instant.now()));
    }

    // ── Payment results ──────────────────────────────────────────────────────

    @Transactional
    public void onPaymentCharged(PaymentChargedEvent event) {
        if (!idempotencyGuard.tryProcess(
                UUID.nameUUIDFromBytes(("pay-charged:" + event.orderId()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "saga-payment-charged")) return;

        var saga = load(event.orderId());
        transition(saga, SagaEvent.PAYMENT_CHARGED);

        outboxWriter.write(event.orderId().toString(), KafkaTopics.CMD_CREATE_SHIPMENT,
                new CreateShipmentCommand(event.orderId(), saga.getCustomerId(),
                        saga.getItemId(), saga.getQuantity(), saga.getTotalAmount()));
    }

    @Transactional
    public void onPaymentFailed(PaymentFailedEvent event) {
        if (!idempotencyGuard.tryProcess(
                UUID.nameUUIDFromBytes(("pay-failed:" + event.orderId()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "saga-payment-failed")) return;

        var saga = load(event.orderId());
        saga.setFailureReason(event.reason());
        transition(saga, SagaEvent.PAYMENT_FAILED);

        outboxWriter.write(event.orderId().toString(), KafkaTopics.CMD_RELEASE_INVENTORY,
                new ReleaseInventoryCommand(event.orderId(), saga.getItemId(), saga.getQuantity()));
    }

    @Transactional
    public void onPaymentRefunded(PaymentRefundedEvent event) {
        if (!idempotencyGuard.tryProcess(
                UUID.nameUUIDFromBytes(("pay-refunded:" + event.orderId()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "saga-payment-refunded")) return;

        var saga = load(event.orderId());
        transition(saga, SagaEvent.PAYMENT_REFUNDED);

        outboxWriter.write(event.orderId().toString(), KafkaTopics.CMD_RELEASE_INVENTORY,
                new ReleaseInventoryCommand(event.orderId(), saga.getItemId(), saga.getQuantity()));
    }

    // ── Shipping results ─────────────────────────────────────────────────────

    @Transactional
    public void onShipmentCreated(ShipmentCreatedEvent event) {
        if (!idempotencyGuard.tryProcess(
                UUID.nameUUIDFromBytes(("ship-created:" + event.orderId()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "saga-shipment-created")) return;

        var saga = load(event.orderId());
        transition(saga, SagaEvent.SHIPMENT_CREATED);

        log.info("[orchestrator] Saga CONFIRMED orderId={}", event.orderId());
        outboxWriter.write(event.orderId().toString(), KafkaTopics.ORDER_EVENTS,
                new OrderConfirmedEvent(event.orderId(), Instant.now()));
    }

    @Transactional
    public void onShipmentFailed(ShipmentFailedEvent event) {
        if (!idempotencyGuard.tryProcess(
                UUID.nameUUIDFromBytes(("ship-failed:" + event.orderId()).getBytes(java.nio.charset.StandardCharsets.UTF_8)),
                "saga-shipment-failed")) return;

        var saga = load(event.orderId());
        saga.setFailureReason(event.reason());
        transition(saga, SagaEvent.SHIPMENT_FAILED);

        outboxWriter.write(event.orderId().toString(), KafkaTopics.CMD_REFUND_PAYMENT,
                new RefundPaymentCommand(event.orderId(), saga.getCustomerId(), saga.getTotalAmount()));
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private OrderSagaState load(UUID orderId) {
        return repository.findById(orderId)
                .orElseThrow(() -> new IllegalStateException("No saga for order " + orderId));
    }

    private void transition(OrderSagaState saga, SagaEvent event) {
        StateMachine<SagaState, SagaEvent> sm =
                stateMachineFactory.getStateMachine(saga.getOrderId().toString());
        sm.stopReactively().block();
        sm.getStateMachineAccessor().doWithAllRegions(a ->
                a.resetStateMachineReactively(
                        new DefaultStateMachineContext<>(saga.getState(), null, null, null)
                ).block());
        sm.startReactively().block();
        sm.sendEvent(Mono.just(
                org.springframework.messaging.support.MessageBuilder.withPayload(event).build()
        )).blockLast();

        SagaState newState = sm.getState().getId();
        log.info("[orchestrator] orderId={} {} → {} (event={})",
                saga.getOrderId(), saga.getState(), newState, event);

        saga.setState(newState);
        saga.setUpdatedAt(Instant.now());
        saga.setLastStepAt(Instant.now());
        repository.save(saga);
    }
}
