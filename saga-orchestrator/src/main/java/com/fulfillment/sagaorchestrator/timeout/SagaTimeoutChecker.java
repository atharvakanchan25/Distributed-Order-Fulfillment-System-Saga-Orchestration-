package com.fulfillment.sagaorchestrator.timeout;

import com.fulfillment.common.events.*;
import com.fulfillment.common.outbox.OutboxWriter;
import com.fulfillment.sagaorchestrator.domain.OrderSagaState;
import com.fulfillment.sagaorchestrator.domain.OrderSagaStateRepository;
import com.fulfillment.sagaorchestrator.domain.SagaState;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

/**
 * Scans for sagas that have been stuck in a non-terminal state for longer than
 * STEP_TIMEOUT. On first detection it triggers compensation. If the saga is
 * already in a compensation state and still stuck, it sends to the DLQ for
 * manual intervention — compensation itself has failed.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SagaTimeoutChecker {

    private static final Duration STEP_TIMEOUT = Duration.ofMinutes(2);

    private static final Set<SagaState> TERMINAL = Set.of(
            SagaState.CONFIRMED, SagaState.COMPENSATED, SagaState.FAILED);

    private static final Set<SagaState> COMPENSATION_IN_PROGRESS = Set.of(
            SagaState.RELEASING_INVENTORY, SagaState.REFUNDING_PAYMENT);

    private final OrderSagaStateRepository repository;
    private final OutboxWriter             outboxWriter;

    @Scheduled(fixedDelay = 30_000)
    @Transactional
    public void checkTimeouts() {
        Instant cutoff = Instant.now().minus(STEP_TIMEOUT);
        List<OrderSagaState> stuck = repository.findStuckSagas(cutoff);

        for (OrderSagaState saga : stuck) {
            if (COMPENSATION_IN_PROGRESS.contains(saga.getState()) && !saga.isDlqSent()) {
                // Compensation itself is stuck — needs human intervention
                log.error("[timeout] Saga {} stuck in compensation state {} — sending to DLQ",
                        saga.getOrderId(), saga.getState());
                outboxWriter.write(saga.getOrderId().toString(), KafkaTopics.SAGA_DLQ,
                        new SagaDeadLetterEvent(saga.getOrderId(), saga.getState().name(),
                                saga.getFailureReason(), Instant.now()));
                saga.setDlqSent(true);
                repository.save(saga);

            } else if (!COMPENSATION_IN_PROGRESS.contains(saga.getState()) && !saga.isDlqSent()) {
                // Forward step timed out — trigger compensation
                log.warn("[timeout] Saga {} timed out in state {} — triggering compensation",
                        saga.getOrderId(), saga.getState());
                triggerCompensation(saga);
            }
        }
    }

    private void triggerCompensation(OrderSagaState saga) {
        switch (saga.getState()) {
            case CREATED -> {
                // Inventory command never acknowledged — nothing to undo, just fail
                saga.setState(SagaState.FAILED);
                saga.setFailureReason("Timeout waiting for inventory reservation");
                saga.setUpdatedAt(Instant.now());
                repository.save(saga);
                outboxWriter.write(saga.getOrderId().toString(), KafkaTopics.ORDER_EVENTS,
                        new OrderCompensatedEvent(saga.getOrderId(), saga.getFailureReason(), Instant.now()));
            }
            case INVENTORY_RESERVED -> {
                // Payment timed out — release inventory
                saga.setFailureReason("Timeout waiting for payment");
                outboxWriter.write(saga.getOrderId().toString(), KafkaTopics.CMD_RELEASE_INVENTORY,
                        new ReleaseInventoryCommand(saga.getOrderId(), saga.getItemId(), saga.getQuantity()));
            }
            case PAYMENT_CHARGED -> {
                // Shipping timed out — refund then release
                saga.setFailureReason("Timeout waiting for shipment");
                outboxWriter.write(saga.getOrderId().toString(), KafkaTopics.CMD_REFUND_PAYMENT,
                        new RefundPaymentCommand(saga.getOrderId(), saga.getCustomerId(), saga.getTotalAmount()));
            }
            default -> log.warn("[timeout] No compensation action for state {}", saga.getState());
        }
        saga.setLastStepAt(Instant.now());
        repository.save(saga);
    }
}
