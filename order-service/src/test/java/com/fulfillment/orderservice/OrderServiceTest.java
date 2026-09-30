package com.fulfillment.orderservice;

import com.fulfillment.common.outbox.OutboxWriter;
import com.fulfillment.orderservice.domain.*;
import com.fulfillment.orderservice.exception.OrderNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock OrderRepository            orderRepository;
    @Mock IdempotencyKeyRepository   idempotencyKeyRepository;
    @Mock OutboxWriter               outboxWriter;
    @InjectMocks OrderService        orderService;

    private final UUID       customerId = UUID.randomUUID();
    private final UUID       itemId     = UUID.randomUUID();
    private final BigDecimal price      = new BigDecimal("99.99");
    private final String     idemKey    = UUID.randomUUID().toString();

    @BeforeEach
    void stubSave() {
        lenient().when(orderRepository.save(any(Order.class)))
                 .thenAnswer(inv -> inv.getArgument(0));
        lenient().when(idempotencyKeyRepository.findById(any()))
                 .thenReturn(Optional.empty());
    }

    // ── createOrder ──────────────────────────────────────────────────────────

    @Test
    void createOrder_savesPendingAndWritesOutbox() {
        Order result = orderService.createOrder(idemKey, customerId, itemId, 2, price);

        assertThat(result.getStatus()).isEqualTo(Order.Status.PENDING);
        verify(orderRepository).save(any(Order.class));
        verify(outboxWriter).write(any(), any(), any());
    }

    @Test
    void createOrder_idempotentReplay_returnsExistingOrder() {
        Order existing = Order.create(customerId, itemId, 2, price);
        when(orderRepository.save(any())).thenReturn(existing);
        IdempotencyKey key = new IdempotencyKey(idemKey, existing.getId());
        when(idempotencyKeyRepository.findById(idemKey)).thenReturn(Optional.of(key));
        when(orderRepository.findById(existing.getId())).thenReturn(Optional.of(existing));

        Order result = orderService.createOrder(idemKey, customerId, itemId, 2, price);

        assertThat(result.getId()).isEqualTo(existing.getId());
        verify(outboxWriter, never()).write(any(), any(), any());
    }

    // ── cancelOrder ──────────────────────────────────────────────────────────

    @Test
    void cancelOrder_whenPending_transitionsToFailed() {
        Order pending = Order.create(customerId, itemId, 1, price);
        when(orderRepository.findById(pending.getId())).thenReturn(Optional.of(pending));

        Order result = orderService.cancelOrder(pending.getId());

        assertThat(result.getStatus()).isEqualTo(Order.Status.FAILED);
    }

    @Test
    void cancelOrder_whenConfirmed_throwsIllegalState() {
        Order confirmed = Order.create(customerId, itemId, 1, price);
        confirmed.transitionTo(Order.Status.CONFIRMED);
        when(orderRepository.findById(confirmed.getId())).thenReturn(Optional.of(confirmed));

        assertThatThrownBy(() -> orderService.cancelOrder(confirmed.getId()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("CONFIRMED");
    }

    // ── getOrder ─────────────────────────────────────────────────────────────

    @Test
    void getOrder_notFound_throwsOrderNotFoundException() {
        UUID missing = UUID.randomUUID();
        when(orderRepository.findById(missing)).thenReturn(Optional.empty());

        assertThatThrownBy(() -> orderService.getOrder(missing))
                .isInstanceOf(OrderNotFoundException.class);
    }

    // ── Order entity state-machine ────────────────────────────────────────────

    @Test
    void order_pendingToConfirmed_allowed() {
        Order o = Order.create(customerId, itemId, 1, price);
        o.transitionTo(Order.Status.CONFIRMED);
        assertThat(o.getStatus()).isEqualTo(Order.Status.CONFIRMED);
    }

    @Test
    void order_pendingToFailed_allowed() {
        Order o = Order.create(customerId, itemId, 1, price);
        o.transitionTo(Order.Status.FAILED);
        assertThat(o.getStatus()).isEqualTo(Order.Status.FAILED);
    }

    @Test
    void order_confirmedToPending_rejected() {
        Order o = Order.create(customerId, itemId, 1, price);
        o.transitionTo(Order.Status.CONFIRMED);
        assertThatThrownBy(() -> o.transitionTo(Order.Status.PENDING))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void order_failedToConfirmed_rejected() {
        Order o = Order.create(customerId, itemId, 1, price);
        o.transitionTo(Order.Status.FAILED);
        assertThatThrownBy(() -> o.transitionTo(Order.Status.CONFIRMED))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void order_confirmedToConfirmed_rejected() {
        Order o = Order.create(customerId, itemId, 1, price);
        o.transitionTo(Order.Status.CONFIRMED);
        assertThatThrownBy(() -> o.transitionTo(Order.Status.CONFIRMED))
                .isInstanceOf(IllegalStateException.class);
    }
}
