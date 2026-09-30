package com.fulfillment.paymentservice.domain;

import com.fulfillment.common.events.*;
import com.fulfillment.common.idempotency.IdempotencyGuard;
import com.fulfillment.common.outbox.OutboxWriter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private final PaymentRecordRepository paymentRecordRepository;
    private final OutboxWriter            outboxWriter;
    private final IdempotencyGuard        idempotencyGuard;

    private static UUID chargeKey(UUID orderId) {
        return UUID.nameUUIDFromBytes(("charge:" + orderId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
    private static UUID refundKey(UUID orderId) {
        return UUID.nameUUIDFromBytes(("refund:" + orderId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Transactional
    public void handleChargePayment(ChargePaymentCommand cmd) {
        if (!idempotencyGuard.tryProcess(chargeKey(cmd.orderId()), "charge-payment")) return;

        log.info("[payment] ChargePayment orderId={} amount={}", cmd.orderId(), cmd.amount());
        boolean charged = !cmd.customerId().toString().startsWith("00000000");

        PaymentRecord.Status status = charged ? PaymentRecord.Status.CHARGED : PaymentRecord.Status.FAILED;
        paymentRecordRepository.save(PaymentRecord.of(cmd.orderId(), cmd.customerId(), cmd.amount(), status));

        if (charged) {
            outboxWriter.write(cmd.orderId().toString(), KafkaTopics.PAYMENT_EVENTS,
                    new PaymentChargedEvent(cmd.orderId(), cmd.customerId(), cmd.amount(), Instant.now()));
        } else {
            log.warn("[payment] Payment declined for order {}", cmd.orderId());
            outboxWriter.write(cmd.orderId().toString(), KafkaTopics.PAYMENT_EVENTS,
                    new PaymentFailedEvent(cmd.orderId(), cmd.customerId(), "Card declined (stub)", Instant.now()));
        }
    }

    @Transactional
    public void handleRefundPayment(RefundPaymentCommand cmd) {
        if (!idempotencyGuard.tryProcess(refundKey(cmd.orderId()), "refund-payment")) return;

        log.info("[payment] RefundPayment orderId={}", cmd.orderId());
        paymentRecordRepository.findByOrderId(cmd.orderId()).ifPresent(record -> {
            record.markRefunded();
            paymentRecordRepository.save(record);
        });

        outboxWriter.write(cmd.orderId().toString(), KafkaTopics.PAYMENT_EVENTS,
                new PaymentRefundedEvent(cmd.orderId(), Instant.now()));
    }
}
