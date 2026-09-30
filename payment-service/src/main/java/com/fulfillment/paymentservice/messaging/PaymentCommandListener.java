package com.fulfillment.paymentservice.messaging;

import com.fulfillment.common.events.ChargePaymentCommand;
import com.fulfillment.common.events.KafkaTopics;
import com.fulfillment.common.events.RefundPaymentCommand;
import com.fulfillment.paymentservice.domain.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentCommandListener {

    private final PaymentService paymentService;

    @KafkaListener(topics = KafkaTopics.CMD_CHARGE_PAYMENT, groupId = "payment-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onChargePayment(ChargePaymentCommand cmd) {
        log.info("[payment-service] Received ChargePaymentCommand orderId={}", cmd.orderId());
        paymentService.handleChargePayment(cmd);
    }

    @KafkaListener(topics = KafkaTopics.CMD_REFUND_PAYMENT, groupId = "payment-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onRefundPayment(RefundPaymentCommand cmd) {
        log.info("[payment-service] Received RefundPaymentCommand orderId={}", cmd.orderId());
        paymentService.handleRefundPayment(cmd);
    }
}
