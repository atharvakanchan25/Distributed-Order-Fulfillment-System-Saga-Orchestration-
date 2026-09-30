package com.fulfillment.shippingservice.messaging;

import com.fulfillment.common.events.CreateShipmentCommand;
import com.fulfillment.common.events.KafkaTopics;
import com.fulfillment.shippingservice.domain.ShippingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CreateShipmentListener {

    private final ShippingService shippingService;

    @KafkaListener(topics = KafkaTopics.CMD_CREATE_SHIPMENT, groupId = "shipping-service-group",
            containerFactory = "kafkaListenerContainerFactory")
    public void onCreateShipment(CreateShipmentCommand cmd) {
        log.info("[shipping-service] Received CreateShipmentCommand orderId={}", cmd.orderId());
        shippingService.handleCreateShipment(cmd);
    }
}
