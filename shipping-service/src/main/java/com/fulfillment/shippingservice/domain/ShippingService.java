package com.fulfillment.shippingservice.domain;

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
public class ShippingService {

    private final ShipmentRepository shipmentRepository;
    private final OutboxWriter        outboxWriter;
    private final IdempotencyGuard    idempotencyGuard;

    private static UUID shipKey(UUID orderId) {
        return UUID.nameUUIDFromBytes(("ship:" + orderId).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    @Transactional
    public void handleCreateShipment(CreateShipmentCommand cmd) {
        if (!idempotencyGuard.tryProcess(shipKey(cmd.orderId()), "create-shipment")) return;

        log.info("[shipping] CreateShipment orderId={}", cmd.orderId());
        boolean created = !cmd.customerId().toString().startsWith("00000000");

        Shipment.Status status = created ? Shipment.Status.CREATED : Shipment.Status.FAILED;
        Shipment shipment = Shipment.of(cmd.orderId(), cmd.customerId(), cmd.itemId(), cmd.quantity(), status);
        shipmentRepository.save(shipment);

        if (created) {
            outboxWriter.write(cmd.orderId().toString(), KafkaTopics.SHIPPING_EVENTS,
                    new ShipmentCreatedEvent(cmd.orderId(), shipment.getId(), Instant.now()));
        } else {
            log.warn("[shipping] Shipment failed for order {}", cmd.orderId());
            outboxWriter.write(cmd.orderId().toString(), KafkaTopics.SHIPPING_EVENTS,
                    new ShipmentFailedEvent(cmd.orderId(), "Shipping unavailable (stub)", Instant.now()));
        }
    }
}
