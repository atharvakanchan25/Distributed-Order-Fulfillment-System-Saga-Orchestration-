package com.fulfillment.orderservice.integration;

import com.fulfillment.common.events.InventoryReservedEvent;
import com.fulfillment.common.events.KafkaTopics;
import com.fulfillment.common.events.OrderCreatedEvent;
import com.fulfillment.common.events.PaymentChargedEvent;
import com.fulfillment.orderservice.domain.Order;
import com.fulfillment.orderservice.domain.OrderRepository;
import com.fulfillment.orderservice.domain.OrderService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end happy-path: POST order → OrderCreated published → consumed by
 * InventoryService stub (simulated here via direct Kafka publish) → InventoryReserved
 * → PaymentCharged → order status = CONFIRMED.
 *
 * Since inventory-service and payment-service run as separate processes in production,
 * this test simulates their Kafka responses by publishing the downstream events directly,
 * verifying that OrderResultListener correctly transitions the order to CONFIRMED.
 *
 * A full multi-service IT (all three Spring contexts in one JVM) would require
 * @SpringBootTest for each service — that's covered by the docker-compose smoke test.
 */
@SpringBootTest
@Testcontainers
class OrderFulfillmentHappyPathIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("order_db")
            .withUsername("order_user")
            .withPassword("order_pass");

    @Container
    static KafkaContainer kafka = new KafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void overrideProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired OrderService    orderService;
    @Autowired OrderRepository orderRepository;

    // Known seeded item from V1__create_stock_items.sql equivalent — use any UUID here
    // since inventory-service is not running; we simulate its response manually.
    private static final UUID ITEM_ID     = UUID.fromString("aaaaaaaa-0000-0000-0000-000000000001");
    private static final UUID CUSTOMER_ID = UUID.randomUUID();

    @Test
    void happyPath_orderCreated_thenInventoryReserved_thenPaymentCharged_orderConfirmed()
            throws Exception {

        // 1. Create order — should be PENDING and OrderCreated published
        Order order = orderService.createOrder(UUID.randomUUID().toString(), CUSTOMER_ID, ITEM_ID, 2, new BigDecimal("49.99"));
        assertThat(order.getStatus()).isEqualTo(Order.Status.PENDING);

        // 2. Verify OrderCreated was published to order-events
        try (KafkaConsumer<String, Object> consumer = buildConsumer("it-verify-group")) {
            consumer.subscribe(List.of(KafkaTopics.ORDER_EVENTS));
            ConsumerRecord<String, Object> record = pollForRecord(consumer, order.getId().toString());
            assertThat(record).isNotNull();
            assertThat(record.value()).isInstanceOf(OrderCreatedEvent.class);
            OrderCreatedEvent evt = (OrderCreatedEvent) record.value();
            assertThat(evt.orderId()).isEqualTo(order.getId());
            assertThat(evt.quantity()).isEqualTo(2);
        }

        // 3. Simulate InventoryService publishing InventoryReserved → inventory-events
        simulateInventoryReserved(order, ITEM_ID, CUSTOMER_ID);

        // 4. Simulate PaymentService publishing PaymentCharged → payment-events
        simulatePaymentCharged(order, CUSTOMER_ID);

        // 5. OrderResultListener should pick up PaymentCharged and set CONFIRMED
        await().atMost(Duration.ofSeconds(15))
               .pollInterval(Duration.ofMillis(500))
               .untilAsserted(() -> {
                   Order refreshed = orderRepository.findById(order.getId()).orElseThrow();
                   assertThat(refreshed.getStatus()).isEqualTo(Order.Status.CONFIRMED);
               });
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    @Autowired
    org.springframework.kafka.core.KafkaTemplate<String, Object> kafkaTemplate;

    private void simulateInventoryReserved(Order order, UUID itemId, UUID customerId) {
        var event = new InventoryReservedEvent(
                order.getId(), customerId, itemId,
                order.getQuantity(), order.getTotalPrice(),
                java.time.Instant.now());
        kafkaTemplate.send(KafkaTopics.INVENTORY_EVENTS, order.getId().toString(), event);
    }

    private void simulatePaymentCharged(Order order, UUID customerId) {
        var event = new PaymentChargedEvent(
                order.getId(), customerId, order.getTotalPrice(), java.time.Instant.now());
        kafkaTemplate.send(KafkaTopics.PAYMENT_EVENTS, order.getId().toString(), event);
    }

    private KafkaConsumer<String, Object> buildConsumer(String groupId) {
        JsonDeserializer<Object> deser = new JsonDeserializer<>();
        deser.addTrustedPackages("com.fulfillment.*");
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
        ), new StringDeserializer(), deser);
    }

    private ConsumerRecord<String, Object> pollForRecord(
            KafkaConsumer<String, Object> consumer, String key) {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            for (ConsumerRecord<String, Object> r : consumer.poll(Duration.ofMillis(500))) {
                if (key.equals(r.key())) return r;
            }
        }
        return null;
    }
}
