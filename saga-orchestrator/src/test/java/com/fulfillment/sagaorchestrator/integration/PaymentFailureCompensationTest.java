package com.fulfillment.sagaorchestrator.integration;

import com.fulfillment.common.events.*;
import com.fulfillment.sagaorchestrator.domain.OrderSagaState;
import com.fulfillment.sagaorchestrator.domain.OrderSagaStateRepository;
import com.fulfillment.sagaorchestrator.domain.SagaState;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Forces payment failure by using a customerId starting with "00000000".
 * The payment-service stub declines such customers, so the orchestrator must:
 *   1. Receive PaymentFailedEvent
 *   2. Transition saga to RELEASING_INVENTORY
 *   3. Send ReleaseInventoryCommand
 *   4. Receive InventoryReleasedEvent → transition to COMPENSATED
 *
 * This test drives the orchestrator directly (no real inventory/payment services).
 * It publishes the result events that those services would have published,
 * and asserts the saga state machine ends in COMPENSATED.
 */
@SpringBootTest
@Testcontainers
class PaymentFailureCompensationTest {

    @Container
    static final PostgreSQLContainer<?> postgres =
            new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                    .withDatabaseName("saga_db")
                    .withUsername("saga_user")
                    .withPassword("saga_pass");

    @Container
    static final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void overrideProps(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    OrderSagaStateRepository repository;

    @Test
    void paymentFailure_shouldReleaseInventoryAndCompensate() throws Exception {
        UUID orderId    = UUID.randomUUID();
        // customerId starting with 00000000 triggers payment decline in the stub
        UUID customerId = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID itemId     = UUID.randomUUID();

        try (KafkaProducer<String, Object> producer = buildProducer();
             KafkaConsumer<String, Object> consumer = buildConsumer("test-compensation-group")) {

            consumer.subscribe(List.of(
                    KafkaTopics.CMD_RESERVE_INVENTORY,
                    KafkaTopics.CMD_RELEASE_INVENTORY,
                    KafkaTopics.ORDER_EVENTS
            ));

            // Step 1: publish OrderCreatedEvent → orchestrator saves saga and sends ReserveInventoryCommand
            producer.send(new ProducerRecord<>(
                    KafkaTopics.ORDER_EVENTS,
                    orderId.toString(),
                    new OrderCreatedEvent(orderId, customerId, itemId, 2,
                            new BigDecimal("49.99"), Instant.now())
            )).get();

            // Step 2: wait for ReserveInventoryCommand, then simulate inventory reserved
            awaitCommand(consumer, KafkaTopics.CMD_RESERVE_INVENTORY, orderId);

            producer.send(new ProducerRecord<>(
                    KafkaTopics.INVENTORY_EVENTS,
                    orderId.toString(),
                    new InventoryReservedEvent(orderId, customerId, itemId, 2,
                            new BigDecimal("49.99"), Instant.now())
            )).get();

            // Step 3: orchestrator receives InventoryReservedEvent → sends ChargePaymentCommand
            // We skip consuming CMD_CHARGE_PAYMENT and instead simulate payment failure directly
            producer.send(new ProducerRecord<>(
                    KafkaTopics.PAYMENT_EVENTS,
                    orderId.toString(),
                    new PaymentFailedEvent(orderId, customerId, "Card declined (stub)", Instant.now())
            )).get();

            // Step 4: orchestrator must send ReleaseInventoryCommand
            awaitCommand(consumer, KafkaTopics.CMD_RELEASE_INVENTORY, orderId);

            // Step 5: simulate inventory released
            producer.send(new ProducerRecord<>(
                    KafkaTopics.INVENTORY_EVENTS,
                    orderId.toString(),
                    new InventoryReleasedEvent(orderId, Instant.now())
            )).get();

            // Step 6: assert saga ends in COMPENSATED
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                Optional<OrderSagaState> saga = repository.findById(orderId);
                assertThat(saga).isPresent();
                assertThat(saga.get().getState()).isEqualTo(SagaState.COMPENSATED);
                assertThat(saga.get().getFailureReason()).isEqualTo("Card declined (stub)");
            });
        }
    }

    /**
     * Polls until a record for the given orderId arrives on the expected topic.
     */
    private void awaitCommand(KafkaConsumer<String, Object> consumer, String topic, UUID orderId) {
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            var records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, Object> r : records) {
                if (r.topic().equals(topic) && orderId.toString().equals(r.key())) return true;
            }
            return false;
        });
    }

    private KafkaProducer<String, Object> buildProducer() {
        var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, true);
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, Object> buildConsumer(String groupId) {
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "com.fulfillment.*");
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, "true");
        return new KafkaConsumer<>(props);
    }
}
