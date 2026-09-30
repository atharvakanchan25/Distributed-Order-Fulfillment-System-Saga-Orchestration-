package com.fulfillment.sagaorchestrator.observability;

import com.fulfillment.sagaorchestrator.domain.OrderSagaStateRepository;
import com.fulfillment.sagaorchestrator.domain.SagaState;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class SagaMetrics {

    private final OrderSagaStateRepository repository;

    @Bean
    public MeterBinder sagaStateGauges(MeterRegistry registry) {
        return r -> {
            for (SagaState state : SagaState.values()) {
                r.gauge("saga.state.count",
                        java.util.List.of(io.micrometer.core.instrument.Tag.of("state", state.name())),
                        repository,
                        repo -> repo.countByState(state));
            }
        };
    }

    /**
     * Increment this counter whenever a saga reaches COMPENSATED.
     * Compensation rate = compensations / total_confirmed+compensated over time window.
     */
    @Bean
    public Counter compensationCounter(MeterRegistry registry) {
        return Counter.builder("saga.compensations.total")
                .description("Number of sagas that ended in COMPENSATED")
                .register(registry);
    }

    @Bean
    public Counter dlqCounter(MeterRegistry registry) {
        return Counter.builder("saga.dlq.total")
                .description("Number of sagas sent to DLQ for manual intervention")
                .register(registry);
    }
}
