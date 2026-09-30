package com.fulfillment.sagaorchestrator.domain;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface OrderSagaStateRepository extends JpaRepository<OrderSagaState, UUID> {

    long countByState(SagaState state);

    @Query("""
            SELECT s FROM OrderSagaState s
            WHERE s.state NOT IN
              (com.fulfillment.sagaorchestrator.domain.SagaState.CONFIRMED,
               com.fulfillment.sagaorchestrator.domain.SagaState.COMPENSATED,
               com.fulfillment.sagaorchestrator.domain.SagaState.FAILED)
            AND s.lastStepAt < :cutoff
            AND s.dlqSent = false
            """)
    List<OrderSagaState> findStuckSagas(@Param("cutoff") Instant cutoff);
}
