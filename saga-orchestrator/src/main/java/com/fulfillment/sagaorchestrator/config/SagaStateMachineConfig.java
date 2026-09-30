package com.fulfillment.sagaorchestrator.config;

import com.fulfillment.sagaorchestrator.domain.SagaEvent;
import com.fulfillment.sagaorchestrator.domain.SagaState;
import org.springframework.context.annotation.Configuration;
import org.springframework.statemachine.config.EnableStateMachineFactory;
import org.springframework.statemachine.config.StateMachineConfigurerAdapter;
import org.springframework.statemachine.config.builders.StateMachineStateConfigurer;
import org.springframework.statemachine.config.builders.StateMachineTransitionConfigurer;

import java.util.EnumSet;

/**
 * Defines the saga state machine topology.
 *
 * Happy path:
 *   CREATED → INVENTORY_RESERVED → PAYMENT_CHARGED → SHIPPING_CREATED → CONFIRMED
 *
 * Compensation paths:
 *   INVENTORY_RESERVED + PAYMENT_FAILED      → RELEASING_INVENTORY → COMPENSATED
 *   PAYMENT_CHARGED    + SHIPMENT_FAILED      → REFUNDING_PAYMENT → RELEASING_INVENTORY → COMPENSATED
 */
@Configuration
@EnableStateMachineFactory
public class SagaStateMachineConfig extends StateMachineConfigurerAdapter<SagaState, SagaEvent> {

    @Override
    public void configure(StateMachineStateConfigurer<SagaState, SagaEvent> states) throws Exception {
        states.withStates()
                .initial(SagaState.CREATED)
                .end(SagaState.CONFIRMED)
                .end(SagaState.COMPENSATED)
                .end(SagaState.FAILED)
                .states(EnumSet.allOf(SagaState.class));
    }

    @Override
    public void configure(StateMachineTransitionConfigurer<SagaState, SagaEvent> transitions) throws Exception {
        transitions
            // ── Happy path ────────────────────────────────────────────────
            .withExternal()
                .source(SagaState.CREATED).target(SagaState.INVENTORY_RESERVED)
                .event(SagaEvent.INVENTORY_RESERVED).and()
            .withExternal()
                .source(SagaState.CREATED).target(SagaState.FAILED)
                .event(SagaEvent.INVENTORY_RESERVATION_FAILED).and()
            .withExternal()
                .source(SagaState.INVENTORY_RESERVED).target(SagaState.PAYMENT_CHARGED)
                .event(SagaEvent.PAYMENT_CHARGED).and()
            .withExternal()
                .source(SagaState.PAYMENT_CHARGED).target(SagaState.CONFIRMED)
                .event(SagaEvent.SHIPMENT_CREATED).and()

            // ── Compensation: payment failed after inventory reserved ──────
            .withExternal()
                .source(SagaState.INVENTORY_RESERVED).target(SagaState.RELEASING_INVENTORY)
                .event(SagaEvent.PAYMENT_FAILED).and()
            .withExternal()
                .source(SagaState.RELEASING_INVENTORY).target(SagaState.COMPENSATED)
                .event(SagaEvent.INVENTORY_RELEASED).and()

            // ── Compensation: shipment failed after payment charged ────────
            .withExternal()
                .source(SagaState.PAYMENT_CHARGED).target(SagaState.REFUNDING_PAYMENT)
                .event(SagaEvent.SHIPMENT_FAILED).and()
            .withExternal()
                .source(SagaState.REFUNDING_PAYMENT).target(SagaState.RELEASING_INVENTORY)
                .event(SagaEvent.PAYMENT_REFUNDED).and()
            .withExternal()
                .source(SagaState.RELEASING_INVENTORY).target(SagaState.COMPENSATED)
                .event(SagaEvent.INVENTORY_RELEASED);
    }
}
