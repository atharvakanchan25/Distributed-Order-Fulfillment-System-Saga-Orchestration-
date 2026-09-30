package com.fulfillment.common.events;

import java.time.Instant;
import java.util.UUID;

public record SagaStepResultEvent(
        UUID sagaId,
        UUID orderId,
        String stepName,
        StepStatus status,
        String failureReason,
        Instant occurredAt
) {
    public enum StepStatus { SUCCESS, FAILURE }
}
