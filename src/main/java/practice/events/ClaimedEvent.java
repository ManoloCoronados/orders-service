package practice.events;

import java.util.UUID;

public record ClaimedEvent(
        UUID id,
        String aggregateId,
        String eventType,
        String payload,
        String correlationId,
        int attempts


) {
}
