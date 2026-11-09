package practice.events;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String correlationId,
        Payload payload) {

    public static final String TYPE = "OrderCreated";
    public static final int SCHEMA_VERSION = 1;

    public record Payload(Long orderId, String branchName, BigDecimal total, List<Item> items) {
    }

    public record Item(String code, String description, BigDecimal price) {
    }
}