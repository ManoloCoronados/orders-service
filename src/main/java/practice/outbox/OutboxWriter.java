package practice.outbox;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import practice.events.OrderCreatedEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;



@Slf4j
@RequiredArgsConstructor
@Component
public class OutboxWriter {



    private static final String AGGREGATE_TYPE = "Order";
    private static final String CORRELATION_ID_KEY = "correlationId";


    private static final String INSERT_SQL =
            """
              INSERT INTO outbox_event
                                    (id, aggregate_type, aggregate_id, event_type, payload, correlation_id)
                                VALUES
                                    (:id, :aggregateType, :aggregateId, :eventType, CAST(:payload AS jsonb), :correlationId)
                    """;

    private final NamedParameterJdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meters;



    @Transactional(propagation = Propagation.MANDATORY)
    public UUID enqueueOrderCreated (OrderCreatedEvent.Payload payload) {
        Objects.requireNonNull(payload, "payload cannot be null");
        Objects.requireNonNull(payload.orderId(), "orderId must be assigned before enqueueing: call this after save()");
        long startedNanos = System.nanoTime();


        UUID eventId = UUID.randomUUID();
        String correlationId = resolveCorrelationId();
        String aggregateId = String.valueOf(payload.orderId());

        OrderCreatedEvent event = new OrderCreatedEvent(
                eventId,
                OrderCreatedEvent.TYPE,
                OrderCreatedEvent.SCHEMA_VERSION,
                Instant.now(),
                correlationId,
                payload);


        jdbc.update(INSERT_SQL, new MapSqlParameterSource()
                .addValue("id", eventId)
                .addValue("aggregateType", AGGREGATE_TYPE)
                .addValue("aggregateId", aggregateId)
                .addValue("eventType", OrderCreatedEvent.TYPE)
                .addValue("payload", toJson(event))
                .addValue("correlationId", correlationId));

        meters.counter("outbox.enqueued", "eventType", OrderCreatedEvent.TYPE).increment();
        log.info("outbox event enqueued eventId={} correlationId={} aggregateId={} eventType={} durationMs={}",
                eventId, correlationId, aggregateId, OrderCreatedEvent.TYPE,
                Duration.ofNanos(System.nanoTime() - startedNanos).toMillis());
        return eventId;

    }
    private String toJson(OrderCreatedEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException ex) {
            // Unchecked on purpose: the business transaction must roll back.
            throw new OutboxSerializationException(
                    "cannot serialize event eventId=" + event.eventId(), ex);
        }
    }


    private String resolveCorrelationId(){
        String current = MDC.get(CORRELATION_ID_KEY);
        if (current == null) {
            String generated = UUID.randomUUID().toString();
            log.warn("no correlationId in MDC, generated one correlationId={}", generated);
            return generated;
        }
        return current;
    }
}
