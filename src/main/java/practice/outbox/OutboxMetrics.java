package practice.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import practice.repositorios.OutboxRepository;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Component
public class OutboxMetrics {

    private final OutboxRepository repository;
    private final long refreshIntervalMs;
    private final Counter refreshErrors;

    private final AtomicLong pending = new AtomicLong();
    private final AtomicLong oldestPendingAgeMillis = new AtomicLong();
    private final AtomicLong lastSuccessEpochMillis = new AtomicLong(System.currentTimeMillis());

    // Only touched by the single scheduler thread.
    private int consecutiveFailures;

    private ScheduledExecutorService executor;

    public OutboxMetrics(OutboxRepository repository,
                         MeterRegistry meters,
                         @Value("${app.outbox.metrics.refresh-interval-ms}") long refreshIntervalMs) {
        if (refreshIntervalMs < 1000) {
            throw new IllegalArgumentException(
                    "app.outbox.metrics.refresh-interval-ms must be >= 1000, got " + refreshIntervalMs);
        }
        this.repository = repository;
        this.refreshIntervalMs = refreshIntervalMs;
        this.refreshErrors = meters.counter("outbox.metrics.refresh.errors");

        Gauge.builder("outbox.pending", pending, AtomicLong::get)
                .description("Outbox events waiting to be published")
                .register(meters);
        Gauge.builder("outbox.oldest.pending.age", oldestPendingAgeMillis, value -> value.get() / 1000.0)
                .description("Age of the oldest pending outbox event")
                .baseUnit("seconds")
                .register(meters);
        Gauge.builder("outbox.metrics.last.refresh.age", lastSuccessEpochMillis,
                        value -> (System.currentTimeMillis() - value.get()) / 1000.0)
                .description("Seconds since the backlog gauges were last refreshed successfully")
                .baseUnit("seconds")
                .register(meters);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "outbox-metrics");
            thread.setDaemon(true);
            return thread;
        });
        executor.scheduleWithFixedDelay(this::refreshSafely, 0, refreshIntervalMs, TimeUnit.MILLISECONDS);
        log.info("outbox metrics started refreshIntervalMs={}", refreshIntervalMs);
    }

    @PreDestroy
    public void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    private void refreshSafely() {
        try {
            OutboxRepository.BacklogSnapshot snapshot = repository.backlogSnapshot();
            pending.set(snapshot.pending());
            oldestPendingAgeMillis.set(Math.round(snapshot.oldestPendingAgeSeconds() * 1000));
            lastSuccessEpochMillis.set(System.currentTimeMillis());
            if (consecutiveFailures > 0) {
                log.info("outbox metrics refresh recovered after consecutiveFailures={}", consecutiveFailures);
                consecutiveFailures = 0;
            }
        } catch (RuntimeException ex) {
            refreshErrors.increment();
            consecutiveFailures++;
            if (consecutiveFailures == 1) {
                // Full stack trace once per failure streak; later attempts only carry the counter.
                log.warn("outbox metrics refresh failed, keeping last values error={}", ex.toString(), ex);
            } else {
                log.warn("outbox metrics refresh still failing consecutiveFailures={} error={}",
                        consecutiveFailures, ex.toString());
            }
        }
    }
}