package practice.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import practice.events.ClaimedEvent;
import practice.repositorios.OutboxRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static practice.outbox.PublishErrorClassifier.classify;

@Slf4j
@Component
public class OutboxRelay {

    private static final String HEADER_CORRELATION_ID = "correlation-id";
    private static final String HEADER_EVENT_ID = "event-id";
    private static final String HEADER_EVENT_TYPE = "event-type";
    private static final int PURGE_EVERY_N_MAINTENANCE_RUNS = 120;


    private record InFlight(ClaimedEvent event,
                            CompletableFuture<SendResult<String, String>> future,
                            long startedNanos) {
    }

    private record Settlement(List<ClaimedEvent> sent,
                              List<OutboxRepository.RetryUpdate> retry,
                              List<OutboxRepository.FailureUpdate> failed,
                              boolean interrupted) {
    }

    private final OutboxRepository repository;
    private final TransactionTemplate tx;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxProperties.Relay cfg;
    private final int retentionDays;
    private final String topic;
    private final MeterRegistry meters;

    private final AtomicInteger maintenanceRuns = new AtomicInteger();
    private volatile boolean running;
    private ScheduledExecutorService executor;

    public OutboxRelay(
            OutboxRepository repository,
            PlatformTransactionManager txManager,
            KafkaTemplate<String, String> kafka,
            OutboxProperties props,
            @Value("${app.kafka.topics.order-created}") String topic,
            MeterRegistry meters) {
        this.repository = repository;
        this.tx = new TransactionTemplate(txManager);
        this.kafka = kafka;
        this.cfg = props.relay();
        this.retentionDays = props.retentionDays();
        this.topic = topic;
        this.meters = meters;
    }


    /**
     *seq.incrementAndGet() metodo de AtomicInteger
     *
     */
    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        running = true;
        AtomicInteger seq = new AtomicInteger();
        ThreadFactory threads = r -> {
            Thread t = new Thread(r, "outbox-relay-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        };

        executor = Executors.newScheduledThreadPool(cfg.workers() + 1, threads);
        for (int i = 0; i < cfg.workers(); i++) {
            executor.scheduleWithFixedDelay(this::runCycleSafely,
                    cfg.pollIntervalMs(), cfg.pollIntervalMs(), TimeUnit.MILLISECONDS);
        }
        executor.scheduleWithFixedDelay(this::maintainSafely, 10_000, 30_000, TimeUnit.MILLISECONDS);

        log.info("outbox relay started topic={} workers={} batchSize={} pollIntervalMs={}",
                topic, cfg.workers(), cfg.batchSize(), cfg.pollIntervalMs());
    }

    @PreDestroy
    public void stop() {
        running = false;
        if (executor == null) {
            return;
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(cfg.shutdownAwaitSeconds(), TimeUnit.SECONDS)) {
                log.warn("outbox relay did not stop within {}s, forcing shutdown", cfg.shutdownAwaitSeconds());
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
        log.info("outbox relay stopped");
    }

    // An exception escaping a scheduled task would cancel it forever.
    private void runCycleSafely() {
        try {
            runCycle();
        } catch (RuntimeException ex) {
            meters.counter("outbox.cycle.errors").increment();
            log.error("outbox cycle failed error={}", ex.toString(), ex);
        }
    }

    private void maintainSafely() {
        try {
            maintain();
        } catch (RuntimeException ex) {
            log.error("outbox maintenance failed error={}", ex.toString(), ex);
        }
    }

    private void runCycle() {
        if (!running) {
            return;
        }
        // 1. Short transaction: claim a batch and commit before touching the network.
        List<ClaimedEvent> batch = tx.execute(s -> repository.claimBatch(cfg.batchSize(), cfg.leaseSeconds()));
        if (batch == null || batch.isEmpty()) {
            return;
        }
        long startedNanos = System.nanoTime();

        // 2. No transaction open: publish and wait for acknowledgements.
        List<InFlight> pending = sendAll(batch);
        Settlement settlement = awaitAndClassify(pending);

        // 3. Short transaction: record the outcome of each event.
        settle(settlement);

        log.info("outbox batch completed claimed={} sent={} retry={} failed={} durationMs={}",
                batch.size(), settlement.sent().size(), settlement.retry().size(),
                settlement.failed().size(), Duration.ofNanos(System.nanoTime() - startedNanos).toMillis());

        if (settlement.interrupted()) {
            Thread.currentThread().interrupt();
        }
    }

    private List<InFlight> sendAll(List<ClaimedEvent> batch) {
        List<InFlight> inFlight = new ArrayList<>(batch.size());
        Throwable brokerFailure = null;

        for (ClaimedEvent event : batch) {
            long started = System.nanoTime();
            CompletableFuture<SendResult<String, String>> future;

            if (brokerFailure != null) {
                // Broker unreachable: do not block again on every remaining record.
                future = CompletableFuture.failedFuture(brokerFailure);
            } else {
                try {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                            topic, (Integer) null, event.aggregateId(), event.payload(), headersOf(event));
                    future = kafka.send(record);
                } catch (RuntimeException e) {
                    future = CompletableFuture.failedFuture(e);
                }
                if (future.isCompletedExceptionally()) {
                    Throwable cause = failureOf(future);
                    if (classify(cause) == PublishErrorClassifier.Kind.RETRYABLE) {
                        brokerFailure = cause;
                    }
                }
            }
            inFlight.add(new InFlight(event, future, started));
        }
        return inFlight;
    }

    private static Throwable failureOf(CompletableFuture<?> future) {
        try {
            future.join();
            return null;
        } catch (CompletionException | CancellationException e) {
            return e;
        }
    }

    private Settlement awaitAndClassify(List<InFlight> pending) {
        List<ClaimedEvent> sent = new ArrayList<>();
        List<OutboxRepository.RetryUpdate> retry = new ArrayList<>();
        List<OutboxRepository.FailureUpdate> failed = new ArrayList<>();
        boolean interrupted = false;
        long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(cfg.publishTimeoutMs());

        for (InFlight item : pending) {
            ClaimedEvent event = item.event();
            if (interrupted) {
                classifyFailure(event, new InterruptedException("relay interrupted"), retry, failed);
                continue;
            }
            try {
                long remaining = Math.max(0L, deadlineNanos - System.nanoTime());
                SendResult<String, String> result = item.future().get(remaining, TimeUnit.NANOSECONDS);
                sent.add(event);
                Duration latency = Duration.ofNanos(System.nanoTime() - item.startedNanos());
                meters.timer("outbox.publish.latency").record(latency);
                meters.counter("outbox.publish", "outcome", "sent").increment();
                logSent(event, result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset(), latency.toMillis());
            } catch (InterruptedException ie) {
                interrupted = true;
                classifyFailure(event, ie, retry, failed);
            } catch (ExecutionException | TimeoutException ex) {
                // After a timeout the message may still arrive: a duplicate is possible (at-least-once).
                classifyFailure(event, ex, retry, failed);
            }
        }
        return new Settlement(sent, retry, failed, interrupted);
    }

    private void classifyFailure(ClaimedEvent event, Throwable error,
                                 List<OutboxRepository.RetryUpdate> retry,
                                 List<OutboxRepository.FailureUpdate> failed) {
        Throwable root = PublishErrorClassifier.unwrap(error);
        String description = root.getClass().getSimpleName() + ": " + root.getMessage();
        boolean fatal = classify(error) == PublishErrorClassifier.Kind.FATAL;
        boolean exhausted = event.attempts() >= cfg.maxAttempts();

        try (MDC.MDCCloseable c1 = MDC.putCloseable("correlationId", event.correlationId());
             MDC.MDCCloseable c2 = MDC.putCloseable("eventId", event.id().toString())) {
            if (fatal || exhausted) {
                failed.add(new OutboxRepository.FailureUpdate(event.id(), event.attempts(), description));
                meters.counter("outbox.publish", "outcome", "failed").increment();
                log.error("outbox publish failed permanently eventId={} correlationId={} aggregateId={} attempt={} fatal={} error={}",
                        event.id(), event.correlationId(), event.aggregateId(), event.attempts(), fatal, description);
            } else {
                long delayMs = BackoffCalculator.delayMs(event.attempts(), cfg.backoffBaseMs(), cfg.backoffMaxMs());
                retry.add(new OutboxRepository.RetryUpdate(event.id(), event.attempts(), delayMs, description));
                meters.counter("outbox.publish", "outcome", "retry").increment();
                log.warn("outbox publish failed, will retry eventId={} correlationId={} aggregateId={} attempt={} nextDelayMs={} error={}",
                        event.id(), event.correlationId(), event.aggregateId(), event.attempts(), delayMs, description);
            }
        }
    }

    private void settle(Settlement settlement) {
        try {
            tx.executeWithoutResult(status -> {
                repository.markSent(settlement.sent());
                repository.markRetry(settlement.retry());
                repository.markFailed(settlement.failed());
            });
        } catch (RuntimeException ex) {
            // If the database fails here, events stay IN_FLIGHT until the lease expires and are
            // then recovered: nothing is lost, but duplicates are possible.
            meters.counter("outbox.settle.errors").increment();
            log.error("outbox settle failed, events stay IN_FLIGHT until lease expires sent={} retry={} failed={} error={}",
                    settlement.sent().size(), settlement.retry().size(), settlement.failed().size(),
                    ex.toString(), ex);
        }
    }

    private void maintain() {
        if (!running) {
            return;
        }
        int recovered = repository.recoverExpiredLeases(cfg.maxAttempts());
        if (recovered > 0) {
            meters.counter("outbox.leases.recovered").increment(recovered);
            log.warn("outbox recovered expired leases count={}", recovered);
        }
        if (maintenanceRuns.incrementAndGet() % PURGE_EVERY_N_MAINTENANCE_RUNS == 0) {
            int purged = repository.purgeSent(retentionDays);
            log.info("outbox purge completed deleted={} retentionDays={}", purged, retentionDays);
        }
    }

    private void logSent(ClaimedEvent event, int partition, long offset, long latencyMs) {
        try (MDC.MDCCloseable c1 = MDC.putCloseable("correlationId", event.correlationId());
             MDC.MDCCloseable c2 = MDC.putCloseable("eventId", event.id().toString())) {
            log.info("outbox event published eventId={} correlationId={} aggregateId={} topic={} partition={} offset={} attempt={} latencyMs={}",
                    event.id(), event.correlationId(), event.aggregateId(), topic,
                    partition, offset, event.attempts(), latencyMs);
        }
    }

    private static List<Header> headersOf(ClaimedEvent event) {
        return List.of(
                new RecordHeader(HEADER_CORRELATION_ID, event.correlationId().getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(HEADER_EVENT_ID, event.id().toString().getBytes(StandardCharsets.UTF_8)),
                new RecordHeader(HEADER_EVENT_TYPE, event.eventType().getBytes(StandardCharsets.UTF_8)));
    }



}