package practice.outbox;

import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "app.outbox")
public record OutboxProperties(
        @Min(1) int retentionDays,
        @NotNull @Valid Relay relay) {

    public record Relay(
            @Min(1) int workers,
            @Min(1) int batchSize,
            @Min(10) long pollIntervalMs,
            @Min(1) int leaseSeconds,
            @Min(1) int maxAttempts,
            @Min(1) long backoffBaseMs,
            @Min(1) long backoffMaxMs,
            @Min(1) long publishTimeoutMs,
            @Min(1) int shutdownAwaitSeconds) {

        @AssertTrue(message = "app.outbox.relay.lease-seconds must be greater than "
                + "app.outbox.relay.publish-timeout-ms (converted to seconds)")
        public boolean isLeaseLongerThanPublishTimeout() {
            return leaseSeconds * 1000L > publishTimeoutMs;
        }

        @AssertTrue(message = "app.outbox.relay.backoff-max-ms must be >= backoff-base-ms")
        public boolean isBackoffCapNotBelowBase() {
            return backoffMaxMs >= backoffBaseMs;
        }
    }
}