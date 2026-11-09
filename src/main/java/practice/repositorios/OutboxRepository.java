package practice.repositorios;


import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import practice.events.ClaimedEvent;
import java.util.Map;
import java.util.Objects;
import java.util.List;
import java.util.UUID;


@Repository
@RequiredArgsConstructor
public class OutboxRepository {


    public record RetryUpdate(UUID id, int attempts, long delayMs, String error) {
    }

    public record FailureUpdate(UUID id, int attempts, String error) {
    }
    public record BacklogSnapshot(long pending, double oldestPendingAgeSeconds) {
    }


    public static final int MAX_ERROR_LENGTH = 1000;
    private static final int PURGE_CHUNK_SIZE = 1000;

    private static final String BACKLOG_SQL = """
        SELECT count(*) AS pending,
               COALESCE(EXTRACT(EPOCH FROM (now() - min(created_at))), 0) AS oldest_age_seconds
        FROM outbox_event
        WHERE status = 'PENDING'
        """;

    private static final RowMapper<ClaimedEvent> CLAIMED_MAPPER = (rs, rowNum) -> new ClaimedEvent(
            rs.getObject("id", UUID.class),
            rs.getString("aggregate_id"),
            rs.getString("event_type"),
            rs.getString("payload"),
            rs.getString("correlation_id"),
            rs.getInt("attempts"));

    private final NamedParameterJdbcTemplate jdbc;

    @Transactional(propagation = Propagation.MANDATORY)
    public List<ClaimedEvent> claimBatch(int batchSize, int leaseSeconds) {
        String sql = """
                WITH claimed AS (
                    SELECT id
                    FROM outbox_event
                    WHERE status = 'PENDING' AND next_attempt_at <= now()
                    ORDER BY next_attempt_at
                    LIMIT :batchSize
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE outbox_event o
                SET status = 'IN_FLIGHT',
                    attempts = o.attempts + 1,
                    locked_until = now() + (:leaseSeconds * INTERVAL '1 second')
                FROM claimed c
                WHERE o.id = c.id
                RETURNING o.id, o.aggregate_id, o.event_type,
                          CAST(o.payload AS text) AS payload,
                          o.correlation_id, o.attempts
                """;
        return jdbc.query(sql,
                new MapSqlParameterSource()
                        .addValue("batchSize", batchSize)
                        .addValue("leaseSeconds", leaseSeconds),
                CLAIMED_MAPPER);
    }
    @Transactional(propagation = Propagation.MANDATORY)
    public void markSent(List<ClaimedEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        String sql = """
                UPDATE outbox_event
                SET status = 'SENT', sent_at = now(), locked_until = NULL, last_error = NULL
                WHERE id = :id AND attempts = :attempts AND status = 'IN_FLIGHT'
                """;
        jdbc.batchUpdate(sql, events.stream()
                .map(e -> new MapSqlParameterSource()
                        .addValue("id", e.id())
                        .addValue("attempts", e.attempts()))
                .toArray(MapSqlParameterSource[]::new));
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void markRetry(List<RetryUpdate> updates) {
        if (updates.isEmpty()) {
            return;
        }
        String sql = """
                UPDATE outbox_event
                SET status = 'PENDING',
                    next_attempt_at = now() + (:delayMs * INTERVAL '1 millisecond'),
                    locked_until = NULL,
                    last_error = :error
                WHERE id = :id AND attempts = :attempts AND status = 'IN_FLIGHT'
                """;
        jdbc.batchUpdate(sql, updates.stream()
                .map(u -> new MapSqlParameterSource()
                        .addValue("id", u.id())
                        .addValue("attempts", u.attempts())
                        .addValue("delayMs", u.delayMs())
                        .addValue("error", truncate(u.error())))
                .toArray(MapSqlParameterSource[]::new));
    }

    @Transactional(propagation = Propagation.MANDATORY)
    public void markFailed(List<FailureUpdate> updates) {
        if (updates.isEmpty()) {
            return;
        }
        String sql = """
                UPDATE outbox_event
                SET status = 'FAILED', locked_until = NULL, last_error = :error
                WHERE id = :id AND attempts = :attempts AND status = 'IN_FLIGHT'
                """;

        jdbc.batchUpdate(sql, updates.stream()
                .map(u -> new MapSqlParameterSource()
                        .addValue("id", u.id())
                        .addValue("attempts", u.attempts())
                        .addValue("error", truncate(u.error())))
                .toArray(MapSqlParameterSource[]::new));
    }


    public int recoverExpiredLeases(int maxAttempts) {
        String sql = """
                UPDATE outbox_event
                SET status = CASE WHEN attempts >= :maxAttempts THEN 'FAILED' ELSE 'PENDING' END,
                    locked_until = NULL,
                    next_attempt_at = now(),
                    last_error = 'lease expired'
                WHERE status = 'IN_FLIGHT' AND locked_until < now()
                """;
        return jdbc.update(sql, new MapSqlParameterSource().addValue("maxAttempts", maxAttempts));
    }

    public int purgeSent(int retentionDays) {
        String sql = """
            DELETE FROM outbox_event
            WHERE id IN (
                SELECT id FROM outbox_event
                WHERE status = 'SENT' AND sent_at < now() - (:days * INTERVAL '1 day')
                LIMIT :chunkSize
            )
            """;
        int total = 0;
        int deleted;
        do {
            deleted = jdbc.update(sql, new MapSqlParameterSource()
                    .addValue("days", retentionDays)
                    .addValue("chunkSize", PURGE_CHUNK_SIZE));
            total += deleted;
        } while (deleted == PURGE_CHUNK_SIZE);
        return total;
    }
    public BacklogSnapshot backlogSnapshot() {
        return Objects.requireNonNull(jdbc.queryForObject(BACKLOG_SQL, Map.of(),
                (rs, rowNum) -> new BacklogSnapshot(
                        rs.getLong("pending"), rs.getDouble("oldest_age_seconds"))));
    }

    private static final String truncate(String value) {
        if(value == null) {
            return null;
        }
        return value.length() <= MAX_ERROR_LENGTH ? value : value.substring(0, MAX_ERROR_LENGTH);
    }


}