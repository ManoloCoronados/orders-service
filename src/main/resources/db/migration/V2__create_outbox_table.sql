CREATE TABLE outbox_event (
                              id              UUID         PRIMARY KEY,
                              aggregate_type  VARCHAR(100) NOT NULL,
                              aggregate_id    VARCHAR(100) NOT NULL,
                              event_type      VARCHAR(100) NOT NULL,
                              payload         JSONB        NOT NULL,
                              correlation_id  VARCHAR(100) NOT NULL,
                              status          VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
                              attempts        INTEGER      NOT NULL DEFAULT 0,
                              next_attempt_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
                              locked_until    TIMESTAMPTZ,
                              last_error      TEXT,
                              created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
                              sent_at         TIMESTAMPTZ,
                              CONSTRAINT ck_outbox_event_status
                                  CHECK (status IN ('PENDING', 'IN_FLIGHT', 'SENT', 'FAILED'))
);

-- claimBatch: PENDING rows whose next_attempt_at is due, oldest first
CREATE INDEX idx_outbox_pending
    ON outbox_event (next_attempt_at)
    WHERE status = 'PENDING';

-- recoverExpiredLeases: IN_FLIGHT rows with an expired lease
CREATE INDEX idx_outbox_in_flight
    ON outbox_event (locked_until)
    WHERE status = 'IN_FLIGHT';

-- purgeSent: SENT rows older than the retention window
CREATE INDEX idx_outbox_sent
    ON outbox_event (sent_at)
    WHERE status = 'SENT';