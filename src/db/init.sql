CREATE TABLE endpoints (
    id           UUID PRIMARY KEY,
    tenant_id    TEXT NOT NULL,
    url          TEXT NOT NULL,
    created_at   TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE events (
    id              UUID PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    endpoint_id     UUID REFERENCES endpoints(id),
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending|in_flight|succeeded|failed|dead
    attempts        INT  NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_events_due ON events (next_attempt_at) WHERE status = 'pending';

ALTER TABLE events ADD COLUMN idempotency_key TEXT;
CREATE UNIQUE INDEX idx_events_idem
    ON events (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE delivery_attempts (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID REFERENCES events(id),
    attempt_number  INT  NOT NULL,
    status_code     INT,                  -- HTTP status returned
    error           TEXT,                 -- if request failed pre-response
    duration_ms     INT,
    attempted_at    TIMESTAMPTZ DEFAULT now()
);