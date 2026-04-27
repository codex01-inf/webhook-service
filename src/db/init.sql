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