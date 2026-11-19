-- +goose Up
CREATE TABLE jobs (
    id               UUID        PRIMARY KEY,
    tenant           TEXT        NOT NULL,
    idempotency_key  TEXT        NOT NULL,
    request_hash     TEXT        NOT NULL,
    status           TEXT        NOT NULL CHECK (status IN ('queued', 'processing', 'succeeded', 'failed')),
    media_type       TEXT        NOT NULL,
    document         BYTEA       NOT NULL,
    webhook_url      TEXT,
    webhook_status   TEXT,
    trace_parent     TEXT,
    result           JSONB,
    error            TEXT,
    attempts         INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until     TIMESTAMPTZ,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at     TIMESTAMPTZ,
    CONSTRAINT jobs_tenant_idempotency_key UNIQUE (tenant, idempotency_key)
);

-- Partial index: the worker only ever scans claimable rows.
CREATE INDEX jobs_claimable ON jobs (next_attempt_at) WHERE status IN ('queued', 'processing');
CREATE INDEX jobs_tenant_created ON jobs (tenant, created_at DESC);

-- +goose Down
DROP TABLE jobs;
