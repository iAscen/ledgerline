-- name: InsertJob :one
-- ON CONFLICT DO NOTHING makes the insert the idempotency check: a replayed
-- key returns no row and the caller falls back to GetJobByIdempotencyKey.
INSERT INTO jobs (id, tenant, idempotency_key, request_hash, status, media_type, document, webhook_url, trace_parent)
VALUES ($1, $2, $3, $4, 'queued', $5, $6, $7, $8)
ON CONFLICT (tenant, idempotency_key) DO NOTHING
RETURNING id, tenant, idempotency_key, request_hash, status, media_type, webhook_url, webhook_status, trace_parent,
          result, error, attempts, next_attempt_at, created_at, updated_at, completed_at;

-- name: GetJobByIdempotencyKey :one
SELECT id, tenant, idempotency_key, request_hash, status, media_type, webhook_url, webhook_status, trace_parent,
       result, error, attempts, next_attempt_at, created_at, updated_at, completed_at
FROM jobs
WHERE tenant = $1 AND idempotency_key = $2;

-- name: GetJob :one
SELECT id, tenant, idempotency_key, request_hash, status, media_type, webhook_url, webhook_status, trace_parent,
       result, error, attempts, next_attempt_at, created_at, updated_at, completed_at
FROM jobs
WHERE tenant = $1 AND id = $2;

-- name: ListJobs :many
SELECT id, tenant, idempotency_key, request_hash, status, media_type, webhook_url, webhook_status, trace_parent,
       result, error, attempts, next_attempt_at, created_at, updated_at, completed_at
FROM jobs
WHERE tenant = $1
ORDER BY created_at DESC
LIMIT $2;

-- name: ClaimJob :one
-- FOR UPDATE SKIP LOCKED lets any number of replicas pull from the same table
-- without double-processing. Rows stuck in 'processing' past their lease are
-- reclaimed, so a crashed pod never strands a job.
UPDATE jobs
SET status = 'processing',
    attempts = attempts + 1,
    locked_until = now() + make_interval(secs => sqlc.arg(lease_seconds)::float8),
    updated_at = now()
WHERE id = (
    SELECT j.id FROM jobs j
    WHERE (j.status = 'queued' AND j.next_attempt_at <= now())
       OR (j.status = 'processing' AND j.locked_until < now())
    ORDER BY j.next_attempt_at
    FOR UPDATE SKIP LOCKED
    LIMIT 1
)
RETURNING id, tenant, idempotency_key, request_hash, status, media_type, document, webhook_url, webhook_status,
          trace_parent, result, error, attempts, next_attempt_at, created_at, updated_at, completed_at;

-- name: CompleteJob :exec
UPDATE jobs
SET status = 'succeeded', result = $2, error = NULL, locked_until = NULL, updated_at = now(), completed_at = now()
WHERE id = $1;

-- name: FailJob :exec
UPDATE jobs
SET status = 'failed', error = $2, locked_until = NULL, updated_at = now(), completed_at = now()
WHERE id = $1;

-- name: RescheduleJob :exec
UPDATE jobs
SET status = 'queued',
    error = $2,
    next_attempt_at = $3,
    attempts = GREATEST(attempts - sqlc.arg(refund)::int, 0),
    locked_until = NULL,
    updated_at = now()
WHERE id = $1;

-- name: SetWebhookStatus :exec
UPDATE jobs SET webhook_status = $2, updated_at = now() WHERE id = $1;
