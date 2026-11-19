# Runbook

Operational procedures for ledgerline. Every alert in [SLO.md](SLO.md) links
to a section here. Commands assume the `ledgerline` namespace.

```bash
NS=ledgerline
kubectl -n $NS get pods,hpa,pdb
kubectl -n $NS logs deploy/ledgerline --tail=200 | grep -v '"level":"INFO"'
kubectl -n $NS port-forward svc/ledgerline 9090:9090   # then curl localhost:9090/metrics
```

Every log line inside a request or job carries `trace_id`; paste it into the
tracing backend (Jaeger locally) to see the whole request → job → provider →
webhook path.

---

## API 5xx

**Alert:** `LedgerlineAPIErrorBudgetFastBurn` / slow burn.

1. Which route? `sum by (route, code) (rate(ledgerline_http_requests_total{code=~"5.."}[5m]))`.
2. Logs: `kubectl -n $NS logs deploy/ledgerline | grep '"level":"ERROR"'`.
   `request failed` lines carry the underlying error.
3. Most likely cause is the database (5xx on every route at once):
   * `kubectl -n $NS exec deploy/ledgerline -- ledgerline healthcheck -url http://127.0.0.1:8080/readyz`
     returns 503 with `"store":"unavailable"` when Postgres is unreachable.
   * Check the database's own health and connection count. pgx pools per pod,
     so connections = replicas × pool size; an HPA scale-out can exhaust
     `max_connections`.
4. If it started with a deploy: `helm -n $NS rollback ledgerline` (the rolling
   update uses `maxUnavailable: 0`, so a rollback is zero-downtime).

## Circuit breaker open

**Alert:** `LedgerlineCircuitOpen`. Also visible at `GET /v1/status`
(`"circuit_breaker":"open"`) and in logs as `circuit breaker state change`.

What it means: the extraction provider failed `BREAKER_THRESHOLD` times in a
row, so calls are being short-circuited for `BREAKER_OPEN_TIMEOUT`, then one
probe is let through. **Jobs are not lost**: they are rescheduled without
consuming a retry attempt, and complete once the provider recovers. The API
stays available and pods stay Ready (by design, `/readyz` does not depend on
the breaker, so a provider outage cannot cascade into a full outage).

1. Is the provider down? Check its status page and
   `sum by (outcome) (rate(ledgerline_provider_requests_total[5m]))`.
   * `transient_error` / `timeout` spike: upstream outage or overload (HTTP
     5xx / 529). Nothing to do but wait; the breaker probes automatically.
   * Every request fails immediately after a deploy or secret rotation: likely
     a bad API key (401/403 counts as transient so that it *does* open the
     breaker). Verify the key in Vault:
     `vault kv get -field=anthropic_api_key secret/ledgerline | cut -c1-10`.
2. Size the backlog in Postgres: `SELECT status, count(*) FROM jobs GROUP BY 1;`
3. After recovery, watch `ledgerline_jobs_finished_total` drain the backlog.
   Throughput is bounded by `WORKER_CONCURRENCY × replicas`; scale the
   Deployment temporarily if the backlog is large.

## Queue stuck

**Alert:** `LedgerlineQueueStuck` (jobs created, none finishing).

1. Are workers alive? Each pod runs the pool; check pods are Running and not
   crash-looping.
2. Breaker open? See above. If so, this alert is a symptom.
3. Stuck leases: a job in `processing` whose pod died is reclaimed
   automatically once `locked_until` passes (lease = job timeout + 30 s).
   ```sql
   SELECT id, attempts, locked_until, error FROM jobs
   WHERE status = 'processing' ORDER BY locked_until LIMIT 20;
   ```
4. Jobs failing permanently are not "stuck"; look at `error` on failed jobs.

## Webhook failures

**Alert:** `LedgerlineWebhookFailures`.

Deliveries retry 3 times with backoff, then mark `webhook_status=failed` on the
job. The job result itself is safe; clients can always poll.

* `webhook destination resolves to a private or reserved address`: the
  customer gave an internal URL; this is the SSRF guard working.
* 4xx from the receiver: their endpoint rejects us (often signature
  verification with a wrong secret). 4xx are not retried.
* Known gap: a pod that dies after storing a result but before delivering the
  webhook skips that delivery. A transactional outbox table is the fix if
  delivery must be guaranteed.

## Not ready / crash-looping

1. `kubectl -n $NS describe pod <pod>`; read the last state and events.
2. Startup errors are explicit, e.g.
   `jwt_secret is not configured (Vault or LEDGERLINE_JWT_SECRET)` or
   `vault: kubernetes auth login: ...`.
3. Vault login failures:
   * `permission denied`: the role is bound to a different SA or namespace.
     `vault read auth/kubernetes/role/ledgerline`.
   * Vault sealed / unreachable: with `vault.required=true` pods refuse to
     start rather than run without secrets. That is intentional.

## Rotating secrets

Secrets are read from Vault at startup.

1. `vault kv patch secret/ledgerline jwt_secret=<new>` (or the field to rotate).
2. `kubectl -n $NS rollout restart deploy/ledgerline`. The rolling update plus
   PDB keeps the service available.
3. Rotating `jwt_secret` invalidates every issued token; coordinate with
   clients or do it during a maintenance window. Rotating `webhook_secret`
   requires receivers to accept the new signature.

## Graceful shutdown (what to expect during deploys)

On SIGTERM a pod: marks itself not-ready, waits `READINESS_DRAIN` (5 s) for
endpoint removal to propagate, finishes in-flight HTTP requests, stops claiming
jobs and finishes the ones in flight, all within `SHUTDOWN_TIMEOUT` (25 s),
below `terminationGracePeriodSeconds` (40 s). A job still running at the
deadline keeps its lease and is retried elsewhere once the lease expires, so
it is delayed, never lost.
