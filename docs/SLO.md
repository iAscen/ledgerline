# Service Level Objectives

Ledgerline has two user journeys with different shapes, so they get separate
SLOs: the **synchronous API** (accepting and reading jobs) and the
**asynchronous pipeline** (turning an accepted job into a result).

All SLIs are computed from the Prometheus metrics the service exports on
`:9090/metrics`. Window: **28 days, rolling**.

## SLO 1: API availability

| | |
|---|---|
| **SLI** | Proportion of API requests that do not fail because of us: non-5xx responses / all responses, on `/v1/jobs*` routes. 4xx (bad input, auth, rate limit) count as good: they are correct behaviour. |
| **Objective** | **99.9 %** |
| **Error budget** | 0.1 % of requests, about 40 minutes of full outage per 28 days |

```promql
# SLI (ratio of good requests), 28d
sum(rate(ledgerline_http_requests_total{route=~".*/v1/jobs.*", code!~"5.."}[28d]))
/
sum(rate(ledgerline_http_requests_total{route=~".*/v1/jobs.*"}[28d]))
```

## SLO 2: API latency

| | |
|---|---|
| **SLI** | Proportion of `POST /v1/jobs` and `GET /v1/jobs/{id}` requests served in under 300 ms. |
| **Objective** | **99 %** |

```promql
sum(rate(ledgerline_http_request_duration_seconds_bucket{route=~"POST /v1/jobs|GET /v1/jobs/\\{id\\}", le="0.25"}[28d]))
/
sum(rate(ledgerline_http_request_duration_seconds_count{route=~"POST /v1/jobs|GET /v1/jobs/\\{id\\}"}[28d]))
```

(`0.25` is the nearest histogram bucket below 300 ms, so the measurement is
slightly stricter than the objective.)

Accepting a job is a single indexed `INSERT`, so this target is about the
service and its database, never about the model provider. That is the point of
making extraction asynchronous.

## SLO 3: Pipeline freshness

| | |
|---|---|
| **SLI** | Proportion of jobs reaching a terminal state within 60 s of creation. |
| **Objective** | **95 %** |

```promql
sum(rate(ledgerline_job_end_to_end_seconds_bucket{le="60"}[28d]))
/
sum(rate(ledgerline_job_end_to_end_seconds_count[28d]))
```

## SLO 4: Pipeline correctness

| | |
|---|---|
| **SLI** | Proportion of finished jobs that succeeded, excluding permanent input errors (documents that are not receipts are correct "failures"). |
| **Objective** | **99 %** |

```promql
sum(rate(ledgerline_jobs_finished_total{status="succeeded"}[28d]))
/
(
  sum(rate(ledgerline_jobs_finished_total[28d]))
  - sum(rate(ledgerline_provider_requests_total{outcome="permanent_error"}[28d]))
)
```

## What is deliberately *not* an SLO

* **Provider latency/availability.** The extraction provider (Claude) is a
  dependency, not something we control. Its failures are absorbed by retries,
  backoff and the circuit breaker and only show up in SLO 3/4 if they outlast
  the retry budget. `ledgerline_provider_request_duration_seconds` is a
  diagnostic, not an objective.
* **Webhook delivery.** The receiver's uptime is not ours. Delivery outcomes
  are tracked (`ledgerline_webhook_deliveries_total`) and visible on the job
  (`webhook_status`), and polling remains the source of truth.

## Alerting: multi-window, multi-burn-rate

Page on fast burns, ticket on slow burns (Google SRE Workbook, ch. 5). For the
99.9 % availability SLO (budget = 0.001):

| Severity | Burn rate | Long window | Short window | Budget consumed when it fires |
|---|---|---|---|---|
| Page | 14.4x | 1 h | 5 min | 2 % in 1 h |
| Page | 6x | 6 h | 30 min | 5 % in 6 h |
| Ticket | 1x | 3 d | 6 h | 10 % in 3 d |

```yaml
# Prometheus rule, fast-burn page (pattern repeats for the other rows)
- alert: LedgerlineAPIErrorBudgetFastBurn
  expr: |
    (
      sum(rate(ledgerline_http_requests_total{route=~".*/v1/jobs.*",code=~"5.."}[1h]))
      / sum(rate(ledgerline_http_requests_total{route=~".*/v1/jobs.*"}[1h]))
    ) > (14.4 * 0.001)
    and
    (
      sum(rate(ledgerline_http_requests_total{route=~".*/v1/jobs.*",code=~"5.."}[5m]))
      / sum(rate(ledgerline_http_requests_total{route=~".*/v1/jobs.*"}[5m]))
    ) > (14.4 * 0.001)
  labels: { severity: page }
  annotations:
    runbook_url: https://github.com/iAscen/ledgerline/blob/main/docs/RUNBOOK.md#api-5xx
```

Supporting (non-SLO) alerts, all linked to the runbook:

| Alert | Condition | Severity |
|---|---|---|
| `LedgerlineCircuitOpen` | `ledgerline_circuit_breaker_state == 1` for 10 min | ticket |
| `LedgerlineQueueStuck` | `rate(ledgerline_jobs_created_total[15m]) > 0` and `rate(ledgerline_jobs_finished_total[15m]) == 0` | page |
| `LedgerlineWebhookFailures` | failed / all webhook deliveries > 20 % over 1 h | ticket |
| `LedgerlineNotReady` | ready replicas < 1 for 5 min | page |

## Error budget policy

* Budget remaining: ship normally.
* Budget exhausted: freeze feature releases for the service. Only reliability
  fixes and security patches ship until the 28-day SLI is back above target.
* A single incident that burns more than 20 % of the budget gets a written
  postmortem (blameless, with action items tracked to completion).
