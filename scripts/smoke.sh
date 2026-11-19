#!/usr/bin/env bash
# End-to-end smoke test against a running ledgerline (compose, kind or Fly.io).
#
#   BASE_URL=http://localhost:8080 TOKEN=<jwt> scripts/smoke.sh
#
# If TOKEN is unset, a demo token is requested (needs LEDGERLINE_PUBLIC_DEMO=true).
# Set CHECK_BREAKER=1 to also force provider failures and assert the circuit
# breaker opens (only meaningful with the fake provider and a low threshold).
# Dependencies: bash, curl, base64, grep, sed (no jq, so it runs anywhere).
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
pass() { printf '  \033[32mok\033[0m  %s\n' "$*"; }
fail() { printf '  \033[31mFAIL\033[0m %s\n' "$*"; exit 1; }
field() { grep -o "\"$1\":\"[^\"]*\"" | head -1 | sed 's/.*:"\(.*\)"/\1/'; }

echo "smoke test against $BASE_URL"

curl -fsS "$BASE_URL/healthz" >/dev/null && pass "GET /healthz"
curl -fsS "$BASE_URL/readyz" >/dev/null && pass "GET /readyz"
status=$(curl -fsS "$BASE_URL/v1/status")
echo "$status" | grep -q '"status":"ok"' && pass "GET /v1/status -> $status"

if [[ -z "${TOKEN:-}" ]]; then
  TOKEN=$(curl -fsS -X POST "$BASE_URL/v1/demo/token" | field access_token)
  [[ -n "$TOKEN" ]] || fail "could not obtain a demo token (set TOKEN or enable LEDGERLINE_PUBLIC_DEMO)"
  pass "POST /v1/demo/token"
fi
auth=(-H "Authorization: Bearer $TOKEN")

# A tiny payload that sniffs as image/png. The fake provider derives a
# deterministic receipt from the bytes; the real provider would reject it.
png_b64() { { printf '\x89PNG\r\n\x1a\n'; printf '%s' "$1"; } | base64 | tr -d '\n'; }
body() { printf '{"document_base64":"%s","media_type":"image/png"}' "$(png_b64 "$1")"; }

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/jobs" -H 'Content-Type: application/json' -d "$(body x)")
[[ "$code" == 401 ]] && pass "POST /v1/jobs without token -> 401" || fail "expected 401, got $code"

key="smoke-$(date +%s)-$RANDOM"
doc="smoke receipt $key"
r1=$(curl -fsS -X POST "$BASE_URL/v1/jobs" "${auth[@]}" -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d "$(body "$doc")")
r2=$(curl -fsS -D - -X POST "$BASE_URL/v1/jobs" "${auth[@]}" -H "Idempotency-Key: $key" -H 'Content-Type: application/json' -d "$(body "$doc")")
id1=$(echo "$r1" | field id); id2=$(echo "$r2" | field id)
[[ -n "$id1" && "$id1" == "$id2" ]] || fail "idempotent POST returned different jobs: $id1 vs $id2"
echo "$r2" | grep -qi '^idempotent-replayed: true' || fail "replay missing Idempotent-Replayed header"
pass "idempotent POST twice -> same job $id1 (second answered as a replay)"

code=$(curl -sS -o /dev/null -w '%{http_code}' -X POST "$BASE_URL/v1/jobs" "${auth[@]}" -H "Idempotency-Key: $key" \
  -H 'Content-Type: application/json' -d "$(body "different body")")
[[ "$code" == 422 ]] && pass "same key, different body -> 422" || fail "expected 422, got $code"

for _ in $(seq 1 60); do
  job=$(curl -fsS "$BASE_URL/v1/jobs/$id1" "${auth[@]}")
  st=$(echo "$job" | field status)
  [[ "$st" == succeeded || "$st" == failed ]] && break
  sleep 0.5
done
[[ "$st" == succeeded ]] || fail "job ended as '$st': $job"
pass "job $id1 succeeded: merchant=$(echo "$job" | field merchant)"

if [[ "${CHECK_BREAKER:-0}" == 1 ]]; then
  for i in $(seq 1 6); do
    curl -fsS -o /dev/null -X POST "$BASE_URL/v1/jobs" "${auth[@]}" -H "Idempotency-Key: fail-$key-$i" \
      -H 'Content-Type: application/json' -d "$(body "LEDGERLINE_FAIL $i")"
  done
  for _ in $(seq 1 40); do
    curl -fsS "$BASE_URL/v1/status" | grep -q '"circuit_breaker":"open"' && break
    sleep 0.5
  done
  curl -fsS "$BASE_URL/v1/status" | grep -q '"circuit_breaker":"open"' || fail "breaker did not open"
  pass "forced provider failures -> circuit breaker open"
  curl -fsS "$BASE_URL/readyz" >/dev/null && pass "readyz still 200 while breaker is open (no cascading outage)"
fi

echo "all smoke checks passed"
