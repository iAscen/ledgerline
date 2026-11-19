#!/usr/bin/env bash
# Port-forward the in-cluster Service and run the smoke test through it.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT="${PORT:-28080}"
if curl -s -o /dev/null "http://localhost:$PORT/" 2>/dev/null; then
  echo "port $PORT is already in use; set PORT=<free port>"; exit 1
fi
kubectl -n ledgerline port-forward svc/ledgerline "$PORT:80" >/tmp/ledgerline-pf.log 2>&1 &
pf=$!
trap 'kill $pf 2>/dev/null || true' EXIT
for _ in $(seq 1 30); do curl -fsS "http://localhost:$PORT/healthz" >/dev/null 2>&1 && break; sleep 1; done

# Prove the pods really got their secrets from Vault via Kubernetes auth.
kubectl -n ledgerline logs deploy/ledgerline | grep -m1 'secrets resolved' | grep -Eq 'jwt_secret"?:"?vault' \
  && echo "  ok  secrets resolved from Vault (kubernetes auth)" \
  || { echo "  FAIL secrets were not loaded from Vault"; exit 1; }

BASE_URL="http://localhost:$PORT" CHECK_BREAKER="${CHECK_BREAKER:-1}" scripts/smoke.sh
