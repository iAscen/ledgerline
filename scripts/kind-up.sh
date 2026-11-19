#!/usr/bin/env bash
# Create the kind cluster with Postgres and a dev Vault (official Helm chart).
set -euo pipefail

CLUSTER="${CLUSTER:-ledgerline}"
VAULT_CHART_VERSION="${VAULT_CHART_VERSION:-0.34.1}"
cd "$(dirname "$0")/.."

if ! kind get clusters 2>/dev/null | grep -qx "$CLUSTER"; then
  kind create cluster --config deploy/kind/cluster.yaml --name "$CLUSTER" --wait 120s
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

kubectl create namespace ledgerline --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -f deploy/kind/postgres.yaml
kubectl -n ledgerline rollout status statefulset/postgres --timeout=180s

helm repo add hashicorp https://helm.releases.hashicorp.com >/dev/null 2>&1 || true
helm repo update hashicorp >/dev/null
helm upgrade --install vault hashicorp/vault --version "$VAULT_CHART_VERSION" \
  --namespace vault --create-namespace -f deploy/kind/vault-values.yaml --wait --timeout 5m

scripts/vault-setup.sh
echo "kind cluster '$CLUSTER' ready (postgres + vault). Next: make deploy"
