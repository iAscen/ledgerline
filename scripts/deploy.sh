#!/usr/bin/env bash
# Build the image, side-load it into kind, and install the Helm chart.
set -euo pipefail

CLUSTER="${CLUSTER:-ledgerline}"
IMAGE="${IMAGE:-ledgerline:dev}"
cd "$(dirname "$0")/.."

if [[ "${SKIP_BUILD:-0}" != 1 ]]; then
  docker build -t "$IMAGE" --build-arg VERSION="$(git describe --tags --always --dirty 2>/dev/null || echo dev)" .
fi
kind load docker-image "$IMAGE" --name "$CLUSTER"

helm upgrade --install ledgerline charts/ledgerline \
  --namespace ledgerline --create-namespace \
  -f deploy/kind/values-kind.yaml \
  --wait --timeout 5m
# The tag is constant (:dev), so force new pods to pick up a rebuilt image.
kubectl -n ledgerline rollout restart deployment/ledgerline
kubectl -n ledgerline rollout status deployment/ledgerline --timeout=180s
helm test ledgerline --namespace ledgerline --timeout 2m
kubectl -n ledgerline get pods,svc,hpa,pdb
