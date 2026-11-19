#!/usr/bin/env bash
# Configure the dev Vault in kind:
#   - KV v2 secret secret/ledgerline holding the service's secrets
#   - a least-privilege policy that can only read that one path
#   - Kubernetes auth, with a role bound to the ledgerline service account
# Secrets are generated here and never written to disk or to the repo.
set -euo pipefail

VAULT_NS="${VAULT_NS:-vault}"
APP_NS="${APP_NS:-ledgerline}"
APP_SA="${APP_SA:-ledgerline}"

vault() { kubectl -n "$VAULT_NS" exec -i vault-0 -- env VAULT_TOKEN=root vault "$@"; }
rand() { head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; }

kubectl -n "$VAULT_NS" wait --for=condition=Ready pod/vault-0 --timeout=180s

pg_password=$(kubectl -n "$APP_NS" get secret postgres -o jsonpath='{.data.POSTGRES_PASSWORD}' | base64 -d)

vault kv put secret/ledgerline \
  jwt_secret="$(rand)" \
  webhook_secret="$(rand)" \
  database_url="postgres://ledgerline:${pg_password}@postgres.${APP_NS}.svc:5432/ledgerline?sslmode=disable" >/dev/null

vault policy write ledgerline - <<'EOF' >/dev/null
path "secret/data/ledgerline" {
  capabilities = ["read"]
}
EOF

vault auth list -format=json | grep -q '"kubernetes/"' || vault auth enable kubernetes >/dev/null
# Vault runs in-cluster, so it can use its own service account to call TokenReview.
vault write auth/kubernetes/config kubernetes_host="https://kubernetes.default.svc:443" >/dev/null
vault write auth/kubernetes/role/ledgerline \
  bound_service_account_names="$APP_SA" \
  bound_service_account_namespaces="$APP_NS" \
  policies=ledgerline \
  ttl=1h >/dev/null

echo "vault configured: secret/ledgerline, policy 'ledgerline', kubernetes role 'ledgerline' -> sa $APP_NS/$APP_SA"
