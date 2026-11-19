// Package secrets resolves the service's secrets from HashiCorp Vault
// (Kubernetes auth + KV v2) with a per-key fallback to environment variables.
//
// In Kubernetes the pod authenticates with its projected service account
// token, so no long-lived Vault token or secret ever lands in a manifest,
// Helm values file or container image. Locally (docker compose, Fly.io
// secrets) the same keys come from LEDGERLINE_* variables.
package secrets

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	vault "github.com/hashicorp/vault/api"
	k8sauth "github.com/hashicorp/vault/api/auth/kubernetes"

	"github.com/iAscen/ledgerline/internal/config"
)

// Keys of the secrets the service understands. In Vault they are fields of
// one KV v2 secret; in the environment they are LEDGERLINE_<UPPER_KEY>.
const (
	JWTSecret       = "jwt_secret"
	AnthropicAPIKey = "anthropic_api_key" //nolint:gosec // key name, not a credential
	WebhookSecret   = "webhook_secret"
	DatabaseURL     = "database_url"
)

var allKeys = []string{JWTSecret, AnthropicAPIKey, WebhookSecret, DatabaseURL}

// Secrets is a resolved secret set. Values never go to logs; Sources records
// where each key came from, which is safe to log.
type Secrets struct {
	values  map[string]string
	Sources map[string]string // key -> "vault" | "env"
}

// Get returns a secret value ("" when unset).
func (s Secrets) Get(key string) string { return s.values[key] }

// KVReader abstracts the Vault read so it can be faked in tests.
type KVReader func(ctx context.Context) (map[string]any, error)

// Load resolves secrets. If Vault is configured it is consulted first; any key
// it does not provide falls back to the environment. If Vault is configured
// but unreachable, Load falls back to env unless cfg.Required is set.
func Load(ctx context.Context, cfg config.VaultConfig, log *slog.Logger) (Secrets, error) {
	var reader KVReader
	if cfg.Addr != "" {
		reader = func(ctx context.Context) (map[string]any, error) { return readVault(ctx, cfg) }
	}
	return resolve(ctx, reader, cfg.Required, log)
}

func resolve(ctx context.Context, reader KVReader, required bool, log *slog.Logger) (Secrets, error) {
	s := Secrets{values: map[string]string{}, Sources: map[string]string{}}
	if reader != nil {
		data, err := reader(ctx)
		switch {
		case err != nil && required:
			return s, fmt.Errorf("vault: %w", err)
		case err != nil:
			log.WarnContext(ctx, "vault unavailable; falling back to environment", "err", err)
		default:
			for _, k := range allKeys {
				if v, ok := data[k].(string); ok && v != "" {
					s.values[k], s.Sources[k] = v, "vault"
				}
			}
		}
	}
	for _, k := range allKeys {
		if _, ok := s.values[k]; ok {
			continue
		}
		if v := os.Getenv("LEDGERLINE_" + strings.ToUpper(k)); v != "" {
			s.values[k], s.Sources[k] = v, "env"
		}
	}
	return s, nil
}

func readVault(ctx context.Context, cfg config.VaultConfig) (map[string]any, error) {
	vc := vault.DefaultConfig()
	vc.Address = cfg.Addr
	client, err := vault.NewClient(vc)
	if err != nil {
		return nil, err
	}
	// VAULT_TOKEN (read by DefaultConfig) is honoured for local development;
	// in-cluster we log in with the service account.
	if client.Token() == "" {
		auth, err := k8sauth.NewKubernetesAuth(cfg.Role,
			k8sauth.WithServiceAccountTokenPath(cfg.TokenPath), k8sauth.WithMountPath(cfg.AuthMount))
		if err != nil {
			return nil, err
		}
		info, err := client.Auth().Login(ctx, auth)
		if err != nil {
			return nil, fmt.Errorf("kubernetes auth login: %w", err)
		}
		if info == nil {
			return nil, fmt.Errorf("kubernetes auth login returned no auth info")
		}
	}
	secret, err := client.KVv2(cfg.KVMount).Get(ctx, cfg.KVPath)
	if err != nil {
		return nil, fmt.Errorf("read %s/%s: %w", cfg.KVMount, cfg.KVPath, err)
	}
	return secret.Data, nil
}
