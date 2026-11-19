package secrets

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

func TestVaultWinsAndEnvFillsGaps(t *testing.T) {
	t.Setenv("LEDGERLINE_JWT_SECRET", "from-env")
	t.Setenv("LEDGERLINE_WEBHOOK_SECRET", "env-webhook")
	reader := func(context.Context) (map[string]any, error) {
		return map[string]any{"jwt_secret": "from-vault", "anthropic_api_key": "sk-vault"}, nil
	}
	s, err := resolve(context.Background(), reader, false, quiet)
	if err != nil {
		t.Fatal(err)
	}
	if s.Get(JWTSecret) != "from-vault" || s.Sources[JWTSecret] != "vault" {
		t.Errorf("jwt_secret: %q from %q", s.Get(JWTSecret), s.Sources[JWTSecret])
	}
	if s.Get(WebhookSecret) != "env-webhook" || s.Sources[WebhookSecret] != "env" {
		t.Errorf("webhook_secret: %q from %q", s.Get(WebhookSecret), s.Sources[WebhookSecret])
	}
	if s.Get(AnthropicAPIKey) != "sk-vault" {
		t.Error("anthropic key not read from vault")
	}
}

func TestVaultDownFallsBackUnlessRequired(t *testing.T) {
	t.Setenv("LEDGERLINE_JWT_SECRET", "from-env")
	down := func(context.Context) (map[string]any, error) { return nil, errors.New("connection refused") }
	s, err := resolve(context.Background(), down, false, quiet)
	if err != nil || s.Get(JWTSecret) != "from-env" {
		t.Fatalf("fallback failed: %v", err)
	}
	if _, err := resolve(context.Background(), down, true, quiet); err == nil {
		t.Fatal("required vault must fail startup")
	}
}
