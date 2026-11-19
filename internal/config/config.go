// Package config loads service configuration from the environment
// (12-factor). Secrets are not read here; see package secrets.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full non-secret configuration.
type Config struct {
	HTTPAddr        string
	AdminAddr       string
	Store           string // postgres | memory
	MigrateOnStart  bool
	Provider        string // fake | anthropic
	AnthropicModel  string
	AnthropicEffort string
	JWTIssuer       string
	JWTAudience     string

	MaxDocumentBytes int64
	RateLimitRPS     float64
	RateLimitBurst   int
	CORSOrigins      []string
	PublicDemo       bool

	WorkerConcurrency  int
	JobTimeout         time.Duration
	MaxAttempts        int
	BreakerThreshold   int
	BreakerOpenTimeout time.Duration
	FakeLatency        time.Duration
	FakeFailureRate    float64

	WebhookAllowPrivate bool

	ShutdownTimeout time.Duration
	ReadinessDrain  time.Duration
	LogLevel        string
	LogFormat       string

	Vault VaultConfig
}

// VaultConfig configures the optional HashiCorp Vault secret source.
type VaultConfig struct {
	Addr      string // empty disables Vault
	Role      string // Kubernetes auth role
	AuthMount string // Kubernetes auth mount path
	KVMount   string // KV v2 mount
	KVPath    string // secret path within the mount
	TokenPath string // projected service account token
	Required  bool   // fail startup instead of falling back to env
}

type env struct{ errs []error }

func (e *env) str(key, def string) string {
	if v, ok := os.LookupEnv("LEDGERLINE_" + key); ok && v != "" {
		return v
	}
	return def
}

func (e *env) int(key string, def int) int {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("LEDGERLINE_%s: %w", key, err))
	}
	return n
}

func (e *env) float(key string, def float64) float64 {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("LEDGERLINE_%s: %w", key, err))
	}
	return f
}

func (e *env) bool(key string, def bool) bool {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("LEDGERLINE_%s: %w", key, err))
	}
	return b
}

func (e *env) dur(key string, def time.Duration) time.Duration {
	v := e.str(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("LEDGERLINE_%s: %w", key, err))
	}
	return d
}

// Load reads configuration from LEDGERLINE_* environment variables.
func Load() (Config, error) {
	e := &env{}
	storeDefault := "memory"
	if os.Getenv("LEDGERLINE_DATABASE_URL") != "" || os.Getenv("LEDGERLINE_VAULT_ADDR") != "" {
		storeDefault = "postgres"
	}
	c := Config{
		HTTPAddr:            e.str("HTTP_ADDR", ":8080"),
		AdminAddr:           e.str("ADMIN_ADDR", ":9090"),
		Store:               e.str("STORE", storeDefault),
		MigrateOnStart:      e.bool("MIGRATE_ON_START", true),
		Provider:            e.str("PROVIDER", "fake"),
		AnthropicModel:      e.str("ANTHROPIC_MODEL", "claude-opus-5-5"),
		AnthropicEffort:     e.str("ANTHROPIC_EFFORT", ""),
		JWTIssuer:           e.str("JWT_ISSUER", "ledgerline"),
		JWTAudience:         e.str("JWT_AUDIENCE", "ledgerline-api"),
		MaxDocumentBytes:    int64(e.int("MAX_DOCUMENT_BYTES", 5<<20)),
		RateLimitRPS:        e.float("RATE_LIMIT_RPS", 5),
		RateLimitBurst:      e.int("RATE_LIMIT_BURST", 10),
		PublicDemo:          e.bool("PUBLIC_DEMO", false),
		WorkerConcurrency:   e.int("WORKER_CONCURRENCY", 4),
		JobTimeout:          e.dur("JOB_TIMEOUT", 2*time.Minute),
		MaxAttempts:         e.int("MAX_ATTEMPTS", 5),
		BreakerThreshold:    e.int("BREAKER_THRESHOLD", 5),
		BreakerOpenTimeout:  e.dur("BREAKER_OPEN_TIMEOUT", 30*time.Second),
		FakeLatency:         e.dur("FAKE_LATENCY", 300*time.Millisecond),
		FakeFailureRate:     e.float("FAKE_FAILURE_RATE", 0),
		WebhookAllowPrivate: e.bool("WEBHOOK_ALLOW_PRIVATE", false),
		ShutdownTimeout:     e.dur("SHUTDOWN_TIMEOUT", 25*time.Second),
		ReadinessDrain:      e.dur("READINESS_DRAIN", 5*time.Second),
		LogLevel:            e.str("LOG_LEVEL", "info"),
		LogFormat:           e.str("LOG_FORMAT", "json"),
		Vault: VaultConfig{
			Addr:      e.str("VAULT_ADDR", ""),
			Role:      e.str("VAULT_ROLE", "ledgerline"),
			AuthMount: e.str("VAULT_AUTH_MOUNT", "kubernetes"),
			KVMount:   e.str("VAULT_KV_MOUNT", "secret"),
			KVPath:    e.str("VAULT_KV_PATH", "ledgerline"),
			TokenPath: e.str("VAULT_TOKEN_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/token"),
			Required:  e.bool("VAULT_REQUIRED", false),
		},
	}
	if o := e.str("CORS_ORIGINS", ""); o != "" {
		for _, s := range strings.Split(o, ",") {
			if s = strings.TrimSpace(s); s != "" {
				c.CORSOrigins = append(c.CORSOrigins, s)
			}
		}
	}
	if c.Store != "postgres" && c.Store != "memory" {
		e.errs = append(e.errs, fmt.Errorf("LEDGERLINE_STORE must be postgres or memory, got %q", c.Store))
	}
	if c.Provider != "fake" && c.Provider != "anthropic" {
		e.errs = append(e.errs, fmt.Errorf("LEDGERLINE_PROVIDER must be fake or anthropic, got %q", c.Provider))
	}
	if c.FakeFailureRate < 0 || c.FakeFailureRate > 1 {
		e.errs = append(e.errs, errors.New("LEDGERLINE_FAKE_FAILURE_RATE must be within [0,1]"))
	}
	return c, errors.Join(e.errs...)
}
