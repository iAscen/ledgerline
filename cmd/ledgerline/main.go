// Command ledgerline runs the receipt-extraction service.
//
//	ledgerline [serve]                       run the API, worker pool and MCP endpoint
//	ledgerline migrate                       apply database migrations and exit
//	ledgerline token -tenant acme [-ttl 1h]  mint a JWT signed with the configured secret
//	ledgerline healthcheck [-url ...]        probe /healthz (for distroless HEALTHCHECK)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/iAscen/ledgerline/internal/app"
	"github.com/iAscen/ledgerline/internal/auth"
	"github.com/iAscen/ledgerline/internal/config"
	"github.com/iAscen/ledgerline/internal/secrets"
	"github.com/iAscen/ledgerline/internal/store"
	"github.com/iAscen/ledgerline/internal/telemetry"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	cmd := "serve"
	args := os.Args[1:]
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "serve":
		err = serve()
	case "migrate":
		err = migrate()
	case "token":
		err = token(args)
	case "healthcheck":
		err = healthcheck(args)
	case "version":
		fmt.Println(version)
	default:
		err = fmt.Errorf("unknown command %q (want serve, migrate, token, healthcheck, version)", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "ledgerline:", err)
		os.Exit(1)
	}
}

func newLogger(cfg config.Config) *slog.Logger {
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.LogLevel))
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler = slog.NewJSONHandler(os.Stdout, opts)
	if cfg.LogFormat == "text" {
		h = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(telemetry.TraceHandler{Handler: h}).With("service", "ledgerline", "version", version)
}

func load(ctx context.Context) (config.Config, secrets.Secrets, *slog.Logger, error) {
	cfg, err := config.Load()
	if err != nil {
		return cfg, secrets.Secrets{}, nil, err
	}
	log := newLogger(cfg)
	slog.SetDefault(log)
	sec, err := secrets.Load(ctx, cfg.Vault, log)
	if err != nil {
		return cfg, sec, log, err
	}
	log.Info("secrets resolved", "sources", sec.Sources) // key -> vault|env, never values
	return cfg, sec, log, nil
}

func serve() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, sec, log, err := load(ctx)
	if err != nil {
		return err
	}
	shutdownTracing, err := telemetry.SetupTracing(ctx, "ledgerline", version)
	if err != nil {
		return fmt.Errorf("tracing: %w", err)
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(c)
	}()

	a, err := app.New(ctx, cfg, sec, version, log)
	if err != nil {
		return err
	}
	defer a.Close()
	return a.Run(ctx)
}

func migrate() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, sec, log, err := load(ctx)
	if err != nil {
		return err
	}
	dsn := sec.Get(secrets.DatabaseURL)
	if dsn == "" {
		return errors.New("database_url is not configured")
	}
	pg, err := store.OpenPostgres(ctx, dsn)
	if err != nil {
		return err
	}
	defer pg.Close()
	if err := pg.Migrate(ctx); err != nil {
		return err
	}
	log.Info("migrations applied")
	return nil
}

func token(args []string) error {
	fs := flag.NewFlagSet("token", flag.ContinueOnError)
	tenant := fs.String("tenant", "", "tenant (JWT subject)")
	ttl := fs.Duration("ttl", time.Hour, "token lifetime")
	scope := fs.String("scope", auth.ScopeJobsRead+" "+auth.ScopeJobsWrite, "space-separated scopes")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *tenant == "" {
		return errors.New("-tenant is required")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	sec, err := secrets.Load(context.Background(), cfg.Vault, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err != nil {
		return err
	}
	a, err := auth.New([]byte(sec.Get(secrets.JWTSecret)), cfg.JWTIssuer, cfg.JWTAudience)
	if err != nil {
		return err
	}
	tok, err := a.Issue(*tenant, strings.Fields(*scope), *ttl)
	if err != nil {
		return err
	}
	fmt.Println(tok)
	return nil
}

func healthcheck(args []string) error {
	fs := flag.NewFlagSet("healthcheck", flag.ContinueOnError)
	url := fs.String("url", "http://127.0.0.1:8080/healthz", "URL to probe")
	if err := fs.Parse(args); err != nil {
		return err
	}
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get(*url) //nolint:noctx // one-shot CLI probe
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unhealthy: HTTP %d", resp.StatusCode)
	}
	return nil
}
