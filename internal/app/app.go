// Package app is the composition root: it wires configuration, secrets,
// storage, the provider, the worker pool and the HTTP servers, and owns the
// graceful-shutdown sequence.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/iAscen/ledgerline/internal/auth"
	"github.com/iAscen/ledgerline/internal/config"
	"github.com/iAscen/ledgerline/internal/extract"
	"github.com/iAscen/ledgerline/internal/httpapi"
	"github.com/iAscen/ledgerline/internal/jobs"
	"github.com/iAscen/ledgerline/internal/mcpserver"
	"github.com/iAscen/ledgerline/internal/ratelimit"
	"github.com/iAscen/ledgerline/internal/resilience"
	"github.com/iAscen/ledgerline/internal/secrets"
	"github.com/iAscen/ledgerline/internal/store"
	"github.com/iAscen/ledgerline/internal/telemetry"
	"github.com/iAscen/ledgerline/internal/webhook"
	"github.com/iAscen/ledgerline/internal/worker"
)

// App is a fully wired service instance.
type App struct {
	Config  config.Config
	Store   store.Store
	Auth    *auth.Authenticator
	Breaker *resilience.Breaker
	Metrics *telemetry.Metrics
	Pool    *worker.Pool
	API     *httpapi.Server
	log     *slog.Logger
}

// New wires the service. The caller owns Close.
func New(ctx context.Context, cfg config.Config, sec secrets.Secrets, version string, log *slog.Logger) (*App, error) {
	a := &App{Config: cfg, Metrics: telemetry.NewMetrics(), log: log}

	jwtSecret := sec.Get(secrets.JWTSecret)
	if jwtSecret == "" {
		return nil, errors.New("jwt_secret is not configured (Vault or LEDGERLINE_JWT_SECRET)")
	}
	var err error
	if a.Auth, err = auth.New([]byte(jwtSecret), cfg.JWTIssuer, cfg.JWTAudience); err != nil {
		return nil, err
	}

	switch cfg.Store {
	case "postgres":
		dsn := sec.Get(secrets.DatabaseURL)
		if dsn == "" {
			return nil, errors.New("database_url is not configured (Vault or LEDGERLINE_DATABASE_URL)")
		}
		pg, err := store.OpenPostgres(ctx, dsn)
		if err != nil {
			return nil, err
		}
		if cfg.MigrateOnStart {
			if err := pg.Migrate(ctx); err != nil {
				pg.Close()
				return nil, err
			}
		}
		a.Store = pg
	default:
		log.WarnContext(ctx, "using in-memory store: data is lost on restart and not shared between replicas")
		a.Store = store.NewMemory()
	}

	var provider extract.Provider
	switch cfg.Provider {
	case "anthropic":
		key := sec.Get(secrets.AnthropicAPIKey)
		if key == "" {
			return nil, errors.New("anthropic_api_key is not configured (Vault or LEDGERLINE_ANTHROPIC_API_KEY)")
		}
		provider = extract.NewAnthropic(extract.AnthropicConfig{
			APIKey: key, Model: cfg.AnthropicModel, Effort: cfg.AnthropicEffort, Timeout: cfg.JobTimeout,
		})
	default:
		provider = extract.NewFake(cfg.FakeLatency, cfg.FakeFailureRate)
	}

	a.Breaker = resilience.NewBreaker(resilience.BreakerConfig{
		FailureThreshold: cfg.BreakerThreshold,
		OpenTimeout:      cfg.BreakerOpenTimeout,
		IsFailure:        worker.IsBreakerFailure,
		OnStateChange: func(from, to resilience.State) {
			a.Metrics.BreakerState.Set(float64(to))
			a.Metrics.BreakerTrips.WithLabelValues(to.String()).Inc()
			log.Warn("circuit breaker state change", "from", from.String(), "to", to.String())
		},
	})

	var notifier worker.Notifier
	if ws := sec.Get(secrets.WebhookSecret); ws != "" {
		notifier = webhook.NewSender(webhook.Config{Secret: []byte(ws), AllowPrivate: cfg.WebhookAllowPrivate})
	} else {
		log.WarnContext(ctx, "webhook_secret not configured: webhooks are disabled")
	}

	a.Pool = worker.New(worker.Config{
		Concurrency: cfg.WorkerConcurrency, JobTimeout: cfg.JobTimeout, MaxAttempts: cfg.MaxAttempts,
	}, a.Store, provider, a.Breaker, notifier, a.Metrics, log)

	svc := &jobs.Service{Store: a.Store, Metrics: a.Metrics, Waker: a.Pool,
		MaxDocumentBytes: cfg.MaxDocumentBytes, AllowHTTPWebhook: cfg.WebhookAllowPrivate}
	a.API = httpapi.New(httpapi.Options{
		Version: version, Provider: provider.Name(), StoreKind: cfg.Store, CORSOrigins: cfg.CORSOrigins,
		PublicDemo: cfg.PublicDemo, MaxBody: cfg.MaxDocumentBytes*4/3 + 64<<10,
	}, svc, a.Auth, ratelimit.New(cfg.RateLimitRPS, cfg.RateLimitBurst), a.Breaker, a.Store, a.Metrics, log,
		mcpserver.NewHandler(svc, version))
	return a, nil
}

// Close releases resources.
func (a *App) Close() { a.Store.Close() }

// Run serves until ctx is cancelled, then shuts down gracefully:
//  1. mark not-ready so the load balancer / kube-proxy stops routing to us,
//  2. wait ReadinessDrain for endpoint removal to propagate,
//  3. stop accepting connections and finish in-flight requests,
//  4. stop claiming jobs and let in-flight jobs finish,
//     all bounded by ShutdownTimeout (keep it below terminationGracePeriodSeconds).
func (a *App) Run(ctx context.Context) error {
	public := &http.Server{
		Addr: a.Config.HTTPAddr, Handler: a.API.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 90 * time.Second,
		IdleTimeout: 120 * time.Second, BaseContext: func(net.Listener) context.Context { return context.WithoutCancel(ctx) },
	}
	admin := &http.Server{Addr: a.Config.AdminAddr, Handler: a.API.AdminHandler(), ReadHeaderTimeout: 5 * time.Second}

	errc := make(chan error, 2)
	for _, srv := range []*http.Server{public, admin} {
		go func(s *http.Server) {
			if err := s.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errc <- fmt.Errorf("listen %s: %w", s.Addr, err)
			}
		}(srv)
	}

	workerCtx, stopWorkers := context.WithCancel(context.WithoutCancel(ctx))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); a.Pool.Run(workerCtx) }()

	a.API.SetReady(true)
	a.log.Info("ledgerline started", "http", a.Config.HTTPAddr, "admin", a.Config.AdminAddr,
		"store", a.Config.Store, "provider", a.Config.Provider)

	var runErr error
	select {
	case <-ctx.Done():
	case runErr = <-errc:
	}

	a.log.Info("shutting down")
	a.API.SetReady(false)
	deadline, cancel := context.WithTimeout(context.WithoutCancel(ctx), a.Config.ShutdownTimeout)
	defer cancel()
	if runErr == nil {
		select {
		case <-time.After(a.Config.ReadinessDrain):
		case <-deadline.Done():
		}
	}
	if err := public.Shutdown(deadline); err != nil {
		a.log.Error("http shutdown", "err", err)
	}
	stopWorkers()
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-deadline.Done():
		a.log.Warn("shutdown deadline reached with jobs in flight; their leases will expire and another replica will retry them")
	}
	_ = admin.Shutdown(deadline)
	a.log.Info("shutdown complete")
	return runErr
}
