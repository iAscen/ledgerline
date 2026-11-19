// Package worker runs the asynchronous extraction pipeline:
// claim job -> extract (through the circuit breaker) -> store -> webhook.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/iAscen/ledgerline/internal/domain"
	"github.com/iAscen/ledgerline/internal/extract"
	"github.com/iAscen/ledgerline/internal/livestats"
	"github.com/iAscen/ledgerline/internal/resilience"
	"github.com/iAscen/ledgerline/internal/store"
	"github.com/iAscen/ledgerline/internal/telemetry"
	"github.com/iAscen/ledgerline/internal/webhook"
)

// Config tunes the pool.
type Config struct {
	Concurrency  int
	Lease        time.Duration // must exceed JobTimeout so a live job is never reclaimed
	JobTimeout   time.Duration
	PollInterval time.Duration
	MaxAttempts  int
	Backoff      resilience.Backoff
}

// Notifier delivers webhooks. *webhook.Sender implements it.
type Notifier interface {
	Send(ctx context.Context, target, event, deliveryID string, body []byte) error
}

// Pool is a fixed-size set of workers pulling from the Store.
type Pool struct {
	cfg      Config
	store    store.Store
	provider extract.Provider
	breaker  *resilience.Breaker
	notifier Notifier
	metrics  *telemetry.Metrics
	log      *slog.Logger
	wake     chan struct{}
	now      func() time.Time
	events   *livestats.Recorder
}

// New builds a Pool.
func New(cfg Config, s store.Store, p extract.Provider, b *resilience.Breaker, n Notifier, m *telemetry.Metrics, log *slog.Logger) *Pool {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.JobTimeout <= 0 {
		cfg.JobTimeout = 2 * time.Minute
	}
	if cfg.Lease <= cfg.JobTimeout {
		cfg.Lease = cfg.JobTimeout + 30*time.Second
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Backoff.Base == 0 {
		cfg.Backoff = resilience.Backoff{Base: 2 * time.Second, Max: 5 * time.Minute}
	}
	return &Pool{cfg: cfg, store: s, provider: p, breaker: b, notifier: n, metrics: m, log: log,
		wake: make(chan struct{}, 1), now: time.Now}
}

// Notify wakes an idle worker immediately (called after a job is created), so
// latency is not bounded by the poll interval. Polling remains the safety net
// for jobs created by other replicas and for retries coming due.
func (p *Pool) Notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Run blocks until ctx is cancelled and every in-flight job has finished.
// In-flight jobs are not cancelled by ctx: they run to completion (bounded by
// JobTimeout), which is what makes shutdown graceful.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			p.loop(ctx, id)
		}(i)
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context, id int) {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		job, err := p.store.ClaimNext(ctx, p.cfg.Lease)
		switch {
		case err == nil:
			p.Process(context.WithoutCancel(ctx), job)
			continue // drain the queue before sleeping
		case errors.Is(err, domain.ErrNoJobAvailable), errors.Is(err, context.Canceled):
		default:
			p.log.ErrorContext(ctx, "claim job", "worker", id, "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-p.wake:
		}
	}
}

// SetLiveStats makes the pool report job outcomes to the public event feed.
func (p *Pool) SetLiveStats(r *livestats.Recorder) { p.events = r }

// jobEvent publishes a fixed-vocabulary outcome; never error text or document data.
func (p *Pool) jobEvent(job domain.Job, outcome string, attempts int) {
	p.events.RecordJob(job.ID.String()[:8], outcome, attempts)
}

// Process runs one claimed job to a terminal or rescheduled state. Exported
// for tests that drive the pipeline deterministically.
func (p *Pool) Process(ctx context.Context, job domain.Job) {
	ctx, cancel := context.WithTimeout(ctx, p.cfg.JobTimeout)
	defer cancel()
	ctx = telemetry.ContextWithTraceParent(ctx, job.TraceParent)
	ctx, span := telemetry.Tracer().Start(ctx, "job.process", trace.WithAttributes(
		attribute.String("job.id", job.ID.String()), attribute.Int("job.attempt", job.Attempts),
		attribute.String("job.media_type", job.MediaType),
	))
	defer span.End()
	log := p.log.With("job_id", job.ID, "attempt", job.Attempts)

	var receipt domain.Receipt
	err := p.breaker.Execute(ctx, func(ctx context.Context) error {
		ctx, pspan := telemetry.Tracer().Start(ctx, "provider.extract", trace.WithAttributes(attribute.String("provider", p.provider.Name())))
		defer pspan.End()
		start := time.Now()
		var err error
		receipt, err = p.provider.Extract(ctx, extract.Document{MediaType: job.MediaType, Data: job.Document})
		p.metrics.ProviderTime.WithLabelValues(p.provider.Name()).Observe(time.Since(start).Seconds())
		p.metrics.ProviderCalls.WithLabelValues(p.provider.Name(), outcome(err)).Inc()
		if err != nil {
			pspan.RecordError(err)
			pspan.SetStatus(codes.Error, err.Error())
		}
		return err
	})

	// Persist the outcome even if the job deadline has passed.
	sctx, scancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer scancel()

	switch {
	case err == nil:
		if serr := p.store.Complete(sctx, job.ID, receipt); serr != nil {
			log.ErrorContext(ctx, "store result", "err", serr)
			return // lease expiry will retry the job
		}
		job.Status, job.Result, job.Error = domain.StatusSucceeded, &receipt, ""
		p.jobEvent(job, "succeeded", job.Attempts)
		p.finished(ctx, sctx, job, log)

	case errors.Is(err, resilience.ErrOpen):
		// The provider was never called; do not burn an attempt.
		at := p.now().Add(p.breaker.RetryAfter() + p.cfg.Backoff.Delay(1))
		p.metrics.JobRetries.WithLabelValues("circuit_open").Inc()
		span.AddEvent("circuit open; rescheduled")
		if serr := p.store.Reschedule(sctx, job.ID, "provider circuit open", at, true); serr != nil {
			log.ErrorContext(ctx, "reschedule", "err", serr)
		}
		p.jobEvent(job, "waiting: circuit open", 0)

	case extract.IsPermanent(err):
		span.SetStatus(codes.Error, err.Error())
		if serr := p.store.Fail(sctx, job.ID, err.Error()); serr != nil {
			log.ErrorContext(ctx, "store failure", "err", serr)
			return
		}
		job.Status, job.Error = domain.StatusFailed, err.Error()
		p.jobEvent(job, "rejected", job.Attempts)
		p.finished(ctx, sctx, job, log)

	case job.Attempts >= p.cfg.MaxAttempts:
		span.SetStatus(codes.Error, err.Error())
		reason := fmt.Sprintf("gave up after %d attempts: %v", job.Attempts, err)
		if serr := p.store.Fail(sctx, job.ID, reason); serr != nil {
			log.ErrorContext(ctx, "store failure", "err", serr)
			return
		}
		job.Status, job.Error = domain.StatusFailed, reason
		p.jobEvent(job, "failed", job.Attempts)
		p.finished(ctx, sctx, job, log)

	default:
		delay := p.cfg.Backoff.Delay(job.Attempts)
		p.metrics.JobRetries.WithLabelValues("provider_error").Inc()
		log.WarnContext(ctx, "extraction failed; retrying", "err", err, "retry_in", delay.String())
		if serr := p.store.Reschedule(sctx, job.ID, err.Error(), p.now().Add(delay), false); serr != nil {
			log.ErrorContext(ctx, "reschedule", "err", serr)
		}
		p.jobEvent(job, "retrying", job.Attempts)
	}
}

func outcome(err error) string {
	switch {
	case err == nil:
		return "success"
	case extract.IsPermanent(err):
		return "permanent_error"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "transient_error"
	}
}

// IsBreakerFailure is the breaker's failure predicate: only provider-side
// faults count, never a bad document or our own cancellation.
func IsBreakerFailure(err error) bool {
	return err != nil && !extract.IsPermanent(err) && !errors.Is(err, context.Canceled)
}

// WebhookPayload is the body delivered to webhook_url.
type WebhookPayload struct {
	Event string     `json:"event"`
	Job   domain.Job `json:"job"`
}

func (p *Pool) finished(ctx, sctx context.Context, job domain.Job, log *slog.Logger) {
	p.metrics.JobsFinished.WithLabelValues(string(job.Status)).Inc()
	p.metrics.JobLatency.Observe(p.now().Sub(job.CreatedAt).Seconds())
	log.InfoContext(ctx, "job finished", "status", job.Status)
	if job.WebhookURL == "" || p.notifier == nil {
		return
	}
	now := p.now().UTC()
	job.CompletedAt = &now
	job.Document = nil
	body, err := json.Marshal(WebhookPayload{Event: "job." + string(job.Status), Job: job})
	if err != nil {
		log.ErrorContext(ctx, "encode webhook", "err", err)
		return
	}
	ctx, span := telemetry.Tracer().Start(ctx, "webhook.deliver")
	defer span.End()
	status := "delivered"
	if err := p.notifier.Send(ctx, job.WebhookURL, "job."+string(job.Status), job.ID.String(), body); err != nil {
		status = "failed"
		span.RecordError(err)
		log.WarnContext(ctx, "webhook delivery failed", "err", err)
	}
	p.metrics.Webhooks.WithLabelValues(status).Inc()
	if err := p.store.SetWebhookStatus(sctx, job.ID, status); err != nil {
		log.ErrorContext(ctx, "store webhook status", "err", err)
	}
}

// Compile-time check that the real sender satisfies Notifier.
var _ Notifier = (*webhook.Sender)(nil)
