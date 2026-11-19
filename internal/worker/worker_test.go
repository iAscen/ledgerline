package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/iAscen/ledgerline/internal/domain"
	"github.com/iAscen/ledgerline/internal/extract"
	"github.com/iAscen/ledgerline/internal/resilience"
	"github.com/iAscen/ledgerline/internal/store"
	"github.com/iAscen/ledgerline/internal/telemetry"
)

type recordingNotifier struct {
	mu     sync.Mutex
	events []WebhookPayload
}

func (r *recordingNotifier) Send(_ context.Context, _, _, _ string, body []byte) error {
	var p WebhookPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return err
	}
	r.mu.Lock()
	r.events = append(r.events, p)
	r.mu.Unlock()
	return nil
}

type harness struct {
	store    *store.Memory
	pool     *Pool
	breaker  *resilience.Breaker
	notifier *recordingNotifier
	metrics  *telemetry.Metrics
}

func newHarness(t *testing.T, threshold int) *harness {
	t.Helper()
	m := telemetry.NewMetrics()
	b := resilience.NewBreaker(resilience.BreakerConfig{FailureThreshold: threshold, OpenTimeout: time.Hour, IsFailure: IsBreakerFailure})
	s := store.NewMemory()
	n := &recordingNotifier{}
	p := New(Config{MaxAttempts: 3, Backoff: resilience.Backoff{Base: time.Nanosecond, Max: time.Nanosecond, Jitter: func(time.Duration) time.Duration { return 0 }}},
		s, extract.NewFake(0, 0), b, n, m, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return &harness{store: s, pool: p, breaker: b, notifier: n, metrics: m}
}

func (h *harness) submit(t *testing.T, doc string) uuid.UUID {
	t.Helper()
	j, _, err := h.store.CreateJob(context.Background(), domain.NewJob{
		ID: uuid.New(), Tenant: "t", IdempotencyKey: uuid.NewString(), RequestHash: "h",
		MediaType: "image/png", Document: []byte(doc), WebhookURL: "https://hooks.example.com/x",
	})
	if err != nil {
		t.Fatal(err)
	}
	return j.ID
}

// step claims and processes one job; it reports false if none was runnable.
func (h *harness) step(t *testing.T) bool {
	t.Helper()
	j, err := h.store.ClaimNext(context.Background(), time.Minute)
	if err != nil {
		return false
	}
	h.pool.Process(context.Background(), j)
	return true
}

func (h *harness) job(t *testing.T, id uuid.UUID) domain.Job {
	t.Helper()
	j, err := h.store.GetJob(context.Background(), "t", id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestPipelineSuccessDeliversWebhook(t *testing.T) {
	h := newHarness(t, 3)
	id := h.submit(t, "a normal receipt")
	if !h.step(t) {
		t.Fatal("no job claimed")
	}
	j := h.job(t, id)
	if j.Status != domain.StatusSucceeded || j.Result == nil || j.Result.Provider != "fake" {
		t.Fatalf("unexpected job: %+v", j)
	}
	if j.WebhookStatus != "delivered" || len(h.notifier.events) != 1 || h.notifier.events[0].Event != "job.succeeded" {
		t.Fatalf("webhook not delivered: status=%q events=%v", j.WebhookStatus, h.notifier.events)
	}
}

func TestTransientFailureRetriesThenGivesUp(t *testing.T) {
	h := newHarness(t, 100)                 // keep the breaker out of this test
	h.pool.provider = extract.NewFake(0, 1) // every call fails transiently
	id := h.submit(t, "a receipt")
	for i := 1; i <= 2; i++ {
		h.step(t)
		j := h.job(t, id)
		if j.Status != domain.StatusQueued || j.Attempts != i {
			t.Fatalf("after attempt %d: status=%s attempts=%d", i, j.Status, j.Attempts)
		}
	}
	h.step(t)
	j := h.job(t, id)
	if j.Status != domain.StatusFailed || j.Attempts != 3 {
		t.Fatalf("want failed after 3 attempts, got %s/%d (%s)", j.Status, j.Attempts, j.Error)
	}
	if h.notifier.events[0].Event != "job.failed" {
		t.Fatal("failure webhook not sent")
	}
}

func TestForcedProviderFailuresOpenTheBreaker(t *testing.T) {
	h := newHarness(t, 3)
	// Push retries far out so each job is processed exactly once here.
	h.pool.cfg.Backoff = resilience.Backoff{Base: time.Hour, Max: time.Hour, Jitter: func(d time.Duration) time.Duration { return d }}
	var ids []uuid.UUID
	for i := 0; i < 5; i++ {
		ids = append(ids, h.submit(t, fmt.Sprintf("LEDGERLINE_FAIL %d", i)))
	}
	for i := 0; i < 5; i++ {
		h.step(t)
	}
	if h.breaker.State() != resilience.StateOpen {
		t.Fatalf("breaker should be open after 3 consecutive provider failures, got %s", h.breaker.State())
	}
	// Jobs that hit the open breaker were rescheduled with their attempt refunded.
	shortCircuited := 0
	for _, id := range ids {
		j := h.job(t, id)
		if j.Error == "provider circuit open" {
			shortCircuited++
			if j.Attempts != 0 {
				t.Errorf("short-circuited job must not burn an attempt, attempts=%d", j.Attempts)
			}
		}
	}
	if shortCircuited != 2 {
		t.Fatalf("want 2 jobs short-circuited by the open breaker, got %d", shortCircuited)
	}
}

func TestPermanentErrorsFailFastAndDoNotTripBreaker(t *testing.T) {
	h := newHarness(t, 1)
	id := h.submit(t, "LEDGERLINE_REJECT")
	h.step(t)
	if j := h.job(t, id); j.Status != domain.StatusFailed || j.Attempts != 1 {
		t.Fatalf("want failed on first attempt, got %s/%d", j.Status, j.Attempts)
	}
	if h.breaker.State() != resilience.StateClosed {
		t.Fatal("a bad document must not open the breaker")
	}
}

func TestRunDrainsQueueAndStopsOnCancel(t *testing.T) {
	h := newHarness(t, 3)
	ids := []uuid.UUID{h.submit(t, "one"), h.submit(t, "two"), h.submit(t, "three")}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.pool.Run(ctx); close(done) }()
	h.pool.Notify()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		all := true
		for _, id := range ids {
			if h.job(t, id).Status != domain.StatusSucceeded {
				all = false
			}
		}
		if all {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	for _, id := range ids {
		if s := h.job(t, id).Status; s != domain.StatusSucceeded {
			t.Fatalf("job %s status %s", id, s)
		}
	}
}

// An outage test must not hold the breaker open: once it has tripped, the
// first half-open probe gets a permanent answer for the test document, which
// closes the breaker so other jobs flow again.
func TestOutageTestJobsLetTheBreakerRecover(t *testing.T) {
	h := newHarness(t, 1)
	h.breaker = resilience.NewBreaker(resilience.BreakerConfig{FailureThreshold: 1, OpenTimeout: 10 * time.Millisecond, IsFailure: IsBreakerFailure})
	h.pool.breaker = h.breaker
	outage := h.submit(t, "LEDGERLINE_FAIL demo")
	h.step(t)
	if h.breaker.State() != resilience.StateOpen {
		t.Fatalf("breaker should open, got %s", h.breaker.State())
	}
	time.Sleep(20 * time.Millisecond) // past the open timeout: next call is the probe
	h.step(t)
	if s := h.breaker.State(); s != resilience.StateClosed {
		t.Fatalf("probe on the outage document should close the breaker, got %s", s)
	}
	if j := h.job(t, outage); j.Status != domain.StatusFailed {
		t.Fatalf("outage test job should end failed, got %s", j.Status)
	}
	normal := h.submit(t, "a visitor's receipt")
	h.step(t)
	if j := h.job(t, normal); j.Status != domain.StatusSucceeded {
		t.Fatalf("visitor job should succeed after recovery, got %s (%s)", j.Status, j.Error)
	}
}
