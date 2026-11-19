package resilience

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type clock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

var errBoom = errors.New("boom")

func TestBreakerOpensAfterThresholdAndRecovers(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	var transitions []string
	b := NewBreaker(BreakerConfig{
		FailureThreshold: 3, OpenTimeout: 10 * time.Second, Now: clk.Now,
		OnStateChange: func(from, to State) { transitions = append(transitions, from.String()+"->"+to.String()) },
	})
	fail := func(context.Context) error { return errBoom }
	ok := func(context.Context) error { return nil }
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := b.Execute(ctx, fail); !errors.Is(err, errBoom) {
			t.Fatalf("call %d: want errBoom, got %v", i, err)
		}
	}
	if b.State() != StateOpen {
		t.Fatalf("want open, got %s", b.State())
	}
	called := false
	if err := b.Execute(ctx, func(context.Context) error { called = true; return nil }); !errors.Is(err, ErrOpen) || called {
		t.Fatalf("open breaker must short-circuit: err=%v called=%v", err, called)
	}
	if ra := b.RetryAfter(); ra != 10*time.Second {
		t.Fatalf("RetryAfter = %s", ra)
	}

	clk.Advance(10 * time.Second)
	if b.State() != StateHalfOpen {
		t.Fatalf("want half-open after timeout, got %s", b.State())
	}
	// Failed probe re-opens.
	_ = b.Execute(ctx, fail)
	if b.State() != StateOpen {
		t.Fatalf("failed probe must re-open, got %s", b.State())
	}
	clk.Advance(10 * time.Second)
	if err := b.Execute(ctx, ok); err != nil {
		t.Fatal(err)
	}
	if b.State() != StateClosed {
		t.Fatalf("successful probe must close, got %s", b.State())
	}
	want := []string{"closed->open", "open->half_open", "half_open->open", "open->half_open", "half_open->closed"}
	if len(transitions) != len(want) {
		t.Fatalf("transitions = %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transitions = %v, want %v", transitions, want)
		}
	}
}

func TestBreakerIgnoresNonCountedErrorsAndResetsOnSuccess(t *testing.T) {
	permanent := errors.New("bad document")
	b := NewBreaker(BreakerConfig{FailureThreshold: 2, IsFailure: func(err error) bool { return !errors.Is(err, permanent) }})
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		_ = b.Execute(ctx, func(context.Context) error { return permanent })
	}
	if b.State() != StateClosed {
		t.Fatal("caller errors must not open the breaker")
	}
	_ = b.Execute(ctx, func(context.Context) error { return errBoom })
	_ = b.Execute(ctx, func(context.Context) error { return nil })
	_ = b.Execute(ctx, func(context.Context) error { return errBoom })
	if b.State() != StateClosed {
		t.Fatal("success must reset the consecutive failure count")
	}
}

func TestBreakerSingleProbeInHalfOpen(t *testing.T) {
	clk := &clock{t: time.Unix(0, 0)}
	b := NewBreaker(BreakerConfig{FailureThreshold: 1, OpenTimeout: time.Second, Now: clk.Now})
	ctx := context.Background()
	_ = b.Execute(ctx, func(context.Context) error { return errBoom })
	clk.Advance(time.Second)

	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_ = b.Execute(ctx, func(context.Context) error { close(started); <-release; return nil })
	}()
	<-started
	if err := b.Execute(ctx, func(context.Context) error { return nil }); !errors.Is(err, ErrOpen) {
		t.Fatalf("second concurrent probe must be rejected, got %v", err)
	}
	close(release)
}

func TestBackoffDelayDoublesAndCaps(t *testing.T) {
	b := Backoff{Base: time.Second, Max: 10 * time.Second, Jitter: func(d time.Duration) time.Duration { return d }}
	want := []time.Duration{1, 2, 4, 8, 10, 10}
	for i, w := range want {
		if got := b.Delay(i + 1); got != w*time.Second {
			t.Errorf("attempt %d: got %s want %s", i+1, got, w*time.Second)
		}
	}
	full := Backoff{Base: time.Second, Max: time.Minute}
	for i := 0; i < 100; i++ {
		if d := full.Delay(3); d < 0 || d > 4*time.Second {
			t.Fatalf("full jitter out of range: %s", d)
		}
	}
}

func TestRetryStopsOnNonRetryable(t *testing.T) {
	calls := 0
	b := Backoff{Jitter: func(time.Duration) time.Duration { return 0 }}
	err := Retry(context.Background(), 5, b, func(err error) bool { return !errors.Is(err, errBoom) }, func(context.Context) error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
