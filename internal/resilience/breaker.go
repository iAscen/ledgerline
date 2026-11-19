// Package resilience provides the reliability primitives that wrap calls to
// the extraction provider: a circuit breaker and exponential backoff.
package resilience

import (
	"context"
	"errors"
	"sync"
	"time"
)

// State is a circuit breaker state.
type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// ErrOpen is returned without calling the protected function while the
// breaker is open (or while the single half-open probe is in flight).
var ErrOpen = errors.New("circuit breaker is open")

// BreakerConfig tunes a Breaker.
type BreakerConfig struct {
	// FailureThreshold is the number of consecutive counted failures that trips
	// the breaker from closed to open.
	FailureThreshold int
	// OpenTimeout is how long the breaker stays open before allowing a probe.
	OpenTimeout time.Duration
	// IsFailure decides whether an error counts against the provider. Errors
	// caused by the caller (a malformed document) must not open the circuit.
	// Nil means every non-nil error counts.
	IsFailure func(error) bool
	// OnStateChange is called (outside the lock) on every transition.
	OnStateChange func(from, to State)
	// Now is injectable for tests.
	Now func() time.Time
}

// Breaker is a consecutive-failure circuit breaker with a single-probe
// half-open state. It is safe for concurrent use.
type Breaker struct {
	cfg BreakerConfig

	mu       sync.Mutex
	state    State
	failures int
	openedAt time.Time
	probing  bool
}

// NewBreaker returns a closed breaker.
func NewBreaker(cfg BreakerConfig) *Breaker {
	if cfg.FailureThreshold <= 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.OpenTimeout <= 0 {
		cfg.OpenTimeout = 30 * time.Second
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Breaker{cfg: cfg}
}

// State returns the current state, accounting for an elapsed open timeout.
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == StateOpen && b.cfg.Now().Sub(b.openedAt) >= b.cfg.OpenTimeout {
		return StateHalfOpen
	}
	return b.state
}

// RetryAfter is how long until the breaker will admit a probe (0 if it would now).
func (b *Breaker) RetryAfter() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateOpen {
		return 0
	}
	if d := b.cfg.OpenTimeout - b.cfg.Now().Sub(b.openedAt); d > 0 {
		return d
	}
	return 0
}

// Execute runs fn if the breaker admits the call and records the outcome.
func (b *Breaker) Execute(ctx context.Context, fn func(context.Context) error) error {
	if err := b.before(); err != nil {
		return err
	}
	err := fn(ctx)
	b.after(err)
	return err
}

func (b *Breaker) before() error {
	b.mu.Lock()
	var transition func()
	defer func() {
		b.mu.Unlock()
		if transition != nil {
			transition()
		}
	}()
	switch b.state {
	case StateClosed:
		return nil
	case StateOpen:
		if b.cfg.Now().Sub(b.openedAt) < b.cfg.OpenTimeout {
			return ErrOpen
		}
		transition = b.setLocked(StateHalfOpen)
		b.probing = true
		return nil
	default: // half-open: only one probe at a time
		if b.probing {
			return ErrOpen
		}
		b.probing = true
		return nil
	}
}

func (b *Breaker) after(err error) {
	failed := err != nil && (b.cfg.IsFailure == nil || b.cfg.IsFailure(err))
	b.mu.Lock()
	var transition func()
	defer func() {
		b.mu.Unlock()
		if transition != nil {
			transition()
		}
	}()
	if b.state == StateHalfOpen {
		b.probing = false
		if failed {
			b.openedAt = b.cfg.Now()
			transition = b.setLocked(StateOpen)
		} else {
			b.failures = 0
			transition = b.setLocked(StateClosed)
		}
		return
	}
	if !failed {
		b.failures = 0
		return
	}
	b.failures++
	if b.state == StateClosed && b.failures >= b.cfg.FailureThreshold {
		b.openedAt = b.cfg.Now()
		transition = b.setLocked(StateOpen)
	}
}

// setLocked changes state and returns the callback to fire after unlocking.
func (b *Breaker) setLocked(to State) func() {
	from := b.state
	b.state = to
	if from == to || b.cfg.OnStateChange == nil {
		return nil
	}
	cb := b.cfg.OnStateChange
	return func() { cb(from, to) }
}
