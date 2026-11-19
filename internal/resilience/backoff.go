package resilience

import (
	"context"
	"math/rand/v2"
	"time"
)

// Backoff computes exponential delays with full jitter
// (https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/).
type Backoff struct {
	Base time.Duration
	Max  time.Duration
	// Jitter, when nil, defaults to full jitter in [0, d]. Tests can pin it.
	Jitter func(d time.Duration) time.Duration
}

// Delay returns the wait before retry number attempt (1-based).
func (b Backoff) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := b.Base
	for i := 1; i < attempt && d < b.Max; i++ {
		d *= 2
	}
	if d > b.Max {
		d = b.Max
	}
	if b.Jitter != nil {
		return b.Jitter(d)
	}
	if d <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(d) + 1)) //nolint:gosec // jitter does not need a CSPRNG
}

// Retry calls fn up to attempts times, sleeping Delay between tries, until fn
// succeeds, retryable reports false, or ctx is done.
func Retry(ctx context.Context, attempts int, b Backoff, retryable func(error) bool, fn func(context.Context) error) error {
	var err error
	for i := 1; i <= attempts; i++ {
		if err = fn(ctx); err == nil || !retryable(err) || i == attempts {
			return err
		}
		t := time.NewTimer(b.Delay(i))
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	return err
}
