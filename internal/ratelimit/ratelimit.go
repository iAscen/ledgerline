// Package ratelimit is a keyed token-bucket limiter (per tenant or per IP).
//
// It is per-process: with N replicas the effective limit is N times the
// configured rate. That is an accepted trade-off for this service (the limit
// protects the provider budget, and the HPA bounds N); a shared store such as
// Redis would be the next step if exact global limits were required.
package ratelimit

import (
	"sync"
	"time"

	"golang.org/x/time/rate"
)

type entry struct {
	lim  *rate.Limiter
	seen time.Time
}

// Limiter hands out one token bucket per key and evicts idle keys.
type Limiter struct {
	rps   rate.Limit
	burst int
	ttl   time.Duration

	mu      sync.Mutex
	buckets map[string]*entry
	lastGC  time.Time
	now     func() time.Time
}

// New returns a limiter allowing rps requests per second with burst.
func New(rps float64, burst int) *Limiter {
	return &Limiter{rps: rate.Limit(rps), burst: burst, ttl: 10 * time.Minute, buckets: map[string]*entry{}, now: time.Now}
}

// Allow consumes a token for key. When denied it returns how long until a
// token is available, for the Retry-After header.
func (l *Limiter) Allow(key string) (bool, time.Duration) {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	if now.Sub(l.lastGC) > time.Minute {
		for k, e := range l.buckets {
			if now.Sub(e.seen) > l.ttl {
				delete(l.buckets, k)
			}
		}
		l.lastGC = now
	}
	e, ok := l.buckets[key]
	if !ok {
		e = &entry{lim: rate.NewLimiter(l.rps, l.burst)}
		l.buckets[key] = e
	}
	e.seen = now
	r := e.lim.ReserveN(now, 1)
	if !r.OK() {
		return false, time.Second
	}
	if d := r.DelayFrom(now); d > 0 {
		r.CancelAt(now)
		return false, d
	}
	return true, 0
}

// Limit returns the configured burst (exposed in RateLimit-Limit headers).
func (l *Limiter) Limit() int { return l.burst }
