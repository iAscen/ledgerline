package extract

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/iAscen/ledgerline/internal/domain"
)

// Magic markers let a caller force failure modes end to end (curl, CI e2e,
// the live demo) without restarting the service.
//
// A document carrying MarkerTransientFailure fails like an outage once, which
// is enough to trip the circuit breaker. Every later attempt on the same
// document fails permanently instead. Without that, the test jobs would keep
// retrying, win the breaker's half-open probes, and hold the breaker open for
// minutes, delaying real jobs submitted by other demo visitors.
var (
	MarkerTransientFailure = []byte("LEDGERLINE_FAIL")
	MarkerPermanentFailure = []byte("LEDGERLINE_REJECT")
)

// ErrFakeUnavailable simulates a provider outage (HTTP 5xx / 529 overloaded).
var ErrFakeUnavailable = errors.New("fake provider: upstream unavailable")

// Fake is a deterministic provider for tests and the public demo. The same
// document always yields the same receipt.
type Fake struct {
	Latency     time.Duration
	FailureRate float64 // 0..1, probability of a transient failure

	mu      sync.Mutex
	rng     *rand.Rand
	outages map[[32]byte]struct{} // marker documents that already failed once
}

// NewFake returns a fake provider.
func NewFake(latency time.Duration, failureRate float64) *Fake {
	return &Fake{
		Latency:     latency,
		FailureRate: failureRate,
		rng:         rand.New(rand.NewPCG(1, 2)), //nolint:gosec // simulation only, not security-sensitive
		outages:     map[[32]byte]struct{}{},
	}
}

// Name implements Provider.
func (f *Fake) Name() string { return "fake" }

// Extract implements Provider.
func (f *Fake) Extract(ctx context.Context, doc Document) (domain.Receipt, error) {
	if f.Latency > 0 {
		t := time.NewTimer(f.Latency)
		select {
		case <-ctx.Done():
			t.Stop()
			return domain.Receipt{}, ctx.Err()
		case <-t.C:
		}
	}
	if bytes.Contains(doc.Data, MarkerPermanentFailure) {
		return domain.Receipt{}, Permanent("document is not a receipt")
	}
	sum := sha256.Sum256(doc.Data)
	if bytes.Contains(doc.Data, MarkerTransientFailure) {
		if f.firstOutage(sum) {
			return domain.Receipt{}, ErrFakeUnavailable
		}
		return domain.Receipt{}, Permanent("simulated outage test document discarded after its first failure")
	}
	if f.roll() {
		return domain.Receipt{}, ErrFakeUnavailable
	}

	seed := binary.BigEndian.Uint64(sum[:8])
	merchants := []string{"Café Olimpico", "Marché Jean-Talon", "Librairie Drawn & Quarterly", "Dépanneur Le Pick-up", "Fairmount Bagel"}
	categories := []string{"meals", "groceries", "office_supplies", "meals", "meals"}
	i := seed % uint64(len(merchants))
	subtotal := int64(500 + seed%9500) // $5.00 .. $99.99
	gst := (subtotal*5 + 50) / 100     // GST/TPS 5%
	qst := (subtotal*9975 + 50000) / 100000
	return domain.Receipt{
		Merchant:      merchants[i],
		Date:          time.Date(2026, time.Month(1+seed%12), int(1+seed%28), 0, 0, 0, 0, time.UTC).Format("2006-01-02"),
		Currency:      "CAD",
		SubtotalMinor: subtotal,
		Taxes:         []domain.Tax{{Name: "GST", AmountMinor: gst}, {Name: "QST", AmountMinor: qst}},
		TotalMinor:    subtotal + gst + qst,
		PaymentMethod: "card",
		Category:      categories[i],
		LineItems:     []domain.LineItem{{Description: "Items", Quantity: 1, AmountMinor: subtotal}},
		Confidence:    0.99,
		Provider:      f.Name(),
	}, nil
}

// firstOutage records sum and reports whether this is its first failure.
func (f *Fake) firstOutage(sum [32]byte) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, seen := f.outages[sum]; seen {
		return false
	}
	if len(f.outages) >= 10000 { // bound memory on a long-running public demo
		clear(f.outages)
	}
	f.outages[sum] = struct{}{}
	return true
}

func (f *Fake) roll() bool {
	if f.FailureRate <= 0 {
		return false
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.rng.Float64() < f.FailureRate
}
