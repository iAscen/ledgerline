package livestats

import (
	"strings"
	"sync"
	"testing"
	"time"
)

type clock struct{ t time.Time }

func (c *clock) now() time.Time       { return c.t }
func (c *clock) add(d time.Duration)  { c.t = c.t.Add(d) }
func newWithClock(c *clock) *Recorder { r := New(); r.now = c.now; return r }
func deref(t *testing.T, p *float64) float64 {
	t.Helper()
	if p == nil {
		t.Fatal("expected a value, got nil")
	}
	return *p
}

func TestEmptySnapshotHasNoFakeNumbers(t *testing.T) {
	s := New().Snapshot()
	if s.RequestsPerMin != 0 || s.RequestsWindow != 0 {
		t.Fatalf("counts = %d/%d, want 0", s.RequestsPerMin, s.RequestsWindow)
	}
	if s.P50Ms != nil || s.ErrorRate != nil {
		t.Fatal("p50 and error rate must be nil (unknown) with no traffic, not zero")
	}
	if s.Events == nil {
		t.Fatal("events must be an empty list, not null")
	}
}

func TestNilRecorderIsSafe(t *testing.T) {
	var r *Recorder
	r.RecordRequest("GET /", 200, time.Millisecond)
	r.RecordJob("abc", "succeeded", 1)
	if s := r.Snapshot(); s.RequestsWindow != 0 {
		t.Fatal("nil recorder must report nothing")
	}
}

func TestP50RateAndErrorRate(t *testing.T) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	r := newWithClock(c)
	for _, ms := range []int{10, 30, 20, 50, 40} {
		r.RecordRequest("POST /v1/jobs", 202, time.Duration(ms)*time.Millisecond)
	}
	r.RecordRequest("GET /v1/jobs/{id}", 500, 5*time.Millisecond)

	s := r.Snapshot()
	if s.RequestsPerMin != 6 || s.RequestsWindow != 6 {
		t.Fatalf("counts = %d/%d, want 6/6", s.RequestsPerMin, s.RequestsWindow)
	}
	// sorted: 5,10,20,30,40,50 -> lower median is 20ms
	if got := deref(t, s.P50Ms); got != 20 {
		t.Fatalf("p50 = %v, want 20", got)
	}
	if got := deref(t, s.ErrorRate); got < 0.166 || got > 0.167 {
		t.Fatalf("error rate = %v, want 1/6", got)
	}
}

func TestSlidingWindows(t *testing.T) {
	c := &clock{t: time.Unix(1_700_000_000, 0)}
	r := newWithClock(c)
	r.RecordRequest("GET /", 200, time.Millisecond)
	c.add(2 * time.Minute) // outside the 1-minute rate, inside the 5-minute window
	r.RecordRequest("GET /", 200, time.Millisecond)

	s := r.Snapshot()
	if s.RequestsPerMin != 1 || s.RequestsWindow != 2 {
		t.Fatalf("counts = %d/%d, want 1/2", s.RequestsPerMin, s.RequestsWindow)
	}

	c.add(6 * time.Minute) // everything expired
	s = r.Snapshot()
	if s.RequestsWindow != 0 || s.P50Ms != nil {
		t.Fatal("expired samples must not count")
	}
	if len(s.Events) != 2 {
		t.Fatalf("events are a history and do not expire: got %d, want 2", len(s.Events))
	}
}

func TestEventsNewestFirstAndCapped(t *testing.T) {
	r := New()
	for i := range maxEvents + 3 {
		r.RecordJob(string(rune('a'+i)), "succeeded", 1)
	}
	s := r.Snapshot()
	if len(s.Events) != maxEvents {
		t.Fatalf("events = %d, want %d", len(s.Events), maxEvents)
	}
	if !strings.HasPrefix(s.Events[0].Text, "job k ") {
		t.Fatalf("newest first: got %q", s.Events[0].Text)
	}
}

func TestEventTextUsesRoutePatternOnly(t *testing.T) {
	r := New()
	r.RecordRequest("GET /v1/jobs/{id}", 200, 1500*time.Microsecond)
	got := r.Snapshot().Events[0].Text
	if got != "GET /v1/jobs/{id} 200 1ms" {
		t.Fatalf("event text = %q", got)
	}
	r.RecordRequest("GET /v1/jobs/{id}", 404, 300*time.Microsecond)
	if got := r.Snapshot().Events[0].Text; got != "GET /v1/jobs/{id} 404 <1ms" {
		t.Fatalf("sub-millisecond text = %q", got)
	}
}

func TestSampleRingIsBounded(t *testing.T) {
	r := New()
	for range maxSamples + 100 {
		r.RecordRequest("GET /", 200, time.Millisecond)
	}
	if s := r.Snapshot(); s.RequestsWindow != maxSamples {
		t.Fatalf("window = %d, want capped at %d", s.RequestsWindow, maxSamples)
	}
}

func TestConcurrentUse(t *testing.T) {
	r := New()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 200 {
				r.RecordRequest("GET /", 200, time.Millisecond)
				r.RecordJob("x", "succeeded", 1)
				_ = r.Snapshot()
			}
		}()
	}
	wg.Wait()
}
