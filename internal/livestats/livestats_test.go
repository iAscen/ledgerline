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

func TestTraceMergesAcceptAndWorkerPhases(t *testing.T) {
	r := New()
	t0 := time.Unix(1_700_000_000, 0)
	at := func(ms int) time.Time { return t0.Add(time.Duration(ms) * time.Millisecond) }
	r.RecordAccept("job-1", []Timing{
		{Name: "auth.verify_jwt", Start: at(0), End: at(1)},
		{Name: "api.decode", Start: at(1), End: at(3)},
		{Name: "store.insert", Start: at(3), End: at(5)},
	})
	r.RecordTrace("job-1", "job1", at(4), []Timing{
		{Name: "provider.extract", Start: at(20), End: at(320)},
		{Name: "store.complete", Start: at(320), End: at(322)},
	})
	tr := r.Snapshot().LastTrace
	if tr == nil {
		t.Fatal("expected a trace")
	}
	names := []string{}
	for _, s := range tr.Spans {
		names = append(names, s.Name)
	}
	want := "auth.verify_jwt api.decode store.insert queue.wait provider.extract store.complete"
	if got := strings.Join(names, " "); got != want {
		t.Fatalf("spans = %q, want %q", got, want)
	}
	if tr.TotalMs != 322 {
		t.Fatalf("total = %v, want 322", tr.TotalMs)
	}
	q := tr.Spans[3]
	if q.StartMs != 5 || q.DurMs != 15 {
		t.Fatalf("queue.wait = %+v, want start 5 dur 15", q)
	}
	if len(r.accepts) != 0 || len(r.order) != 0 {
		t.Fatal("accept entry must be released after the trace completes")
	}
}

func TestTraceWithoutAcceptStartsAtCreation(t *testing.T) {
	r := New()
	t0 := time.Unix(1_700_000_000, 0)
	r.RecordTrace("job-2", "job2", t0, []Timing{{Name: "provider.extract", Start: t0.Add(10 * time.Millisecond), End: t0.Add(60 * time.Millisecond)}})
	tr := r.Snapshot().LastTrace
	if tr == nil || len(tr.Spans) != 2 || tr.Spans[0].Name != "queue.wait" || tr.TotalMs != 60 {
		t.Fatalf("unexpected trace: %+v", tr)
	}
}

func TestPendingAcceptsAreBounded(t *testing.T) {
	r := New()
	now := time.Now()
	for i := range maxPendingAccepts + 10 {
		r.RecordAccept(string(rune('A'+i)), []Timing{{Name: "x", Start: now, End: now}})
	}
	if len(r.accepts) != maxPendingAccepts || len(r.order) != maxPendingAccepts {
		t.Fatalf("pending accepts = %d/%d, want %d", len(r.accepts), len(r.order), maxPendingAccepts)
	}
}

func TestNoTraceUntilAJobCompletes(t *testing.T) {
	if New().Snapshot().LastTrace != nil {
		t.Fatal("last_trace must be null until a real job completes")
	}
}
