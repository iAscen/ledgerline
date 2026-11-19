// Package livestats keeps a small, in-process rolling view of real traffic for
// the public /v1/stats endpoint: requests per minute, p50 latency and error
// rate over a sliding window, plus the last few sanitized events.
//
// It is deliberately tiny and bounded (fixed-size rings, no per-tenant data,
// no IPs, no tokens) so it is safe to expose publicly. Prometheus remains the
// source of truth for real monitoring; this is a human-readable window onto it.
package livestats

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

const (
	// Window is the span used for p50 latency and error rate.
	Window = 5 * time.Minute
	// maxSamples bounds memory: a demo instance never needs more.
	maxSamples = 2048
	// maxEvents is how many recent events the endpoint returns.
	maxEvents = 8
)

type sample struct {
	at       time.Time
	duration time.Duration
	status   int
}

// Event is one sanitized line for the public feed.
type Event struct {
	At   time.Time `json:"at"`
	Kind string    `json:"kind"` // "http" or "job"
	Text string    `json:"text"`
}

// Snapshot is the computed view returned by /v1/stats.
type Snapshot struct {
	WindowSeconds  int      `json:"window_seconds"`
	RequestsPerMin int      `json:"requests_per_min"`
	RequestsWindow int      `json:"requests_in_window"`
	P50Ms          *float64 `json:"p50_ms"`     // nil when there were no requests in the window
	ErrorRate      *float64 `json:"error_rate"` // share of 5xx responses; nil when no requests
	Events         []Event  `json:"events"`
}

// Recorder is safe for concurrent use. The zero value is not usable; call New.
type Recorder struct {
	mu      sync.Mutex
	samples []sample // ring
	next    int
	full    bool
	events  []Event // ring
	evNext  int
	evFull  bool
	now     func() time.Time
}

// New returns an empty recorder.
func New() *Recorder {
	return &Recorder{
		samples: make([]sample, maxSamples),
		events:  make([]Event, maxEvents),
		now:     time.Now,
	}
}

// RecordRequest stores one served request. route is the matched route pattern
// (for example "GET /v1/jobs/{id}"), never the raw path, so no IDs leak.
func (r *Recorder) RecordRequest(route string, status int, d time.Duration) {
	if r == nil {
		return
	}
	now := r.now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples[r.next] = sample{at: now, duration: d, status: status}
	r.next = (r.next + 1) % maxSamples
	if r.next == 0 {
		r.full = true
	}
	r.pushEvent(Event{At: now, Kind: "http", Text: fmt.Sprintf("%s %d %s", route, status, fmtDuration(d))})
}

// RecordJob stores a job lifecycle event. shortID should be a short prefix of
// the job ID; outcome is a fixed vocabulary word ("succeeded", "failed", ...).
func (r *Recorder) RecordJob(shortID, outcome string, attempts int) {
	if r == nil {
		return
	}
	now := r.now().UTC()
	r.mu.Lock()
	defer r.mu.Unlock()
	text := fmt.Sprintf("job %s %s", shortID, outcome)
	if attempts > 0 {
		text += fmt.Sprintf(" (attempt %d)", attempts)
	}
	r.pushEvent(Event{At: now, Kind: "job", Text: text})
}

func (r *Recorder) pushEvent(e Event) {
	r.events[r.evNext] = e
	r.evNext = (r.evNext + 1) % maxEvents
	if r.evNext == 0 {
		r.evFull = true
	}
}

// Snapshot computes the current view.
func (r *Recorder) Snapshot() Snapshot {
	snap := Snapshot{WindowSeconds: int(Window / time.Second), Events: []Event{}}
	if r == nil {
		return snap
	}
	now := r.now()
	r.mu.Lock()
	defer r.mu.Unlock()

	n := r.next
	if r.full {
		n = maxSamples
	}
	var durations []time.Duration
	errs := 0
	for i := range n {
		s := r.samples[i]
		age := now.Sub(s.at)
		if age > Window {
			continue
		}
		if age <= time.Minute {
			snap.RequestsPerMin++
		}
		durations = append(durations, s.duration)
		if s.status >= 500 {
			errs++
		}
	}
	snap.RequestsWindow = len(durations)
	if len(durations) > 0 {
		slices.Sort(durations)
		p50 := float64(durations[(len(durations)-1)/2].Microseconds()) / 1000
		rate := float64(errs) / float64(len(durations))
		snap.P50Ms, snap.ErrorRate = &p50, &rate
	}

	// Events, newest first.
	count := r.evNext
	if r.evFull {
		count = maxEvents
	}
	for i := range count {
		idx := (r.evNext - 1 - i + maxEvents) % maxEvents
		snap.Events = append(snap.Events, r.events[idx])
	}
	return snap
}

func fmtDuration(d time.Duration) string {
	if d < time.Millisecond {
		return "<1ms"
	}
	if d < 10*time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Round(time.Second).String()
}
