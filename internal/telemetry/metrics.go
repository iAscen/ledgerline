// Package telemetry wires Prometheus metrics, OpenTelemetry tracing and
// trace-correlated structured logging.
package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Metrics holds every instrument the service exports. A dedicated registry
// (rather than the global default) keeps tests isolated.
type Metrics struct {
	Registry *prometheus.Registry

	HTTPRequests  *prometheus.CounterVec
	HTTPDuration  *prometheus.HistogramVec
	JobsCreated   prometheus.Counter
	IdemReplays   prometheus.Counter
	JobsFinished  *prometheus.CounterVec
	JobLatency    prometheus.Histogram
	JobRetries    *prometheus.CounterVec
	ProviderCalls *prometheus.CounterVec
	ProviderTime  *prometheus.HistogramVec
	BreakerState  prometheus.Gauge
	BreakerTrips  *prometheus.CounterVec
	Webhooks      *prometheus.CounterVec
	RateLimited   *prometheus.CounterVec
}

// NewMetrics registers all instruments on a fresh registry.
func NewMetrics() *Metrics {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	f := func(c prometheus.Collector) { r.MustRegister(c) }
	m := &Metrics{Registry: r}

	m.HTTPRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledgerline_http_requests_total", Help: "HTTP requests by route pattern, method and status code.",
	}, []string{"route", "method", "code"})
	m.HTTPDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ledgerline_http_request_duration_seconds", Help: "HTTP request latency.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"route", "method"})
	m.JobsCreated = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ledgerline_jobs_created_total", Help: "Extraction jobs accepted.",
	})
	m.IdemReplays = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ledgerline_idempotent_replays_total", Help: "POSTs answered from an existing idempotency key.",
	})
	m.JobsFinished = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledgerline_jobs_finished_total", Help: "Jobs reaching a terminal state.",
	}, []string{"status"})
	m.JobLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: "ledgerline_job_end_to_end_seconds", Help: "Time from job creation to terminal state.",
		Buckets: []float64{.5, 1, 2, 5, 10, 20, 30, 60, 120, 300, 600},
	})
	m.JobRetries = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledgerline_job_retries_total", Help: "Jobs rescheduled, by reason.",
	}, []string{"reason"})
	m.ProviderCalls = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledgerline_provider_requests_total", Help: "Extraction provider calls by outcome.",
	}, []string{"provider", "outcome"})
	m.ProviderTime = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "ledgerline_provider_request_duration_seconds", Help: "Extraction provider latency.",
		Buckets: []float64{.1, .25, .5, 1, 2, 5, 10, 20, 40, 60, 120},
	}, []string{"provider"})
	m.BreakerState = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "ledgerline_circuit_breaker_state", Help: "Provider circuit breaker: 0=closed, 1=open, 2=half_open.",
	})
	m.BreakerTrips = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledgerline_circuit_breaker_transitions_total", Help: "Breaker state transitions by target state.",
	}, []string{"to"})
	m.Webhooks = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledgerline_webhook_deliveries_total", Help: "Webhook deliveries by outcome.",
	}, []string{"outcome"})
	m.RateLimited = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "ledgerline_rate_limited_total", Help: "Requests rejected by the rate limiter.",
	}, []string{"scope"})

	for _, c := range []prometheus.Collector{
		m.HTTPRequests, m.HTTPDuration, m.JobsCreated, m.IdemReplays, m.JobsFinished, m.JobLatency, m.JobRetries,
		m.ProviderCalls, m.ProviderTime, m.BreakerState, m.BreakerTrips, m.Webhooks, m.RateLimited,
	} {
		f(c)
	}
	return m
}
