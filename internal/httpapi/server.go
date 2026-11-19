// Package httpapi is the REST transport: routing, middleware, handlers.
package httpapi

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/iAscen/ledgerline/api"
	"github.com/iAscen/ledgerline/internal/auth"
	"github.com/iAscen/ledgerline/internal/domain"
	"github.com/iAscen/ledgerline/internal/jobs"
	"github.com/iAscen/ledgerline/internal/ratelimit"
	"github.com/iAscen/ledgerline/internal/resilience"
	"github.com/iAscen/ledgerline/internal/store"
	"github.com/iAscen/ledgerline/internal/telemetry"
)

// Options configures the API server.
type Options struct {
	Version     string
	Provider    string
	StoreKind   string
	CORSOrigins []string
	PublicDemo  bool
	MaxBody     int64
}

// Server holds the API's dependencies.
type Server struct {
	opts      Options
	jobs      *jobs.Service
	auth      *auth.Authenticator
	limiter   *ratelimit.Limiter
	demoLimit *ratelimit.Limiter
	breaker   *resilience.Breaker
	store     store.Store
	metrics   *telemetry.Metrics
	log       *slog.Logger
	mcp       http.Handler
	started   time.Time
	ready     atomic.Bool
}

// New builds the server. mcp may be nil.
func New(opts Options, svc *jobs.Service, a *auth.Authenticator, lim *ratelimit.Limiter, b *resilience.Breaker,
	st store.Store, m *telemetry.Metrics, log *slog.Logger, mcp http.Handler) *Server {
	return &Server{opts: opts, jobs: svc, auth: a, limiter: lim, demoLimit: ratelimit.New(0.1, 3), breaker: b,
		store: st, metrics: m, log: log, mcp: mcp, started: time.Now()}
}

// SetReady flips readiness (false during startup and shutdown drain).
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// Handler returns the public API handler.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	route := func(pattern string, h http.HandlerFunc) { mux.Handle(pattern, s.instrument(pattern, h)) }

	route("GET /healthz", s.healthz)
	route("GET /readyz", s.readyz)
	route("GET /v1/status", s.cors(s.status))
	route("OPTIONS /v1/status", s.cors(func(http.ResponseWriter, *http.Request) {}))
	route("GET /{$}", s.demoPage)
	route("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(api.OpenAPI)
	})
	if s.opts.PublicDemo {
		route("POST /v1/demo/token", s.cors(s.demoToken))
		route("OPTIONS /v1/demo/token", s.cors(func(http.ResponseWriter, *http.Request) {}))
	}
	route("POST /v1/jobs", s.authn(auth.ScopeJobsWrite, s.createJob))
	route("GET /v1/jobs", s.authn(auth.ScopeJobsRead, s.listJobs))
	route("GET /v1/jobs/{id}", s.authn(auth.ScopeJobsRead, s.getJob))
	if s.mcp != nil {
		route("/mcp", s.authn(auth.ScopeJobsRead, s.mcp.ServeHTTP))
	}
	route("/", func(w http.ResponseWriter, _ *http.Request) {
		problem(w, http.StatusNotFound, "Not Found", "no such route")
	})

	var h http.Handler = mux
	h = s.accessLog(h)
	h = otelhttp.NewHandler(h, "http.server", otelhttp.WithFilter(func(r *http.Request) bool {
		return r.URL.Path != "/healthz" && r.URL.Path != "/readyz"
	}), otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + r.URL.Path }))
	h = recoverer(s.log, h)
	return h
}

// AdminHandler serves /metrics on the internal port (never exposed publicly).
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", s.healthz)
	return mux
}

// --- handlers ---------------------------------------------------------------

//go:embed web/index.html
var demoHTML []byte

// demoPage serves the self-contained interactive demo (no external assets).
func (s *Server) demoPage(w http.ResponseWriter, _ *http.Request) {
	h := w.Header()
	h.Set("Content-Type", "text/html; charset=utf-8")
	h.Set("Cache-Control", "no-cache") // revalidate so a deploy is visible immediately
	h.Set("Content-Security-Policy", "default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; "+
		"img-src 'self' blob: data:; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Referrer-Policy", "no-referrer")
	_, _ = w.Write(demoHTML)
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// readyz checks the dependencies needed to serve traffic. The provider breaker
// is reported but deliberately does not fail readiness: an upstream outage
// should queue work, not pull every pod out of the Service and turn a
// degraded API into a full outage.
func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	checks := map[string]string{"store": "ok", "circuit_breaker": s.breaker.State().String()}
	status := http.StatusOK
	if !s.ready.Load() {
		checks["server"] = "draining"
		status = http.StatusServiceUnavailable
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	if err := s.store.Ping(ctx); err != nil {
		checks["store"] = "unavailable"
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, checks)
}

// status is the public, CORS-enabled endpoint the portfolio status card polls.
func (s *Server) status(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, map[string]any{
		"service":         "ledgerline",
		"status":          "ok",
		"version":         s.opts.Version,
		"uptime_seconds":  int(time.Since(s.started).Seconds()),
		"provider":        s.opts.Provider,
		"store":           s.opts.StoreKind,
		"circuit_breaker": s.breaker.State().String(),
		"region":          os.Getenv("FLY_REGION"),
	})
}

type createJobRequest struct {
	DocumentBase64 string `json:"document_base64"`
	MediaType      string `json:"media_type"`
	WebhookURL     string `json:"webhook_url,omitempty"`
}

func (s *Server) createJob(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	r.Body = http.MaxBytesReader(w, r.Body, s.opts.MaxBody)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	var req createJobRequest
	if err := dec.Decode(&req); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			problem(w, http.StatusRequestEntityTooLarge, "Payload Too Large", "request body exceeds the limit")
			return
		}
		problem(w, http.StatusBadRequest, "Bad Request", "invalid JSON body: "+err.Error())
		return
	}
	doc, err := base64.StdEncoding.DecodeString(req.DocumentBase64)
	if err != nil {
		problem(w, http.StatusBadRequest, "Bad Request", "document_base64: not valid base64")
		return
	}
	job, created, err := s.jobs.Create(r.Context(), p.Tenant, jobs.CreateInput{
		IdempotencyKey: r.Header.Get("Idempotency-Key"), MediaType: req.MediaType, Document: doc, WebhookURL: req.WebhookURL,
	})
	switch {
	case err == nil:
	case jobs.IsValidation(err):
		problem(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	case errors.Is(err, domain.ErrIdempotencyConflict):
		problem(w, http.StatusUnprocessableEntity, "Idempotency Key Reused", err.Error())
		return
	default:
		s.internalError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/jobs/"+job.ID.String())
	if !created {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, job)
		return
	}
	writeJSON(w, http.StatusAccepted, job)
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	job, err := s.jobs.Get(r.Context(), p.Tenant, r.PathValue("id"))
	if errors.Is(err, domain.ErrNotFound) {
		problem(w, http.StatusNotFound, "Not Found", "job not found")
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	if !job.Status.Terminal() {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, http.StatusOK, job)
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	limit := 20
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			problem(w, http.StatusBadRequest, "Bad Request", "limit must be an integer")
			return
		}
		limit = n
	}
	list, err := s.jobs.List(r.Context(), p.Tenant, limit)
	if jobs.IsValidation(err) {
		problem(w, http.StatusBadRequest, "Bad Request", err.Error())
		return
	}
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"jobs": list})
}

// demoToken mints a short-lived token for a fresh throwaway tenant so the
// public demo can be tried without credentials. Off by default.
func (s *Server) demoToken(w http.ResponseWriter, r *http.Request) {
	if ok, retry := s.demoLimit.Allow("ip:" + clientIP(r)); !ok {
		s.metrics.RateLimited.WithLabelValues("demo_token").Inc()
		tooMany(w, retry)
		return
	}
	tenant := "demo-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	tok, err := s.auth.Issue(tenant, []string{auth.ScopeJobsRead, auth.ScopeJobsWrite}, 15*time.Minute)
	if err != nil {
		s.internalError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{"access_token": tok, "token_type": "Bearer", "expires_in": 900, "tenant": tenant})
}

// --- middleware -------------------------------------------------------------

func (s *Server) authn(scope string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		tok, ok := strings.CutPrefix(h, "Bearer ")
		if !ok || tok == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ledgerline"`)
			problem(w, http.StatusUnauthorized, "Unauthorized", "missing bearer token")
			return
		}
		p, err := s.auth.Verify(tok)
		if err != nil {
			w.Header().Set("WWW-Authenticate", `Bearer realm="ledgerline", error="invalid_token"`)
			problem(w, http.StatusUnauthorized, "Unauthorized", "invalid or expired token")
			return
		}
		if !p.Has(scope) {
			problem(w, http.StatusForbidden, "Forbidden", "token lacks scope "+scope)
			return
		}
		w.Header().Set("RateLimit-Limit", strconv.Itoa(s.limiter.Limit()))
		if ok, retry := s.limiter.Allow("tenant:" + p.Tenant); !ok {
			s.metrics.RateLimited.WithLabelValues("tenant").Inc()
			tooMany(w, retry)
			return
		}
		next(w, r.WithContext(auth.WithPrincipal(r.Context(), p)))
	}
}

func (s *Server) cors(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && (slices.Contains(s.opts.CORSOrigins, "*") || slices.Contains(s.opts.CORSOrigins, origin)) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Idempotency-Key")
			w.Header().Set("Access-Control-Max-Age", "600")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

type statusRecorder struct {
	http.ResponseWriter
	code  int
	bytes int
}

func (r *statusRecorder) WriteHeader(code int) { r.code = code; r.ResponseWriter.WriteHeader(code) }
func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.code == 0 {
		r.code = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += n
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

// Flush keeps streaming responses (MCP over SSE) working through the wrapper.
func (r *statusRecorder) Flush() {
	if f, ok := r.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// instrument records RED metrics keyed by the route pattern (bounded
// cardinality: never the raw path).
func (s *Server) instrument(pattern string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if rec.code == 0 {
			rec.code = http.StatusOK
		}
		s.metrics.HTTPRequests.WithLabelValues(pattern, r.Method, strconv.Itoa(rec.code)).Inc()
		s.metrics.HTTPDuration.WithLabelValues(pattern, r.Method).Observe(time.Since(start).Seconds())
	})
}

func (s *Server) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if r.URL.Path == "/healthz" || r.URL.Path == "/readyz" {
			return
		}
		s.log.InfoContext(r.Context(), "http request", "method", r.Method, "path", r.URL.Path, "status", rec.code,
			"bytes", rec.bytes, "duration_ms", time.Since(start).Milliseconds(), "remote_ip", clientIP(r))
	})
}

func recoverer(log *slog.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { //nolint:contextcheck // logs with r.Context(); problem() needs no context
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler { //nolint:errorlint // sentinel compared by identity, as net/http does
					panic(v)
				}
				log.ErrorContext(r.Context(), "panic", "value", v)
				problem(w, http.StatusInternalServerError, "Internal Server Error", "unexpected error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// --- helpers ----------------------------------------------------------------

func clientIP(r *http.Request) string {
	// Fly's edge sets Fly-Client-IP; only trust it when actually running on Fly.
	if os.Getenv("FLY_APP_NAME") != "" {
		if ip := r.Header.Get("Fly-Client-IP"); ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (s *Server) internalError(w http.ResponseWriter, r *http.Request, err error) {
	s.log.ErrorContext(r.Context(), "request failed", "err", err)
	problem(w, http.StatusInternalServerError, "Internal Server Error", "unexpected error")
}

func tooMany(w http.ResponseWriter, retry time.Duration) {
	secs := int(retry.Seconds() + 0.999)
	if secs < 1 {
		secs = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(secs))
	problem(w, http.StatusTooManyRequests, "Too Many Requests", "rate limit exceeded")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false) // a JSON API, not HTML: keep "&" as "&"
	_ = enc.Encode(v)
}

// problem writes an RFC 9457 problem details response.
func problem(w http.ResponseWriter, code int, title, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"type": "about:blank", "title": title, "status": code, "detail": detail})
}
