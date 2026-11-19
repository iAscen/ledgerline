package app_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/iAscen/ledgerline/internal/app"
	"github.com/iAscen/ledgerline/internal/config"
	"github.com/iAscen/ledgerline/internal/domain"
	"github.com/iAscen/ledgerline/internal/secrets"
)

const jwtSecret = "test-secret-test-secret-test-secret!"

// png returns bytes that sniff as image/png, with payload appended (the fake
// provider derives its output from the bytes, and reacts to failure markers).
func png(payload string) []byte {
	return append([]byte("\x89PNG\r\n\x1a\n"), payload...)
}

type env struct {
	t     *testing.T
	app   *app.App
	srv   *httptest.Server
	admin *httptest.Server
}

func newEnv(t *testing.T, mutate func(*config.Config)) *env {
	t.Helper()
	t.Setenv("LEDGERLINE_JWT_SECRET", jwtSecret)
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Store, cfg.Provider, cfg.FakeLatency = "memory", "fake", 0
	cfg.RateLimitRPS, cfg.RateLimitBurst = 1000, 1000
	cfg.CORSOrigins = []string{"https://portfolio.example"}
	if mutate != nil {
		mutate(&cfg)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	sec, err := secrets.Load(context.Background(), cfg.Vault, log)
	if err != nil {
		t.Fatal(err)
	}
	a, err := app.New(context.Background(), cfg, sec, "test", log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { a.Pool.Run(ctx); close(done) }()
	a.API.SetReady(true)
	e := &env{t: t, app: a, srv: httptest.NewServer(a.API.Handler()), admin: httptest.NewServer(a.API.AdminHandler())}
	t.Cleanup(func() { e.srv.Close(); e.admin.Close(); cancel(); <-done; a.Close() })
	return e
}

func (e *env) token(tenant string, scopes ...string) string {
	if len(scopes) == 0 {
		scopes = []string{"jobs:read", "jobs:write"}
	}
	tok, err := e.app.Auth.Issue(tenant, scopes, time.Hour)
	if err != nil {
		e.t.Fatal(err)
	}
	return tok
}

func (e *env) do(method, path, token string, headers map[string]string, body any) (*http.Response, []byte) {
	e.t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, e.srv.URL+path, r)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func jobBody(doc []byte) map[string]any {
	return map[string]any{"document_base64": base64.StdEncoding.EncodeToString(doc), "media_type": "image/png"}
}

func decodeJob(t *testing.T, b []byte) domain.Job {
	t.Helper()
	var j domain.Job
	if err := json.Unmarshal(b, &j); err != nil {
		t.Fatalf("decode %s: %v", b, err)
	}
	return j
}

func (e *env) waitTerminal(tok, id string) domain.Job {
	e.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, b := e.do("GET", "/v1/jobs/"+id, tok, nil, nil)
		if j := decodeJob(e.t, b); j.Status.Terminal() {
			return j
		}
		time.Sleep(20 * time.Millisecond)
	}
	e.t.Fatalf("job %s did not finish", id)
	return domain.Job{}
}

func TestIdempotentPostReturnsSameJob(t *testing.T) {
	e := newEnv(t, nil)
	tok := e.token("acme")
	h := map[string]string{"Idempotency-Key": "receipt-0001"}

	r1, b1 := e.do("POST", "/v1/jobs", tok, h, jobBody(png("coffee")))
	if r1.StatusCode != http.StatusAccepted {
		t.Fatalf("first POST: %d %s", r1.StatusCode, b1)
	}
	r2, b2 := e.do("POST", "/v1/jobs", tok, h, jobBody(png("coffee")))
	if r2.StatusCode != http.StatusOK || r2.Header.Get("Idempotent-Replayed") != "true" {
		t.Fatalf("replay: %d replayed=%q %s", r2.StatusCode, r2.Header.Get("Idempotent-Replayed"), b2)
	}
	j1, j2 := decodeJob(t, b1), decodeJob(t, b2)
	if j1.ID != j2.ID {
		t.Fatalf("idempotent POST created two jobs: %s and %s", j1.ID, j2.ID)
	}
	if r1.Header.Get("Location") != "/v1/jobs/"+j1.ID.String() {
		t.Errorf("Location = %q", r1.Header.Get("Location"))
	}

	r3, b3 := e.do("POST", "/v1/jobs", tok, h, jobBody(png("a different receipt")))
	if r3.StatusCode != http.StatusUnprocessableEntity {
		t.Fatalf("key reuse with different body: %d %s", r3.StatusCode, b3)
	}

	done := e.waitTerminal(tok, j1.ID.String())
	if done.Status != domain.StatusSucceeded || done.Result == nil || done.Result.TotalMinor == 0 {
		t.Fatalf("job did not succeed: %+v", done)
	}
}

func TestAuthAndValidation(t *testing.T) {
	e := newEnv(t, nil)
	if r, _ := e.do("POST", "/v1/jobs", "", nil, jobBody(png("x"))); r.StatusCode != http.StatusUnauthorized {
		t.Errorf("no token: %d", r.StatusCode)
	}
	if r, _ := e.do("GET", "/v1/jobs", "not-a-jwt", nil, nil); r.StatusCode != http.StatusUnauthorized {
		t.Errorf("bad token: %d", r.StatusCode)
	}
	readOnly := e.token("acme", "jobs:read")
	if r, _ := e.do("POST", "/v1/jobs", readOnly, map[string]string{"Idempotency-Key": "k"}, jobBody(png("x"))); r.StatusCode != http.StatusForbidden {
		t.Errorf("read-only token on POST: %d", r.StatusCode)
	}
	tok := e.token("acme")
	cases := map[string]struct {
		key  string
		body any
	}{
		"missing idempotency key": {"", jobBody(png("x"))},
		"bytes are not a png":     {"k1", map[string]any{"document_base64": base64.StdEncoding.EncodeToString([]byte("hello")), "media_type": "image/png"}},
		"unsupported media type":  {"k2", map[string]any{"document_base64": base64.StdEncoding.EncodeToString(png("x")), "media_type": "text/plain"}},
		"http webhook":            {"k3", map[string]any{"document_base64": base64.StdEncoding.EncodeToString(png("x")), "media_type": "image/png", "webhook_url": "http://example.com"}},
		"unknown field":           {"k4", map[string]any{"document_base64": "", "media_type": "image/png", "extra": 1}},
		"bad base64":              {"k5", map[string]any{"document_base64": "%%%", "media_type": "image/png"}},
	}
	for name, c := range cases {
		h := map[string]string{}
		if c.key != "" {
			h["Idempotency-Key"] = c.key
		}
		r, b := e.do("POST", "/v1/jobs", tok, h, c.body)
		if r.StatusCode != http.StatusBadRequest || r.Header.Get("Content-Type") != "application/problem+json" {
			t.Errorf("%s: got %d %s", name, r.StatusCode, b)
		}
	}
}

func TestTenantIsolation(t *testing.T) {
	e := newEnv(t, nil)
	_, b := e.do("POST", "/v1/jobs", e.token("alice"), map[string]string{"Idempotency-Key": "k"}, jobBody(png("x")))
	id := decodeJob(t, b).ID.String()
	if r, _ := e.do("GET", "/v1/jobs/"+id, e.token("mallory"), nil, nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("other tenant read: %d", r.StatusCode)
	}
}

func TestRateLimit(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.RateLimitRPS, c.RateLimitBurst = 0.01, 3 })
	tok := e.token("acme")
	for i := 0; i < 3; i++ {
		if r, _ := e.do("GET", "/v1/jobs", tok, nil, nil); r.StatusCode != http.StatusOK {
			t.Fatalf("request %d: %d", i, r.StatusCode)
		}
	}
	r, _ := e.do("GET", "/v1/jobs", tok, nil, nil)
	if r.StatusCode != http.StatusTooManyRequests || r.Header.Get("Retry-After") == "" {
		t.Fatalf("want 429 with Retry-After, got %d", r.StatusCode)
	}
	if r, _ := e.do("GET", "/v1/jobs", e.token("other"), nil, nil); r.StatusCode != http.StatusOK {
		t.Fatal("limits must be per tenant")
	}
}

func TestBreakerOpensUnderForcedFailuresAndIsVisible(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.BreakerThreshold, c.BreakerOpenTimeout = 2, time.Hour })
	tok := e.token("acme")
	for i := 0; i < 4; i++ {
		e.do("POST", "/v1/jobs", tok, map[string]string{"Idempotency-Key": "fail-" + string(rune('a'+i))}, jobBody(png("LEDGERLINE_FAIL")))
	}
	deadline := time.Now().Add(5 * time.Second)
	for e.app.Breaker.State().String() != "open" && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	_, b := e.do("GET", "/v1/status", "", nil, nil)
	if !strings.Contains(string(b), `"circuit_breaker":"open"`) {
		t.Fatalf("status should report open breaker: %s", b)
	}
	// Readiness must stay green while the provider is down (queue, don't cascade).
	if r, rb := e.do("GET", "/readyz", "", nil, nil); r.StatusCode != http.StatusOK {
		t.Fatalf("readyz: %d %s", r.StatusCode, rb)
	}
	resp, err := http.Get(e.admin.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	m, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"ledgerline_circuit_breaker_state 1", `ledgerline_circuit_breaker_transitions_total{to="open"} 1`, "ledgerline_jobs_created_total 4"} {
		if !strings.Contains(string(m), want) {
			t.Errorf("metrics missing %q", want)
		}
	}
}

func TestStatusCORSAndProbes(t *testing.T) {
	e := newEnv(t, nil)
	r, b := e.do("GET", "/v1/status", "", map[string]string{"Origin": "https://portfolio.example"}, nil)
	if r.StatusCode != http.StatusOK || r.Header.Get("Access-Control-Allow-Origin") != "https://portfolio.example" {
		t.Fatalf("status/CORS: %d %q %s", r.StatusCode, r.Header.Get("Access-Control-Allow-Origin"), b)
	}
	if r, _ := e.do("GET", "/v1/status", "", map[string]string{"Origin": "https://evil.example"}, nil); r.Header.Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("CORS must not allow unknown origins")
	}
	if r, _ := e.do("GET", "/healthz", "", nil, nil); r.StatusCode != http.StatusOK {
		t.Fatal("healthz")
	}
	e.app.API.SetReady(false)
	if r, _ := e.do("GET", "/readyz", "", nil, nil); r.StatusCode != http.StatusServiceUnavailable {
		t.Fatal("readyz must fail while draining")
	}
	if r, b := e.do("GET", "/openapi.yaml", "", nil, nil); r.StatusCode != http.StatusOK || !bytes.HasPrefix(b, []byte("openapi: 3.1.0")) {
		t.Fatal("openapi not served")
	}
	r, b = e.do("GET", "/", "", nil, nil)
	if r.StatusCode != http.StatusOK || !strings.Contains(string(b), "<title>ledgerline") ||
		!strings.Contains(r.Header.Get("Content-Security-Policy"), "frame-ancestors 'none'") {
		t.Fatalf("demo page: %d csp=%q", r.StatusCode, r.Header.Get("Content-Security-Policy"))
	}
	if r, _ := e.do("GET", "/no-such-page", "", nil, nil); r.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown path must 404, got %d", r.StatusCode)
	}
}

type bearer struct {
	token string
	base  http.RoundTripper
}

func (b bearer) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+b.token)
	return b.base.RoundTrip(r)
}

func TestMCPExtractReceipt(t *testing.T) {
	e := newEnv(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-agent", Version: "1"}, nil)
	session, err := client.Connect(ctx, &mcp.StreamableClientTransport{
		Endpoint:   e.srv.URL + "/mcp",
		HTTPClient: &http.Client{Transport: bearer{e.token("agent-tenant"), http.DefaultTransport}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = session.Close() }()

	tools, err := session.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tl := range tools.Tools {
		names[tl.Name] = true
	}
	for _, want := range []string{"extract_receipt", "get_job", "list_jobs"} {
		if !names[want] {
			t.Errorf("tool %s not listed", want)
		}
	}

	res, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "extract_receipt", Arguments: map[string]any{
		"document_base64": base64.StdEncoding.EncodeToString(png("mcp receipt")), "media_type": "image/png", "wait_seconds": 5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if res.IsError {
		t.Fatalf("tool error: %v", res.Content)
	}
	text := res.Content[0].(*mcp.TextContent).Text
	j := decodeJob(t, []byte(text))
	if j.Status != domain.StatusSucceeded || j.Result == nil {
		t.Fatalf("want succeeded job with result, got %s", text)
	}
	// Same document again: the default idempotency key dedupes agent retries.
	res2, _ := session.CallTool(ctx, &mcp.CallToolParams{Name: "extract_receipt", Arguments: map[string]any{
		"document_base64": base64.StdEncoding.EncodeToString(png("mcp receipt")), "media_type": "image/png",
	}})
	if j2 := decodeJob(t, []byte(res2.Content[0].(*mcp.TextContent).Text)); j2.ID != j.ID {
		t.Fatal("agent retry created a duplicate job")
	}
	// The job is visible through REST to the same tenant.
	if r, _ := e.do("GET", "/v1/jobs/"+j.ID.String(), e.token("agent-tenant"), nil, nil); r.StatusCode != http.StatusOK {
		t.Fatalf("REST view of MCP job: %d", r.StatusCode)
	}
}
