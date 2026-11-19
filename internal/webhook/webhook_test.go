package webhook

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/iAscen/ledgerline/internal/resilience"
)

var noWait = resilience.Backoff{Base: time.Millisecond, Max: time.Millisecond, Jitter: func(time.Duration) time.Duration { return 0 }}

func TestSignVerifyRoundTrip(t *testing.T) {
	secret, body, now := []byte("s3cret"), []byte(`{"a":1}`), time.Now()
	h := Sign(secret, body, now)
	if !Verify(secret, body, h, now, time.Minute) {
		t.Fatal("valid signature rejected")
	}
	if Verify(secret, []byte(`{"a":2}`), h, now, time.Minute) {
		t.Fatal("tampered body accepted")
	}
	if Verify([]byte("other"), body, h, now, time.Minute) {
		t.Fatal("wrong secret accepted")
	}
	if Verify(secret, body, h, now.Add(10*time.Minute), time.Minute) {
		t.Fatal("stale signature accepted")
	}
}

func TestSendRetriesThenSucceedsWithValidSignature(t *testing.T) {
	var calls atomic.Int32
	secret := []byte("k")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !Verify(secret, body, r.Header.Get(HeaderSignature), time.Now(), time.Minute) {
			t.Error("bad signature on delivery")
		}
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	s := NewSender(Config{Secret: secret, Attempts: 3, Backoff: noWait, AllowPrivate: true})
	if err := s.Send(context.Background(), srv.URL, "job.succeeded", "d1", []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("calls = %d, want 3", calls.Load())
	}
}

func TestSendDoesNotRetryClientErrors(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusGone)
	}))
	defer srv.Close()
	s := NewSender(Config{Secret: []byte("k"), Attempts: 5, Backoff: noWait, AllowPrivate: true})
	if err := s.Send(context.Background(), srv.URL, "e", "d", nil); err == nil || calls.Load() != 1 {
		t.Fatalf("err=%v calls=%d", err, calls.Load())
	}
}

func TestSendBlocksPrivateAddresses(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("request must not reach a loopback server")
	}))
	defer srv.Close()
	s := NewSender(Config{Secret: []byte("k"), Attempts: 3, Backoff: noWait})
	err := s.Send(context.Background(), srv.URL, "e", "d", nil)
	if !errors.Is(err, ErrBlockedAddress) {
		t.Fatalf("want ErrBlockedAddress, got %v", err)
	}
}

func TestPublicAndValidateURL(t *testing.T) {
	for ip, want := range map[string]bool{
		"8.8.8.8": true, "127.0.0.1": false, "10.1.2.3": false, "169.254.169.254": false,
		"100.64.0.1": false, "::1": false, "fd00::1": false, "2606:4700::1111": true,
	} {
		if got := Public(net.ParseIP(ip)); got != want {
			t.Errorf("Public(%s) = %v", ip, got)
		}
	}
	if ValidateURL("http://example.com/h", false) == nil {
		t.Error("http must be rejected in production mode")
	}
	if ValidateURL("https://user:pw@example.com/h", false) == nil {
		t.Error("credentials in URL must be rejected")
	}
	if err := ValidateURL("https://example.com/h", false); err != nil {
		t.Error(err)
	}
}
