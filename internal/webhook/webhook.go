// Package webhook delivers job-completion notifications. Payloads are signed
// with HMAC-SHA256 and outbound connections to private networks are refused
// (webhook URLs are user-supplied, which makes them an SSRF vector).
package webhook

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"syscall"
	"time"

	"github.com/iAscen/ledgerline/internal/resilience"
)

// Header names on outbound deliveries.
const (
	HeaderSignature = "Ledgerline-Signature" // t=<unix>,v1=<hex hmac>
	HeaderEvent     = "Ledgerline-Event"
	HeaderDelivery  = "Ledgerline-Delivery"
)

// ErrBlockedAddress is returned when a webhook resolves to a disallowed IP.
var ErrBlockedAddress = errors.New("webhook destination resolves to a private or reserved address")

// Config tunes the Sender.
type Config struct {
	Secret       []byte
	Timeout      time.Duration
	Attempts     int
	Backoff      resilience.Backoff
	AllowPrivate bool // true only for local dev / tests
}

// Sender posts signed JSON payloads with retries.
type Sender struct {
	cfg    Config
	client *http.Client
}

// NewSender builds a Sender whose dialer enforces the address policy at
// connect time (after DNS resolution, so DNS rebinding cannot bypass it).
func NewSender(cfg Config) *Sender {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.Attempts == 0 {
		cfg.Attempts = 3
	}
	if cfg.Backoff.Base == 0 {
		cfg.Backoff = resilience.Backoff{Base: 500 * time.Millisecond, Max: 5 * time.Second}
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	if !cfg.AllowPrivate {
		dialer.Control = func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			if ip := net.ParseIP(host); ip == nil || !Public(ip) {
				return ErrBlockedAddress
			}
			return nil
		}
	}
	transport := &http.Transport{
		Proxy:               nil, // never route webhooks through an env proxy
		DialContext:         dialer.DialContext,
		TLSHandshakeTimeout: 5 * time.Second,
		MaxIdleConns:        20,
		IdleConnTimeout:     30 * time.Second,
	}
	return &Sender{cfg: cfg, client: &http.Client{
		Transport: transport,
		Timeout:   cfg.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse // a redirect is a delivery failure, not a hop to follow
		},
	}}
}

// Public reports whether ip is a globally routable unicast address.
func Public(ip net.IP) bool {
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !isCGNAT(ip)
}

func isCGNAT(ip net.IP) bool {
	_, cgnat, _ := net.ParseCIDR("100.64.0.0/10")
	return cgnat.Contains(ip)
}

// ValidateURL checks a webhook URL at submission time.
func ValidateURL(raw string, allowHTTP bool) error {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return errors.New("webhook_url must be an absolute URL")
	}
	if u.Scheme != "https" && (!allowHTTP || u.Scheme != "http") {
		return errors.New("webhook_url must use https")
	}
	if u.User != nil {
		return errors.New("webhook_url must not contain credentials")
	}
	return nil
}

// Sign returns the signature header value for body at time ts.
func Sign(secret, body []byte, ts time.Time) string {
	mac := hmac.New(sha256.New, secret)
	unix := strconv.FormatInt(ts.Unix(), 10)
	mac.Write([]byte(unix))
	mac.Write([]byte("."))
	mac.Write(body)
	return "t=" + unix + ",v1=" + hex.EncodeToString(mac.Sum(nil))
}

// Verify checks a signature header and rejects stale timestamps (replay).
func Verify(secret, body []byte, header string, now time.Time, tolerance time.Duration) bool {
	var ts int64
	var sig string
	for _, part := range bytes.Split([]byte(header), []byte(",")) {
		k, v, ok := bytes.Cut(part, []byte("="))
		if !ok {
			continue
		}
		switch string(k) {
		case "t":
			ts, _ = strconv.ParseInt(string(v), 10, 64)
		case "v1":
			sig = string(v)
		}
	}
	t := time.Unix(ts, 0)
	if ts == 0 || now.Sub(t) > tolerance || t.Sub(now) > tolerance {
		return false
	}
	want := Sign(secret, body, t)
	return hmac.Equal([]byte(want), []byte("t="+strconv.FormatInt(ts, 10)+",v1="+sig))
}

type statusError struct{ code int }

func (e statusError) Error() string { return fmt.Sprintf("webhook endpoint returned HTTP %d", e.code) }

// Send delivers body to target, retrying transient failures.
func (s *Sender) Send(ctx context.Context, target, event, deliveryID string, body []byte) error {
	retryable := func(err error) bool {
		var se statusError
		if errors.As(err, &se) {
			return se.code == http.StatusTooManyRequests || se.code >= 500
		}
		return !errors.Is(err, ErrBlockedAddress)
	}
	return resilience.Retry(ctx, s.cfg.Attempts, s.cfg.Backoff, retryable, func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("User-Agent", "ledgerline-webhook/1")
		req.Header.Set(HeaderEvent, event)
		req.Header.Set(HeaderDelivery, deliveryID)
		req.Header.Set(HeaderSignature, Sign(s.cfg.Secret, body, time.Now()))
		resp, err := s.client.Do(req)
		if err != nil {
			return err
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return statusError{resp.StatusCode}
		}
		return nil
	})
}
