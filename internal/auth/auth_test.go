package auth

import (
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var secret = []byte(strings.Repeat("k", 32))

func TestIssueVerifyRoundTrip(t *testing.T) {
	a, err := New(secret, "ledgerline", "ledgerline-api")
	if err != nil {
		t.Fatal(err)
	}
	tok, err := a.Issue("acme", []string{ScopeJobsRead, ScopeJobsWrite}, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	p, err := a.Verify(tok)
	if err != nil {
		t.Fatal(err)
	}
	if p.Tenant != "acme" || !p.Has(ScopeJobsWrite) {
		t.Fatalf("bad principal %+v", p)
	}
}

func TestVerifyRejects(t *testing.T) {
	a, _ := New(secret, "ledgerline", "ledgerline-api")
	other, _ := New([]byte(strings.Repeat("x", 32)), "ledgerline", "ledgerline-api")
	wrongAud, _ := New(secret, "ledgerline", "someone-else")

	expired := func() string {
		a2, _ := New(secret, "ledgerline", "ledgerline-api")
		a2.now = func() time.Time { return time.Now().Add(-2 * time.Hour) }
		tok, _ := a2.Issue("acme", nil, time.Hour)
		return tok
	}()
	forged, _ := other.Issue("acme", nil, time.Hour)
	aud, _ := wrongAud.Issue("acme", nil, time.Hour)
	none, _ := jwt.NewWithClaims(jwt.SigningMethodNone, jwt.MapClaims{
		"sub": "acme", "iss": "ledgerline", "aud": "ledgerline-api", "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString(jwt.UnsafeAllowNoneSignatureType)

	for name, tok := range map[string]string{
		"expired": expired, "wrong key": forged, "wrong audience": aud, "alg none": none, "garbage": "a.b.c",
	} {
		if _, err := a.Verify(tok); err == nil {
			t.Errorf("%s: token accepted", name)
		}
	}
	if _, err := New([]byte("short"), "i", "a"); err == nil {
		t.Error("short secret accepted")
	}
}
