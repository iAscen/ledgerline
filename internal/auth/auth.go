// Package auth issues and verifies the HS256 JWTs that authenticate API and
// MCP callers. The token subject is the tenant: every query is scoped by it.
package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Scopes.
const (
	ScopeJobsRead  = "jobs:read"
	ScopeJobsWrite = "jobs:write"
)

// Principal is the authenticated caller.
type Principal struct {
	Tenant string
	Scopes []string
}

// Has reports whether the principal holds scope.
func (p Principal) Has(scope string) bool { return slices.Contains(p.Scopes, scope) }

type claims struct {
	Scope string `json:"scope"`
	jwt.RegisteredClaims
}

// Authenticator verifies and issues tokens.
type Authenticator struct {
	secret   []byte
	issuer   string
	audience string
	now      func() time.Time
}

// New returns an Authenticator. The secret must be at least 32 bytes.
func New(secret []byte, issuer, audience string) (*Authenticator, error) {
	if len(secret) < 32 {
		return nil, errors.New("jwt secret must be at least 32 bytes")
	}
	return &Authenticator{secret: secret, issuer: issuer, audience: audience, now: time.Now}, nil
}

// Issue mints a token for tenant with the given scopes and lifetime.
func (a *Authenticator) Issue(tenant string, scopes []string, ttl time.Duration) (string, error) {
	now := a.now()
	t := jwt.NewWithClaims(jwt.SigningMethodHS256, claims{
		Scope: strings.Join(scopes, " "),
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: tenant, Issuer: a.issuer, Audience: jwt.ClaimStrings{a.audience},
			IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(ttl)),
		},
	})
	return t.SignedString(a.secret)
}

// Verify parses a bearer token. Algorithm, issuer, audience and expiry are all
// enforced; "alg: none" and RS/HS confusion are rejected by WithValidMethods.
func (a *Authenticator) Verify(token string) (Principal, error) {
	var c claims
	_, err := jwt.ParseWithClaims(token, &c, func(*jwt.Token) (any, error) { return a.secret, nil },
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithIssuer(a.issuer), jwt.WithAudience(a.audience), jwt.WithExpirationRequired(),
		jwt.WithTimeFunc(a.now), jwt.WithLeeway(30*time.Second),
	)
	if err != nil {
		return Principal{}, fmt.Errorf("invalid token: %w", err)
	}
	if c.Subject == "" {
		return Principal{}, errors.New("invalid token: missing subject")
	}
	return Principal{Tenant: c.Subject, Scopes: strings.Fields(c.Scope)}, nil
}

type ctxKey struct{}

// WithPrincipal stores p in ctx.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, ctxKey{}, p)
}

// FromContext returns the principal stored by the auth middleware.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}
