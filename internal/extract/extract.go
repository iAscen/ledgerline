// Package extract turns a receipt image or PDF into a structured
// domain.Receipt. Providers sit behind the Provider interface so the pipeline,
// the tests and the public demo can run without calling a paid API.
package extract

import (
	"context"
	"errors"
	"fmt"

	"github.com/iAscen/ledgerline/internal/domain"
)

// Document is the provider input.
type Document struct {
	MediaType string
	Data      []byte
}

// Provider extracts a receipt from a document.
type Provider interface {
	Name() string
	Extract(ctx context.Context, doc Document) (domain.Receipt, error)
}

// PermanentError marks a failure that retrying will not fix (unreadable
// document, refusal, invalid request). Permanent errors fail the job
// immediately and do not count against the circuit breaker.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return "permanent: " + e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// Permanent wraps err as a PermanentError.
func Permanent(format string, args ...any) error {
	return &PermanentError{Err: fmt.Errorf(format, args...)}
}

// IsPermanent reports whether err (or anything it wraps) is permanent.
func IsPermanent(err error) bool {
	var p *PermanentError
	return errors.As(err, &p)
}
