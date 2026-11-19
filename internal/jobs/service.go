// Package jobs is the application service shared by the REST API and the
// MCP server: validation, idempotency hashing and job creation live here once.
package jobs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/google/uuid"

	"github.com/iAscen/ledgerline/internal/domain"
	"github.com/iAscen/ledgerline/internal/store"
	"github.com/iAscen/ledgerline/internal/telemetry"
	"github.com/iAscen/ledgerline/internal/webhook"
)

// ValidationError is a client input error, rendered as HTTP 400 / MCP tool error.
type ValidationError struct {
	Field string
	Msg   string
}

func (e *ValidationError) Error() string { return e.Field + ": " + e.Msg }

func invalid(field, format string, args ...any) error {
	return &ValidationError{Field: field, Msg: fmt.Sprintf(format, args...)}
}

// IsValidation reports whether err is a ValidationError.
func IsValidation(err error) bool {
	var v *ValidationError
	return errors.As(err, &v)
}

// CreateInput is a new extraction request.
type CreateInput struct {
	IdempotencyKey string
	MediaType      string
	Document       []byte
	WebhookURL     string
}

// Waker is notified after a job is created (the worker pool).
type Waker interface{ Notify() }

// Service implements the job use cases.
type Service struct {
	Store            store.Store
	Metrics          *telemetry.Metrics
	Waker            Waker
	MaxDocumentBytes int64
	AllowHTTPWebhook bool
}

// Create validates and enqueues a job, or returns the existing job for a
// replayed idempotency key.
func (s *Service) Create(ctx context.Context, tenant string, in CreateInput) (domain.Job, bool, error) {
	if err := validateKey(in.IdempotencyKey); err != nil {
		return domain.Job{}, false, err
	}
	if len(in.Document) == 0 {
		return domain.Job{}, false, invalid("document", "is required")
	}
	if int64(len(in.Document)) > s.MaxDocumentBytes {
		return domain.Job{}, false, invalid("document", "exceeds %d bytes", s.MaxDocumentBytes)
	}
	if !domain.SupportedMediaTypes[in.MediaType] {
		return domain.Job{}, false, invalid("media_type", "must be one of image/jpeg, image/png, image/gif, image/webp, application/pdf")
	}
	// Trust, but verify: the declared type must match the bytes.
	if sniffed := http.DetectContentType(in.Document); !strings.HasPrefix(sniffed, in.MediaType) {
		return domain.Job{}, false, invalid("document", "content looks like %q, not %q", sniffed, in.MediaType)
	}
	if in.WebhookURL != "" {
		if err := webhook.ValidateURL(in.WebhookURL, s.AllowHTTPWebhook); err != nil {
			return domain.Job{}, false, invalid("webhook_url", "%s", err.Error())
		}
	}

	job, created, err := s.Store.CreateJob(ctx, domain.NewJob{
		ID: uuid.New(), Tenant: tenant, IdempotencyKey: in.IdempotencyKey, RequestHash: requestHash(in),
		MediaType: in.MediaType, Document: in.Document, WebhookURL: in.WebhookURL,
		TraceParent: telemetry.TraceParent(ctx),
	})
	if err != nil {
		return domain.Job{}, false, err
	}
	if created {
		s.Metrics.JobsCreated.Inc()
		if s.Waker != nil {
			s.Waker.Notify()
		}
	} else {
		s.Metrics.IdemReplays.Inc()
	}
	return job, created, nil
}

// Get returns one job for tenant.
func (s *Service) Get(ctx context.Context, tenant, id string) (domain.Job, error) {
	uid, err := uuid.Parse(id)
	if err != nil {
		return domain.Job{}, domain.ErrNotFound
	}
	return s.Store.GetJob(ctx, tenant, uid)
}

// List returns the tenant's most recent jobs.
func (s *Service) List(ctx context.Context, tenant string, limit int) ([]domain.Job, error) {
	if limit < 1 || limit > 100 {
		return nil, invalid("limit", "must be between 1 and 100")
	}
	return s.Store.ListJobs(ctx, tenant, limit)
}

func validateKey(k string) error {
	if k == "" {
		return invalid("Idempotency-Key", "header is required")
	}
	if len(k) > 255 {
		return invalid("Idempotency-Key", "must be at most 255 characters")
	}
	for _, r := range k {
		if r < 0x21 || r > 0x7e {
			return invalid("Idempotency-Key", "must be printable ASCII without spaces")
		}
	}
	return nil
}

// requestHash fingerprints the request so a reused key with a different body
// is detected instead of silently returning an unrelated job.
func requestHash(in CreateInput) string {
	h := sha256.New()
	h.Write([]byte(in.MediaType))
	h.Write([]byte{0})
	h.Write([]byte(in.WebhookURL))
	h.Write([]byte{0})
	h.Write(in.Document)
	return hex.EncodeToString(h.Sum(nil))
}
