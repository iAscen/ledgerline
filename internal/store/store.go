// Package store defines the persistence contract for jobs and its two
// implementations: Postgres (production) and in-memory (tests, zero-dependency
// demo deployments). Both are exercised by the same contract test suite.
package store

import (
	"context"
	"io/fs"
	"time"

	"github.com/google/uuid"

	"github.com/iAscen/ledgerline/internal/domain"
)

// Store is the persistence contract.
type Store interface {
	// CreateJob inserts a job, or returns the existing job for the same
	// (tenant, idempotency key). created is false on a replay. A replay with a
	// different request hash returns domain.ErrIdempotencyConflict.
	CreateJob(ctx context.Context, in domain.NewJob) (job domain.Job, created bool, err error)
	GetJob(ctx context.Context, tenant string, id uuid.UUID) (domain.Job, error)
	ListJobs(ctx context.Context, tenant string, limit int) ([]domain.Job, error)

	// ClaimNext leases the next runnable job (queued and due, or processing with
	// an expired lease) and increments its attempt counter. It returns
	// domain.ErrNoJobAvailable when there is nothing to do.
	ClaimNext(ctx context.Context, lease time.Duration) (domain.Job, error)
	Complete(ctx context.Context, id uuid.UUID, r domain.Receipt) error
	Fail(ctx context.Context, id uuid.UUID, reason string) error
	// Reschedule puts a job back in the queue. refundAttempt undoes the claim's
	// attempt increment (used when the provider was never actually called,
	// e.g. because the circuit breaker was open).
	Reschedule(ctx context.Context, id uuid.UUID, reason string, at time.Time, refundAttempt bool) error
	SetWebhookStatus(ctx context.Context, id uuid.UUID, status string) error
	// QueueDepth counts jobs not yet finished (queued or processing).
	QueueDepth(ctx context.Context) (int, error)

	Ping(ctx context.Context) error
	Close()
}

func mustSub(f fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		panic(err)
	}
	return sub
}
