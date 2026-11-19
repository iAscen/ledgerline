package store

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/iAscen/ledgerline/internal/domain"
)

type memJob struct {
	job         domain.Job
	lockedUntil time.Time
}

// Memory is an in-process Store. It honours the same semantics as Postgres
// (idempotency, leases, attempts) so it can back tests and single-replica demos.
type Memory struct {
	mu    sync.Mutex
	jobs  map[uuid.UUID]*memJob
	byKey map[string]uuid.UUID
	now   func() time.Time
}

// NewMemory returns an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{jobs: map[uuid.UUID]*memJob{}, byKey: map[string]uuid.UUID{}, now: time.Now}
}

func idemKey(tenant, k string) string { return tenant + "\x00" + k }

// public returns a copy safe to hand out (no document bytes, no shared pointers).
func public(j domain.Job) domain.Job {
	j.Document = nil
	if j.Result != nil {
		r := *j.Result
		j.Result = &r
	}
	return j
}

// CreateJob implements Store.
func (m *Memory) CreateJob(_ context.Context, in domain.NewJob) (domain.Job, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if id, ok := m.byKey[idemKey(in.Tenant, in.IdempotencyKey)]; ok {
		existing := m.jobs[id].job
		if existing.RequestHash != in.RequestHash {
			return domain.Job{}, false, domain.ErrIdempotencyConflict
		}
		return public(existing), false, nil
	}
	now := m.now().UTC()
	j := domain.Job{
		ID: in.ID, Tenant: in.Tenant, IdempotencyKey: in.IdempotencyKey, RequestHash: in.RequestHash,
		Status: domain.StatusQueued, MediaType: in.MediaType, WebhookURL: in.WebhookURL, TraceParent: in.TraceParent,
		Document: append([]byte(nil), in.Document...), NextAttemptAt: now, CreatedAt: now, UpdatedAt: now,
	}
	m.jobs[j.ID] = &memJob{job: j}
	m.byKey[idemKey(in.Tenant, in.IdempotencyKey)] = j.ID
	return public(j), true, nil
}

// GetJob implements Store.
func (m *Memory) GetJob(_ context.Context, tenant string, id uuid.UUID) (domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mj, ok := m.jobs[id]
	if !ok || mj.job.Tenant != tenant {
		return domain.Job{}, domain.ErrNotFound
	}
	return public(mj.job), nil
}

// ListJobs implements Store.
func (m *Memory) ListJobs(_ context.Context, tenant string, limit int) ([]domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]domain.Job, 0)
	for _, mj := range m.jobs {
		if mj.job.Tenant == tenant {
			out = append(out, public(mj.job))
		}
	}
	sort.Slice(out, func(i, k int) bool { return out[i].CreatedAt.After(out[k].CreatedAt) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// ClaimNext implements Store.
func (m *Memory) ClaimNext(_ context.Context, lease time.Duration) (domain.Job, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()
	var best *memJob
	for _, mj := range m.jobs {
		runnable := (mj.job.Status == domain.StatusQueued && !mj.job.NextAttemptAt.After(now)) ||
			(mj.job.Status == domain.StatusProcessing && mj.lockedUntil.Before(now))
		if runnable && (best == nil || mj.job.NextAttemptAt.Before(best.job.NextAttemptAt)) {
			best = mj
		}
	}
	if best == nil {
		return domain.Job{}, domain.ErrNoJobAvailable
	}
	best.job.Status = domain.StatusProcessing
	best.job.Attempts++
	best.job.UpdatedAt = now.UTC()
	best.lockedUntil = now.Add(lease)
	j := best.job
	j.Document = append([]byte(nil), best.job.Document...)
	return j, nil
}

func (m *Memory) update(id uuid.UUID, fn func(mj *memJob)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	mj, ok := m.jobs[id]
	if !ok {
		return domain.ErrNotFound
	}
	fn(mj)
	mj.job.UpdatedAt = m.now().UTC()
	return nil
}

// Complete implements Store.
func (m *Memory) Complete(_ context.Context, id uuid.UUID, r domain.Receipt) error {
	return m.update(id, func(mj *memJob) {
		now := m.now().UTC()
		mj.job.Status, mj.job.Result, mj.job.Error, mj.job.CompletedAt = domain.StatusSucceeded, &r, "", &now
		mj.lockedUntil = time.Time{}
	})
}

// Fail implements Store.
func (m *Memory) Fail(_ context.Context, id uuid.UUID, reason string) error {
	return m.update(id, func(mj *memJob) {
		now := m.now().UTC()
		mj.job.Status, mj.job.Error, mj.job.CompletedAt = domain.StatusFailed, reason, &now
		mj.lockedUntil = time.Time{}
	})
}

// Reschedule implements Store.
func (m *Memory) Reschedule(_ context.Context, id uuid.UUID, reason string, at time.Time, refund bool) error {
	return m.update(id, func(mj *memJob) {
		mj.job.Status, mj.job.Error, mj.job.NextAttemptAt = domain.StatusQueued, reason, at.UTC()
		if refund && mj.job.Attempts > 0 {
			mj.job.Attempts--
		}
		mj.lockedUntil = time.Time{}
	})
}

// SetWebhookStatus implements Store.
func (m *Memory) SetWebhookStatus(_ context.Context, id uuid.UUID, status string) error {
	return m.update(id, func(mj *memJob) { mj.job.WebhookStatus = status })
}

// Ping implements Store.
func (m *Memory) Ping(context.Context) error { return nil }

// Close implements Store.
func (m *Memory) Close() {}

// QueueDepth implements Store.
func (m *Memory) QueueDepth(_ context.Context) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, mj := range m.jobs {
		if mj.job.Status == domain.StatusQueued || mj.job.Status == domain.StatusProcessing {
			n++
		}
	}
	return n, nil
}
