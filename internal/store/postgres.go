package store

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/iAscen/ledgerline/internal/domain"
	"github.com/iAscen/ledgerline/internal/store/pgdb"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Postgres is the production Store, backed by pgx and sqlc-generated queries.
type Postgres struct {
	pool *pgxpool.Pool
	q    *pgdb.Queries
}

// OpenPostgres connects and verifies the connection.
func OpenPostgres(ctx context.Context, dsn string) (*Postgres, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Postgres{pool: pool, q: pgdb.New(pool)}, nil
}

// Migrate applies the embedded goose migrations.
func (p *Postgres) Migrate(ctx context.Context) error {
	db := stdlib.OpenDBFromPool(p.pool)
	defer func() { _ = db.Close() }()
	provider, err := goose.NewProvider(goose.DialectPostgres, db, mustSub(migrations, "migrations"))
	if err != nil {
		return fmt.Errorf("goose provider: %w", err)
	}
	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	return nil
}

func text(s string) pgtype.Text { return pgtype.Text{String: s, Valid: s != ""} }

// row is the common column set returned by every non-claim query; sqlc emits
// one identical struct per query, which convert to this type directly.
type row struct {
	ID             uuid.UUID
	Tenant         string
	IdempotencyKey string
	RequestHash    string
	Status         string
	MediaType      string
	WebhookUrl     pgtype.Text
	WebhookStatus  pgtype.Text
	TraceParent    pgtype.Text
	Result         []byte
	Error          pgtype.Text
	Attempts       int32
	NextAttemptAt  time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
	CompletedAt    *time.Time
}

func (r row) job() (domain.Job, error) {
	j := domain.Job{
		ID: r.ID, Tenant: r.Tenant, IdempotencyKey: r.IdempotencyKey, RequestHash: r.RequestHash,
		Status: domain.Status(r.Status), MediaType: r.MediaType, WebhookURL: r.WebhookUrl.String,
		WebhookStatus: r.WebhookStatus.String, TraceParent: r.TraceParent.String, Error: r.Error.String,
		Attempts: int(r.Attempts), NextAttemptAt: r.NextAttemptAt.UTC(), CreatedAt: r.CreatedAt.UTC(),
		UpdatedAt: r.UpdatedAt.UTC(), CompletedAt: r.CompletedAt,
	}
	if len(r.Result) > 0 {
		var rc domain.Receipt
		if err := json.Unmarshal(r.Result, &rc); err != nil {
			return domain.Job{}, fmt.Errorf("decode result: %w", err)
		}
		j.Result = &rc
	}
	return j, nil
}

// CreateJob implements Store. The INSERT ... ON CONFLICT DO NOTHING is the
// idempotency check, so two concurrent requests with the same key cannot both
// create a job.
func (p *Postgres) CreateJob(ctx context.Context, in domain.NewJob) (domain.Job, bool, error) {
	ins, err := p.q.InsertJob(ctx, pgdb.InsertJobParams{
		ID: in.ID, Tenant: in.Tenant, IdempotencyKey: in.IdempotencyKey, RequestHash: in.RequestHash,
		MediaType: in.MediaType, Document: in.Document, WebhookUrl: text(in.WebhookURL), TraceParent: text(in.TraceParent),
	})
	if err == nil {
		j, err := row(ins).job()
		return j, true, err
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, false, fmt.Errorf("insert job: %w", err)
	}
	ex, err := p.q.GetJobByIdempotencyKey(ctx, pgdb.GetJobByIdempotencyKeyParams{Tenant: in.Tenant, IdempotencyKey: in.IdempotencyKey})
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("load idempotent job: %w", err)
	}
	if ex.RequestHash != in.RequestHash {
		return domain.Job{}, false, domain.ErrIdempotencyConflict
	}
	j, err := row(ex).job()
	return j, false, err
}

// GetJob implements Store.
func (p *Postgres) GetJob(ctx context.Context, tenant string, id uuid.UUID) (domain.Job, error) {
	r, err := p.q.GetJob(ctx, pgdb.GetJobParams{Tenant: tenant, ID: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, domain.ErrNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("get job: %w", err)
	}
	return row(r).job()
}

// ListJobs implements Store.
func (p *Postgres) ListJobs(ctx context.Context, tenant string, limit int) ([]domain.Job, error) {
	rows, err := p.q.ListJobs(ctx, pgdb.ListJobsParams{Tenant: tenant, Limit: int32(limit)}) //nolint:gosec // limit is bounded (1..100) by the API
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	out := make([]domain.Job, 0, len(rows))
	for _, r := range rows {
		j, err := row(r).job()
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, nil
}

// ClaimNext implements Store using FOR UPDATE SKIP LOCKED.
func (p *Postgres) ClaimNext(ctx context.Context, lease time.Duration) (domain.Job, error) {
	r, err := p.q.ClaimJob(ctx, lease.Seconds())
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Job{}, domain.ErrNoJobAvailable
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("claim job: %w", err)
	}
	j, err := row{
		ID: r.ID, Tenant: r.Tenant, IdempotencyKey: r.IdempotencyKey, RequestHash: r.RequestHash, Status: r.Status,
		MediaType: r.MediaType, WebhookUrl: r.WebhookUrl, WebhookStatus: r.WebhookStatus, TraceParent: r.TraceParent,
		Result: r.Result, Error: r.Error, Attempts: r.Attempts, NextAttemptAt: r.NextAttemptAt, CreatedAt: r.CreatedAt,
		UpdatedAt: r.UpdatedAt, CompletedAt: r.CompletedAt,
	}.job()
	j.Document = r.Document
	return j, err
}

// Complete implements Store.
func (p *Postgres) Complete(ctx context.Context, id uuid.UUID, rc domain.Receipt) error {
	b, err := json.Marshal(rc)
	if err != nil {
		return err
	}
	return p.q.CompleteJob(ctx, pgdb.CompleteJobParams{ID: id, Result: b})
}

// Fail implements Store.
func (p *Postgres) Fail(ctx context.Context, id uuid.UUID, reason string) error {
	return p.q.FailJob(ctx, pgdb.FailJobParams{ID: id, Error: text(reason)})
}

// Reschedule implements Store.
func (p *Postgres) Reschedule(ctx context.Context, id uuid.UUID, reason string, at time.Time, refund bool) error {
	var r int32
	if refund {
		r = 1
	}
	return p.q.RescheduleJob(ctx, pgdb.RescheduleJobParams{ID: id, Error: text(reason), NextAttemptAt: at, Refund: r})
}

// SetWebhookStatus implements Store.
func (p *Postgres) SetWebhookStatus(ctx context.Context, id uuid.UUID, status string) error {
	return p.q.SetWebhookStatus(ctx, pgdb.SetWebhookStatusParams{ID: id, WebhookStatus: text(status)})
}

// Exec runs raw SQL; used by tests and operational tooling only.
func (p *Postgres) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	tag, err := p.pool.Exec(ctx, sql, args...)
	return tag.RowsAffected(), err
}

// QueueDepth implements Store. Served by the jobs_claimable partial index.
func (p *Postgres) QueueDepth(ctx context.Context) (int, error) {
	var n int
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status IN ('queued', 'processing')`).Scan(&n)
	return n, err
}

// Ping implements Store.
func (p *Postgres) Ping(ctx context.Context) error { return p.pool.Ping(ctx) }

// Close implements Store.
func (p *Postgres) Close() { p.pool.Close() }
