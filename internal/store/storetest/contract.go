// Package storetest is a contract test suite run against every Store
// implementation, so the in-memory store cannot drift from Postgres.
package storetest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/iAscen/ledgerline/internal/domain"
	"github.com/iAscen/ledgerline/internal/store"
)

func newJob(tenant, key, hash string) domain.NewJob {
	return domain.NewJob{ID: uuid.New(), Tenant: tenant, IdempotencyKey: key, RequestHash: hash,
		MediaType: "image/png", Document: []byte("doc-" + key), WebhookURL: "https://example.com/h"}
}

// Run executes the contract against the store returned by factory. factory
// must return an empty store each call.
func Run(t *testing.T, factory func(t *testing.T) store.Store) {
	ctx := context.Background()

	t.Run("idempotent create returns the same job", func(t *testing.T) {
		s := factory(t)
		a, created, err := s.CreateJob(ctx, newJob("t1", "k1", "h1"))
		if err != nil || !created {
			t.Fatalf("first create: created=%v err=%v", created, err)
		}
		b, created, err := s.CreateJob(ctx, newJob("t1", "k1", "h1"))
		if err != nil || created {
			t.Fatalf("replay: created=%v err=%v", created, err)
		}
		if a.ID != b.ID {
			t.Fatalf("replay returned a different job: %s vs %s", a.ID, b.ID)
		}
		if _, _, err := s.CreateJob(ctx, newJob("t1", "k1", "DIFFERENT")); !errors.Is(err, domain.ErrIdempotencyConflict) {
			t.Fatalf("want conflict, got %v", err)
		}
		// Keys are scoped per tenant.
		c, created, err := s.CreateJob(ctx, newJob("t2", "k1", "h1"))
		if err != nil || !created || c.ID == a.ID {
			t.Fatalf("other tenant same key: created=%v err=%v", created, err)
		}
	})

	t.Run("concurrent creates with one key yield one job", func(t *testing.T) {
		s := factory(t)
		var wg sync.WaitGroup
		ids := make(chan uuid.UUID, 20)
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				j, _, err := s.CreateJob(ctx, newJob("t", "same", "h"))
				if err != nil {
					t.Error(err)
					return
				}
				ids <- j.ID
			}()
		}
		wg.Wait()
		close(ids)
		seen := map[uuid.UUID]bool{}
		for id := range ids {
			seen[id] = true
		}
		if len(seen) != 1 {
			t.Fatalf("want exactly one job, got %d", len(seen))
		}
	})

	t.Run("tenant isolation", func(t *testing.T) {
		s := factory(t)
		j, _, _ := s.CreateJob(ctx, newJob("owner", "k", "h"))
		if _, err := s.GetJob(ctx, "intruder", j.ID); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("cross-tenant read must be not found, got %v", err)
		}
		list, err := s.ListJobs(ctx, "intruder", 10)
		if err != nil || len(list) != 0 {
			t.Fatalf("cross-tenant list: %v %v", list, err)
		}
	})

	t.Run("claim, complete and lifecycle", func(t *testing.T) {
		s := factory(t)
		j, _, _ := s.CreateJob(ctx, newJob("t", "k", "h"))
		c, err := s.ClaimNext(ctx, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if c.ID != j.ID || c.Status != domain.StatusProcessing || c.Attempts != 1 || string(c.Document) != "doc-k" {
			t.Fatalf("bad claim: %+v", c)
		}
		if _, err := s.ClaimNext(ctx, time.Minute); !errors.Is(err, domain.ErrNoJobAvailable) {
			t.Fatalf("leased job must not be claimable, got %v", err)
		}
		r := domain.Receipt{Merchant: "M", Currency: "CAD", TotalMinor: 1234, Taxes: []domain.Tax{{Name: "GST", AmountMinor: 5}}, LineItems: []domain.LineItem{}}
		if err := s.Complete(ctx, j.ID, r); err != nil {
			t.Fatal(err)
		}
		if err := s.SetWebhookStatus(ctx, j.ID, "delivered"); err != nil {
			t.Fatal(err)
		}
		got, err := s.GetJob(ctx, "t", j.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != domain.StatusSucceeded || got.Result == nil || got.Result.TotalMinor != 1234 ||
			got.CompletedAt == nil || got.WebhookStatus != "delivered" || got.Document != nil {
			t.Fatalf("bad completed job: %+v", got)
		}
	})

	t.Run("reschedule and refund", func(t *testing.T) {
		s := factory(t)
		j, _, _ := s.CreateJob(ctx, newJob("t", "k", "h"))
		_, _ = s.ClaimNext(ctx, time.Minute)
		if err := s.Reschedule(ctx, j.ID, "later", time.Now().Add(time.Hour), true); err != nil {
			t.Fatal(err)
		}
		got, _ := s.GetJob(ctx, "t", j.ID)
		if got.Status != domain.StatusQueued || got.Attempts != 0 || got.Error != "later" {
			t.Fatalf("bad rescheduled job: %+v", got)
		}
		if _, err := s.ClaimNext(ctx, time.Minute); !errors.Is(err, domain.ErrNoJobAvailable) {
			t.Fatal("job scheduled in the future must not be claimable")
		}
		_ = s.Reschedule(ctx, j.ID, "now", time.Now().Add(-time.Second), false)
		c, err := s.ClaimNext(ctx, time.Minute)
		if err != nil || c.ID != j.ID {
			t.Fatalf("due job must be claimable: %v", err)
		}
		_ = s.Fail(ctx, j.ID, "boom")
		got, _ = s.GetJob(ctx, "t", j.ID)
		if got.Status != domain.StatusFailed || got.Error != "boom" {
			t.Fatalf("bad failed job: %+v", got)
		}
	})

	t.Run("expired lease is reclaimed", func(t *testing.T) {
		s := factory(t)
		j, _, _ := s.CreateJob(ctx, newJob("t", "k", "h"))
		if _, err := s.ClaimNext(ctx, time.Millisecond); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
		c, err := s.ClaimNext(ctx, time.Minute)
		if err != nil || c.ID != j.ID || c.Attempts != 2 {
			t.Fatalf("crashed worker's job must be reclaimed: %+v %v", c, err)
		}
	})

	t.Run("list is newest first and limited", func(t *testing.T) {
		s := factory(t)
		for _, k := range []string{"a", "b", "c"} {
			_, _, _ = s.CreateJob(ctx, newJob("t", k, "h"))
			time.Sleep(2 * time.Millisecond)
		}
		list, err := s.ListJobs(ctx, "t", 2)
		if err != nil || len(list) != 2 || list[0].IdempotencyKey != "c" {
			t.Fatalf("bad list: %+v %v", list, err)
		}
	})
}
