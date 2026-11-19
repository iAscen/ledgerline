package store_test

import (
	"context"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/iAscen/ledgerline/internal/store"
	"github.com/iAscen/ledgerline/internal/store/storetest"
)

func TestMemoryContract(t *testing.T) {
	storetest.Run(t, func(*testing.T) store.Store { return store.NewMemory() })
}

// TestPostgresContract runs the same contract against a real Postgres in a
// throwaway container. It skips when Docker is unavailable locally, and is
// forced on in CI (LEDGERLINE_REQUIRE_DOCKER=1) so it can never silently skip there.
func TestPostgresContract(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	if runtime.GOOS == "windows" && os.Getenv("TESTCONTAINERS_RYUK_DISABLED") == "" {
		// The Ryuk reaper fails to start on Docker Desktop for Windows; the
		// t.Cleanup below terminates the container anyway.
		t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
	}
	if os.Getenv("LEDGERLINE_REQUIRE_DOCKER") != "1" {
		testcontainers.SkipIfProviderIsNotHealthy(t)
	}
	ctx := context.Background()
	ctr, err := tcpostgres.Run(ctx, "postgres:17-alpine",
		tcpostgres.WithDatabase("ledgerline"), tcpostgres.WithUsername("ledgerline"), tcpostgres.WithPassword("ledgerline"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(ctr) })
	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pg, err := store.OpenPostgres(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Close)
	if err := pg.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pg.Migrate(ctx); err != nil { // migrations must be re-runnable
		t.Fatal(err)
	}
	storetest.Run(t, func(t *testing.T) store.Store {
		if _, err := pg.Exec(ctx, "TRUNCATE jobs"); err != nil {
			t.Fatal(err)
		}
		return pg
	})
}
