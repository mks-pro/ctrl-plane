package store_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/your-org/ctrlplane/internal/domain"
	"github.com/your-org/ctrlplane/migrations"

	"github.com/your-org/ctrlplane/internal/store"
)

// testEnv bundles the repository under test with a raw pool for assertions.
// Tests are skipped unless TEST_DATABASE_URL points at a disposable database:
// the schema is dropped and recreated on every run.
type testEnv struct {
	st *store.Store
	db *pgxpool.Pool
}

const testSchema = "ctrlplane_store_it"

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run store integration tests")
	}
	dsn = schemaDSN(t, dsn, testSchema)
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(db.Close)
	if err := resetSchema(ctx, db); err != nil {
		t.Fatalf("reset schema: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(ctx, dsn, 4, "ctrlplane.events", domain.SystemClock{}, logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)
	return &testEnv{st: st, db: db}
}

// schemaDSN gives every test package its own schema so `go test ./...` can run
// packages in parallel against one database.
func schemaDSN(t *testing.T, dsn, schema string) string {
	t.Helper()
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("admin pool: %v", err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE; CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema %s: %v", schema, err)
	}
	sep := "?"
	if strings.Contains(dsn, "?") {
		sep = "&"
	}
	return dsn + sep + "search_path=" + schema
}

func resetSchema(ctx context.Context, db *pgxpool.Pool) error {
	if _, err := db.Exec(ctx, "DROP SCHEMA IF EXISTS "+testSchema+" CASCADE; CREATE SCHEMA "+testSchema); err != nil {
		return err
	}
	all, err := migrations.All()
	if err != nil {
		return err
	}
	for _, m := range all {
		if _, err := db.Exec(ctx, m.Up); err != nil {
			return fmt.Errorf("apply migration %s: %w", m.Version, err)
		}
	}
	return nil
}

func mustCreateTenant(t *testing.T, env *testEnv, slug string) (domain.Tenant, domain.Task) {
	t.Helper()
	tenant, task, err := env.st.CreateTenant(context.Background(), slug, "Name "+slug)
	if err != nil {
		t.Fatalf("create tenant %s: %v", slug, err)
	}
	return tenant, task
}

func resultEvent(tenantID, taskID uuid.UUID, status domain.TaskStatus) domain.ResultEvent {
	return domain.ResultEvent{
		SchemaVersion: domain.SchemaVersion,
		EventID:       uuid.New(),
		EventType:     domain.EventTaskResult,
		OccurredAt:    domain.FormatTime(domain.SystemClock{}.Now()),
		TaskID:        taskID,
		TenantID:      tenantID,
		Status:        status,
		Attempt:       1,
		WorkerID:      "worker-test",
	}
}

func mustApply(t *testing.T, env *testEnv, ev domain.ResultEvent) store.ResultOutcome {
	t.Helper()
	out, err := env.st.ApplyResult(context.Background(), ev)
	if err != nil {
		t.Fatalf("apply result %s/%s: %v", ev.EventID, ev.Status, err)
	}
	return out
}

// scalarInt runs a single-column integer query, failing the test on error.
func scalarInt(t *testing.T, env *testEnv, query string, args ...any) int {
	t.Helper()
	var n int
	if err := env.db.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}
