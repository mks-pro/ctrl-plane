package api_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/your-org/ctrlplane/internal/api"
	"github.com/your-org/ctrlplane/internal/domain"
	"github.com/your-org/ctrlplane/internal/store"
	"github.com/your-org/ctrlplane/migrations"
)

// realEnv exposes the HTTP layer plus the real store for integration tests.
type realEnv struct {
	h  http.Handler
	st *store.Store
	db *pgxpool.Pool
}

const apiTestSchema = "ctrlplane_api_it"

// newRealEnv wires the HTTP layer to a real store. Tests are skipped unless
// TEST_DATABASE_URL points at a disposable database (a private schema is
// recreated on every call).
func newRealEnv(t *testing.T) *realEnv {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run concurrency integration tests")
	}
	dsn = scopedDSN(t, dsn, apiTestSchema)
	ctx := context.Background()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(db.Close)
	all, err := migrations.All()
	if err != nil {
		t.Fatalf("migrations: %v", err)
	}
	for _, m := range all {
		if _, err := db.Exec(ctx, m.Up); err != nil {
			t.Fatalf("apply %s: %v", m.Version, err)
		}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	st, err := store.Open(ctx, dsn, 16, "ctrlplane.events", domain.SystemClock{}, logger)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)
	return &realEnv{h: api.New(st, fakeBroker{open: true}, domain.SystemClock{}, logger).Routes(), st: st, db: db}
}

// scopedDSN gives this package a private schema so `go test ./...` can run
// packages in parallel against one database.
func scopedDSN(t *testing.T, dsn, schema string) string {
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

// TestConcurrentCreateSameSlug proves the unique index, not application code,
// arbitrates the race (AC-3): exactly one 201, the rest 409.
func TestConcurrentCreateSameSlug(t *testing.T) {
	env := newRealEnv(t)
	h := env.h
	const workers = 20
	codes := make([]int, workers)
	bodies := make([]string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := post(t, h, "/v1/tenants", `{"slug":"race-corp","name":"Racer"}`)
			codes[i] = rec.Code
			bodies[i] = rec.Body.String()
		}(i)
	}
	wg.Wait()

	created, conflict := 0, 0
	for i, code := range codes {
		switch code {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			conflict++
			if !strings.Contains(bodies[i], "tenant_already_exists") {
				t.Fatalf("unexpected conflict body: %s", bodies[i])
			}
		default:
			t.Fatalf("unexpected status %d: %s", code, bodies[i])
		}
	}
	if created != 1 || conflict != workers-1 {
		t.Fatalf("created=%d conflict=%d", created, conflict)
	}
}

// TestConcurrentPatchSameVersion proves optimistic locking (AC-4): 50 PATCHes
// carrying the same version produce exactly one 202, the rest 409, and the
// version moves exactly once.
func TestConcurrentPatchSameVersion(t *testing.T) {
	env := newRealEnv(t)
	ctx := context.Background()

	rec := post(t, env.h, "/v1/tenants", `{"slug":"race-corp","name":"Racer"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	var created mutationJSON
	decodeInto(t, rec, &created)
	tenantID := created.Tenant.ID
	taskID, err := uuid.Parse(created.Task.ID)
	if err != nil {
		t.Fatalf("task id: %v", err)
	}

	// Drive the deploy task to done so the tenant becomes active and PATCHable.
	ev := domain.ResultEvent{
		SchemaVersion: domain.SchemaVersion,
		EventID:       uuid.New(),
		EventType:     domain.EventTaskResult,
		OccurredAt:    domain.FormatTime(domain.SystemClock{}.Now()),
		TaskID:        taskID,
		TenantID:      uuid.MustParse(tenantID),
		Status:        domain.TaskDone,
		Attempt:       1,
		WorkerID:      "worker-test",
	}
	if _, err := env.st.ApplyResult(ctx, ev); err != nil {
		t.Fatalf("apply deploy result: %v", err)
	}
	active, err := env.st.GetTenant(ctx, uuid.MustParse(tenantID))
	if err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	if active.Status != domain.TenantActive {
		t.Fatalf("tenant status = %s, want active", active.Status)
	}
	base := active.Version
	body := fmt.Sprintf(`{"version":%d,"name":"Racer"}`, base)

	const workers = 50
	codes := make([]int, workers)
	bodies := make([]string, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			rec := patch(t, env.h, "/v1/tenants/"+tenantID, body)
			codes[i] = rec.Code
			bodies[i] = rec.Body.String()
		}(i)
	}
	wg.Wait()

	accepted, conflicts := 0, 0
	for i, code := range codes {
		switch code {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicts++
			if !strings.Contains(bodies[i], "tenant_version_conflict") {
				t.Fatalf("unexpected conflict body: %s", bodies[i])
			}
		default:
			t.Fatalf("unexpected status %d: %s", code, bodies[i])
		}
	}
	if accepted != 1 || conflicts != workers-1 {
		t.Fatalf("accepted=%d conflicts=%d", accepted, conflicts)
	}

	var version int
	var status string
	if err := env.db.QueryRow(ctx, `SELECT version, status FROM tenants WHERE id = $1`, tenantID).
		Scan(&version, &status); err != nil {
		t.Fatalf("select tenant: %v", err)
	}
	if version != base+1 || status != string(domain.TenantUpdating) {
		t.Fatalf("tenant after race = version %d status %s, want %d/updating", version, status, base+1)
	}
}
