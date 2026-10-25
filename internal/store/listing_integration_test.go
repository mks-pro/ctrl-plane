package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/your-org/ctrlplane/internal/domain"
	"github.com/your-org/ctrlplane/internal/store"
)

func TestOutboxStopsAtFirstPublishFailure(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	mustCreateTenant(t, env, "alpha-tenant")
	mustCreateTenant(t, env, "beta-tenant")
	mustCreateTenant(t, env, "gamma-tenant")

	var seen []store.OutboxEvent
	publish := func(_ context.Context, e store.OutboxEvent) error {
		seen = append(seen, e)
		if len(seen) == 2 {
			return errors.New("broker down")
		}
		return nil
	}
	n, full, err := env.st.ProcessOutboxBatch(ctx, 10, publish)
	if err == nil || !strings.Contains(err.Error(), "broker down") {
		t.Fatalf("batch error = %v", err)
	}
	if n != 1 || full {
		t.Fatalf("batch = %d/%v, want 1/false", n, full)
	}
	if len(seen) != 2 {
		t.Fatalf("publish attempts = %d, want 2 (no overtaking)", len(seen))
	}
	if got := scalarInt(t, env, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); got != 2 {
		t.Fatalf("unpublished rows = %d, want 2", got)
	}
	if got := scalarInt(t, env, `SELECT attempts FROM outbox_events WHERE id = $1`, seen[1].ID); got != 1 {
		t.Fatalf("failed row attempts = %d, want 1", got)
	}
	var lastErr string
	if err := env.db.QueryRow(ctx, `SELECT last_error FROM outbox_events WHERE id = $1`, seen[1].ID).Scan(&lastErr); err != nil {
		t.Fatalf("select last_error: %v", err)
	}
	if !strings.Contains(lastErr, "broker down") {
		t.Fatalf("last_error = %q", lastErr)
	}

	// The retry publishes the failed row first, then the untouched third row.
	failedID := seen[1].ID
	seen = nil
	publish = func(_ context.Context, e store.OutboxEvent) error {
		seen = append(seen, e)
		return nil
	}
	if n, _, err := env.st.ProcessOutboxBatch(ctx, 10, publish); err != nil || n != 2 {
		t.Fatalf("retry batch = %d/%v", n, err)
	}
	if len(seen) != 2 || seen[0].ID != failedID {
		t.Fatalf("retry order = %+v, want first %s", seen, failedID)
	}
	if got := scalarInt(t, env, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); got != 0 {
		t.Fatalf("unpublished rows after retry = %d", got)
	}
}

func TestListingPaginationAndFilters(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	created := make([]domain.Tenant, 0, 3)
	for _, slug := range []string{"alpha-tenant", "beta-tenant", "gamma-tenant"} {
		tenant, _ := mustCreateTenant(t, env, slug)
		created = append(created, tenant)
	}

	page, err := env.st.ListTenants(ctx, store.TenantFilter{Limit: 2})
	if err != nil {
		t.Fatalf("list tenants: %v", err)
	}
	if len(page.Items) != 2 || page.Next == nil {
		t.Fatalf("first page = %+v", page)
	}
	next, err := env.st.ListTenants(ctx, store.TenantFilter{Limit: 2, After: page.Next})
	if err != nil {
		t.Fatalf("second page: %v", err)
	}
	if len(next.Items) != 1 || next.Next != nil {
		t.Fatalf("second page = %+v", next)
	}
	seen := map[string]bool{}
	for _, tenant := range append(page.Items, next.Items...) {
		seen[tenant.ID.String()] = true
	}
	if len(seen) != len(created) {
		t.Fatalf("pagination returned %d distinct tenants, want %d", len(seen), len(created))
	}

	slug := "beta-tenant"
	filtered, err := env.st.ListTenants(ctx, store.TenantFilter{Slug: &slug})
	if err != nil || len(filtered.Items) != 1 || filtered.Items[0].Slug != slug {
		t.Fatalf("slug filter = %+v, %v", filtered, err)
	}
	provisioning := domain.TenantProvisioning
	byStatus, err := env.st.ListTenants(ctx, store.TenantFilter{Status: &provisioning})
	if err != nil || len(byStatus.Items) != 3 {
		t.Fatalf("status filter = %d, %v", len(byStatus.Items), err)
	}

	tenantID := created[0].ID
	taskType := domain.TaskDeploy
	tasks, err := env.st.ListTasks(ctx, store.TaskFilter{TenantID: &tenantID, Type: &taskType})
	if err != nil || len(tasks.Items) != 1 {
		t.Fatalf("task filter = %+v, %v", tasks, err)
	}
	if tasks.Items[0].TenantID != tenantID || tasks.Items[0].Type != domain.TaskDeploy {
		t.Fatalf("unexpected task = %+v", tasks.Items[0])
	}
}
