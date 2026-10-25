package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/your-org/ctrlplane/internal/domain"
)

func TestTenantLifecycleThroughStore(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	tenant, deploy := mustCreateTenant(t, env, "acme-corp")

	if _, _, err := env.st.UpdateTenant(ctx, tenant.ID, 1, "Renamed"); !errors.Is(err, domain.ErrTenantUpdateNotAllowed) {
		t.Fatalf("update while provisioning = %v", err)
	}
	if _, _, err := env.st.DeleteTenant(ctx, tenant.ID); !errors.Is(err, domain.ErrTenantUpdateNotAllowed) {
		t.Fatalf("delete while provisioning = %v", err)
	}

	mustApply(t, env, resultEvent(tenant.ID, deploy.ID, domain.TaskDone))

	updated, updateTask, err := env.st.UpdateTenant(ctx, tenant.ID, 2, "Renamed")
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if updated.Status != domain.TenantUpdating || updated.Version != 3 || updated.Name != "Renamed" {
		t.Fatalf("tenant after update = %+v", updated)
	}
	if updateTask.Type != domain.TaskUpdate || updateTask.Status != domain.TaskAccepted {
		t.Fatalf("update task = %+v", updateTask)
	}

	_, _, err = env.st.UpdateTenant(ctx, tenant.ID, 2, "Stale")
	var conflict *domain.VersionConflictError
	if !errors.As(err, &conflict) || conflict.Expected != 2 || conflict.Current != 3 {
		t.Fatalf("stale version error = %v", err)
	}
	if _, _, err := env.st.UpdateTenant(ctx, tenant.ID, 3, "Again"); !errors.Is(err, domain.ErrTenantUpdateNotAllowed) {
		t.Fatalf("second update while updating = %v", err)
	}

	mustApply(t, env, resultEvent(tenant.ID, updateTask.ID, domain.TaskDone))
	if got, _ := env.st.GetTenant(ctx, tenant.ID); got.Status != domain.TenantActive || got.Version != 4 {
		t.Fatalf("tenant after update result = %+v", got)
	}

	destroyed, destroyTask, err := env.st.DeleteTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if destroyed.Status != domain.TenantDestroying || destroyed.Version != 5 || destroyTask.Type != domain.TaskDestroy {
		t.Fatalf("delete result = %+v / %+v", destroyed, destroyTask)
	}
	if _, _, err := env.st.DeleteTenant(ctx, tenant.ID); !errors.Is(err, domain.ErrTenantUpdateNotAllowed) {
		t.Fatalf("second delete = %v", err)
	}

	mustApply(t, env, resultEvent(tenant.ID, destroyTask.ID, domain.TaskDone))
	if got, _ := env.st.GetTenant(ctx, tenant.ID); got.Status != domain.TenantDestroyed || got.Version != 6 {
		t.Fatalf("tenant after destroy result = %+v", got)
	}
}

func TestFailedTaskMarksTenantFailed(t *testing.T) {
	env := newTestEnv(t)
	tenant, task := mustCreateTenant(t, env, "acme-corp")
	out := mustApply(t, env, resultEvent(tenant.ID, task.ID, domain.TaskFailed))
	if out.Outcome != domain.OutcomeFailed || !out.TenantUpdated {
		t.Fatalf("failed outcome = %+v", out)
	}
	if got, _ := env.st.GetTenant(context.Background(), tenant.ID); got.Status != domain.TenantFailed {
		t.Fatalf("tenant after failure = %+v", got)
	}
}

func TestSlugUniquenessIsEnforcedByTheDatabase(t *testing.T) {
	env := newTestEnv(t)
	mustCreateTenant(t, env, "acme-corp")
	if _, _, err := env.st.CreateTenant(context.Background(), "acme-corp", "Other"); !errors.Is(err, domain.ErrSlugTaken) {
		t.Fatalf("duplicate slug error = %v", err)
	}
	if n := scalarInt(t, env, `SELECT count(*) FROM tenants`); n != 1 {
		t.Fatalf("tenants = %d, want 1", n)
	}
}
