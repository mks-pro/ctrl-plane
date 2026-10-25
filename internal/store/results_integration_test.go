package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
)

func TestApplyResultIsIdempotentAndGuarded(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	tenant, task := mustCreateTenant(t, env, "acme-corp")

	progress := resultEvent(tenant.ID, task.ID, domain.TaskInProgress)
	out := mustApply(t, env, progress)
	if out.Duplicate || out.TenantUpdated || out.Outcome != domain.OutcomeInProgress {
		t.Fatalf("first in_progress outcome = %+v", out)
	}
	got, err := env.st.GetTask(ctx, task.ID)
	if err != nil || got.Status != domain.TaskInProgress {
		t.Fatalf("task after in_progress = %+v, %v", got, err)
	}
	if got, _ := env.st.GetTenant(ctx, tenant.ID); got.Status != domain.TenantProvisioning {
		t.Fatalf("tenant must stay provisioning during in_progress: %+v", got)
	}

	// A redelivered event must be recognised as a duplicate and change nothing.
	if out := mustApply(t, env, progress); !out.Duplicate {
		t.Fatalf("redelivery = %+v, want duplicate", out)
	}
	if n := scalarInt(t, env, `SELECT count(*) FROM processed_updates`); n != 1 {
		t.Fatalf("processed_updates rows = %d, want 1", n)
	}

	done := resultEvent(tenant.ID, task.ID, domain.TaskDone)
	out = mustApply(t, env, done)
	if out.Duplicate || !out.TenantUpdated || out.Outcome != domain.OutcomeDone {
		t.Fatalf("done outcome = %+v", out)
	}
	gotTenant, err := env.st.GetTenant(ctx, tenant.ID)
	if err != nil {
		t.Fatalf("get tenant: %v", err)
	}
	if gotTenant.Status != domain.TenantActive || gotTenant.Version != 2 {
		t.Fatalf("tenant after deploy = %+v", gotTenant)
	}
	if got, _ := env.st.GetTask(ctx, task.ID); got.Status != domain.TaskDone {
		t.Fatalf("task after done = %+v", got)
	}

	// A late event for an already terminal task is recorded but never applied.
	late := mustApply(t, env, resultEvent(tenant.ID, task.ID, domain.TaskFailed))
	if late.Duplicate || late.TenantUpdated || late.Outcome != domain.OutcomeIgnored {
		t.Fatalf("late event outcome = %+v", late)
	}
	if got, _ := env.st.GetTask(ctx, task.ID); got.Status != domain.TaskDone {
		t.Fatalf("late event must not change the task: %+v", got)
	}
	if got, _ := env.st.GetTenant(ctx, tenant.ID); got.Status != domain.TenantActive || got.Version != 2 {
		t.Fatalf("late event must not change the tenant: %+v", got)
	}
}

func TestApplyResultRejectsBadEvents(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	tenant, task := mustCreateTenant(t, env, "acme-corp")

	ev := resultEvent(tenant.ID, task.ID, domain.TaskDone)
	ev.TenantID = uuid.New()
	if _, err := env.st.ApplyResult(ctx, ev); !errors.Is(err, domain.ErrTenantMismatch) {
		t.Fatalf("tenant mismatch error = %v", err)
	}

	ev = resultEvent(tenant.ID, uuid.New(), domain.TaskDone)
	if _, err := env.st.ApplyResult(ctx, ev); !errors.Is(err, domain.ErrTaskNotFound) {
		t.Fatalf("unknown task error = %v", err)
	}

	ev = resultEvent(tenant.ID, task.ID, domain.TaskAccepted)
	if _, err := env.st.ApplyResult(ctx, ev); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("accepted result error = %v", err)
	}

	// None of the rejected claims may leave a processed_updates row behind.
	if n := scalarInt(t, env, `SELECT count(*) FROM processed_updates`); n != 0 {
		t.Fatalf("processed_updates rows = %d, want 0", n)
	}
	if got, _ := env.st.GetTask(ctx, task.ID); got.Status != domain.TaskAccepted {
		t.Fatalf("task changed by rejected events: %+v", got)
	}
}
