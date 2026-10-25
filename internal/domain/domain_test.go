package domain_test

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
)

func TestEvaluateResult(t *testing.T) {
	cases := []struct {
		current  domain.TaskStatus
		result   domain.TaskStatus
		apply    bool
		next     domain.TaskStatus
		outcome  domain.Outcome
		terminal bool
	}{
		{domain.TaskAccepted, domain.TaskInProgress, true, domain.TaskInProgress, domain.OutcomeInProgress, false},
		{domain.TaskInProgress, domain.TaskInProgress, true, domain.TaskInProgress, domain.OutcomeInProgress, false},
		{domain.TaskAccepted, domain.TaskDone, true, domain.TaskDone, domain.OutcomeDone, true},
		{domain.TaskInProgress, domain.TaskDone, true, domain.TaskDone, domain.OutcomeDone, true},
		{domain.TaskAccepted, domain.TaskFailed, true, domain.TaskFailed, domain.OutcomeFailed, true},
		{domain.TaskInProgress, domain.TaskFailed, true, domain.TaskFailed, domain.OutcomeFailed, true},
		{domain.TaskDone, domain.TaskInProgress, false, domain.TaskDone, domain.OutcomeIgnored, false},
		{domain.TaskDone, domain.TaskFailed, false, domain.TaskDone, domain.OutcomeIgnored, false},
		{domain.TaskFailed, domain.TaskDone, false, domain.TaskFailed, domain.OutcomeIgnored, false},
	}
	for _, c := range cases {
		got, err := domain.EvaluateResult(c.current, c.result)
		if err != nil {
			t.Fatalf("EvaluateResult(%s, %s): %v", c.current, c.result, err)
		}
		if got.Apply != c.apply || got.Next != c.next || got.Outcome != c.outcome || got.Terminal != c.terminal {
			t.Fatalf("EvaluateResult(%s, %s) = %+v, want apply=%v next=%s outcome=%s terminal=%v",
				c.current, c.result, got, c.apply, c.next, c.outcome, c.terminal)
		}
	}
	if _, err := domain.EvaluateResult(domain.TaskAccepted, domain.TaskAccepted); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("accepted result status must be invalid, got %v", err)
	}
	if _, err := domain.EvaluateResult(domain.TaskAccepted, "bogus"); !errors.Is(err, domain.ErrInvalid) {
		t.Fatalf("unknown result status must be invalid, got %v", err)
	}
}

func TestTenantTargetAndSource(t *testing.T) {
	cases := []struct {
		taskType domain.TaskType
		result   domain.TaskStatus
		want     domain.TenantStatus
		ok       bool
	}{
		{domain.TaskDeploy, domain.TaskDone, domain.TenantActive, true},
		{domain.TaskUpdate, domain.TaskDone, domain.TenantActive, true},
		{domain.TaskDestroy, domain.TaskDone, domain.TenantDestroyed, true},
		{domain.TaskDeploy, domain.TaskFailed, domain.TenantFailed, true},
		{domain.TaskUpdate, domain.TaskFailed, domain.TenantFailed, true},
		{domain.TaskDestroy, domain.TaskFailed, domain.TenantFailed, true},
		{domain.TaskDeploy, domain.TaskInProgress, "", false},
		{"bogus", domain.TaskDone, "", false},
	}
	for _, c := range cases {
		got, ok := domain.TenantTarget(c.taskType, c.result)
		if got != c.want || ok != c.ok {
			t.Fatalf("TenantTarget(%s, %s) = %s/%v, want %s/%v", c.taskType, c.result, got, ok, c.want, c.ok)
		}
	}
	src := map[domain.TaskType]domain.TenantStatus{
		domain.TaskDeploy:  domain.TenantProvisioning,
		domain.TaskUpdate:  domain.TenantUpdating,
		domain.TaskDestroy: domain.TenantDestroying,
	}
	for tt, want := range src {
		got, ok := domain.ExpectedSourceStatus(tt)
		if !ok || got != want {
			t.Fatalf("ExpectedSourceStatus(%s) = %s/%v, want %s", tt, got, ok, want)
		}
	}
}

func TestEventsValidate(t *testing.T) {
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	task := domain.Task{
		ID: uuid.New(), TenantID: uuid.New(), Type: domain.TaskDeploy,
		Status: domain.TaskAccepted, CreatedAt: now, UpdatedAt: now,
	}
	ev := domain.NewTaskCreatedEvent(task)
	if err := ev.Validate(); err != nil {
		t.Fatalf("valid task.created rejected: %v", err)
	}
	if ev.Task.CreatedAt != "2026-10-24T12:00:00.000Z" {
		t.Fatalf("event timestamp = %q", ev.Task.CreatedAt)
	}

	bad := ev
	bad.SchemaVersion = 2
	if err := bad.Validate(); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatalf("schema 2: got %v", err)
	}
	bad = ev
	bad.Task.Status = domain.TaskDone
	if err := bad.Validate(); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatalf("non-accepted status: got %v", err)
	}

	result := domain.ResultEvent{
		SchemaVersion: 1, EventID: uuid.New(), EventType: domain.EventTaskResult,
		OccurredAt: "2026-10-24T12:00:01.000Z", TaskID: task.ID, TenantID: task.TenantID,
		Status: domain.TaskDone, Attempt: 1, WorkerID: "w1",
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("valid task.result rejected: %v", err)
	}
	failed := result
	failed.Status = domain.TaskFailed
	if err := failed.Validate(); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatalf("failed without error: got %v", err)
	}
	failed.Error = &domain.ResultError{Code: "provisioning_failed", Message: "boom"}
	if err := failed.Validate(); err != nil {
		t.Fatalf("failed with error rejected: %v", err)
	}
	withErr := result
	withErr.Error = &domain.ResultError{Code: "x"}
	if err := withErr.Validate(); !errors.Is(err, domain.ErrInvalidEvent) {
		t.Fatalf("error on done: got %v", err)
	}
}
