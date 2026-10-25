package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TaskType is the kind of work a task performs on its tenant.
type TaskType string

const (
	TaskDeploy  TaskType = "deploy"
	TaskUpdate  TaskType = "update"
	TaskDestroy TaskType = "destroy"
)

var taskTypes = []TaskType{TaskDeploy, TaskUpdate, TaskDestroy}

// Valid reports whether t is a known task type.
func (t TaskType) Valid() bool {
	for _, v := range taskTypes {
		if t == v {
			return true
		}
	}
	return false
}

// ParseTaskType validates a type coming from a query parameter.
func ParseTaskType(s string) (TaskType, error) {
	t := TaskType(s)
	if !t.Valid() {
		return "", Invalid("type", fmt.Sprintf("unknown task type %q", s))
	}
	return t, nil
}

// TaskStatus is the state of a single asynchronous operation.
type TaskStatus string

const (
	TaskAccepted   TaskStatus = "accepted"
	TaskInProgress TaskStatus = "in_progress"
	TaskDone       TaskStatus = "done"
	TaskFailed     TaskStatus = "failed"
)

var taskStatuses = []TaskStatus{TaskAccepted, TaskInProgress, TaskDone, TaskFailed}

// Valid reports whether s is a known task status.
func (s TaskStatus) Valid() bool {
	for _, v := range taskStatuses {
		if s == v {
			return true
		}
	}
	return false
}

// ParseTaskStatus validates a status coming from a query parameter.
func ParseTaskStatus(s string) (TaskStatus, error) {
	st := TaskStatus(s)
	if !st.Valid() {
		return "", Invalid("status", fmt.Sprintf("unknown task status %q", s))
	}
	return st, nil
}

// IsTerminal reports whether the task can never change again.
func (s TaskStatus) IsTerminal() bool { return s == TaskDone || s == TaskFailed }

// Outcome is what the consumer records in processed_updates for an event.
type Outcome string

const (
	OutcomeInProgress Outcome = "in_progress"
	OutcomeDone       Outcome = "done"
	OutcomeFailed     Outcome = "failed"
	OutcomeIgnored    Outcome = "ignored"
)

// Task is one asynchronous operation on a tenant. Its ID doubles as the outbox
// event id, which is why it is generated before the insert.
type Task struct {
	ID        uuid.UUID
	TenantID  uuid.UUID
	Type      TaskType
	Status    TaskStatus
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Transition is the decision the consumer makes for one result event.
type Transition struct {
	Apply    bool
	Next     TaskStatus
	Outcome  Outcome
	Terminal bool
}

// EvaluateResult applies the task state machine. Unknown result statuses are
// invalid input; transitions out of a terminal state are ignored, never applied.
// The first terminal outcome wins.
func EvaluateResult(current, result TaskStatus) (Transition, error) {
	if !result.Valid() || result == TaskAccepted {
		return Transition{}, Invalid("status", "result status must be in_progress, done or failed")
	}
	switch current {
	case TaskDone, TaskFailed:
		return Transition{Apply: false, Next: current, Outcome: OutcomeIgnored}, nil
	case TaskAccepted, TaskInProgress:
		if result == TaskInProgress {
			return Transition{Apply: true, Next: TaskInProgress, Outcome: OutcomeInProgress}, nil
		}
		return Transition{Apply: true, Next: result, Outcome: Outcome(result), Terminal: true}, nil
	default:
		return Transition{}, fmt.Errorf("%w: unknown task status %q", ErrInvalidEvent, current)
	}
}

// TenantTarget maps a terminal task outcome to the tenant status it produces.
func TenantTarget(t TaskType, result TaskStatus) (TenantStatus, bool) {
	if result == TaskFailed {
		return TenantFailed, true
	}
	if result != TaskDone {
		return "", false
	}
	switch t {
	case TaskDeploy, TaskUpdate:
		return TenantActive, true
	case TaskDestroy:
		return TenantDestroyed, true
	default:
		return "", false
	}
}

// ExpectedSourceStatus is the tenant status a task of this type must have been
// started from; the consumer guards its tenant update with it.
func ExpectedSourceStatus(t TaskType) (TenantStatus, bool) {
	switch t {
	case TaskDeploy:
		return TenantProvisioning, true
	case TaskUpdate:
		return TenantUpdating, true
	case TaskDestroy:
		return TenantDestroying, true
	default:
		return "", false
	}
}
