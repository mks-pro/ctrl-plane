package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// SchemaVersion is the only message schema this build produces and accepts.
// Unknown major versions are poison, not silently dropped.
const SchemaVersion = 1

// Event types (and routing keys).
const (
	EventTaskCreated = "task.created"
	EventTaskResult  = "task.result"
)

// EventTask is the task snapshot embedded in a task.created event. Timestamps
// are strings so the wire format is exactly the documented millisecond layout.
type EventTask struct {
	ID        uuid.UUID  `json:"id"`
	TenantID  uuid.UUID  `json:"tenant_id"`
	Type      TaskType   `json:"type"`
	Status    TaskStatus `json:"status"`
	CreatedAt string     `json:"created_at"`
	UpdatedAt string     `json:"updated_at"`
}

// TaskCreatedEvent is published by the outbox relay after commit.
type TaskCreatedEvent struct {
	SchemaVersion int       `json:"schema_version"`
	ID            uuid.UUID `json:"id"`
	Type          string    `json:"type"`
	OccurredAt    string    `json:"occurred_at"`
	Task          EventTask `json:"task"`
}

// NewTaskCreatedEvent builds the outbound event for a freshly inserted task.
func NewTaskCreatedEvent(t Task) TaskCreatedEvent {
	return TaskCreatedEvent{
		SchemaVersion: SchemaVersion,
		ID:            t.ID,
		Type:          EventTaskCreated,
		OccurredAt:    FormatTime(t.CreatedAt),
		Task: EventTask{
			ID:        t.ID,
			TenantID:  t.TenantID,
			Type:      t.Type,
			Status:    t.Status,
			CreatedAt: FormatTime(t.CreatedAt),
			UpdatedAt: FormatTime(t.UpdatedAt),
		},
	}
}

// Validate checks the envelope a worker received. Failures are permanent.
func (e *TaskCreatedEvent) Validate() error {
	if e.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: unsupported schema_version %d", ErrInvalidEvent, e.SchemaVersion)
	}
	if e.Type != EventTaskCreated {
		return fmt.Errorf("%w: unexpected type %q", ErrInvalidEvent, e.Type)
	}
	if e.ID == uuid.Nil {
		return fmt.Errorf("%w: id is required", ErrInvalidEvent)
	}
	if e.Task.ID != e.ID {
		return fmt.Errorf("%w: task.id must equal id", ErrInvalidEvent)
	}
	if e.Task.TenantID == uuid.Nil {
		return fmt.Errorf("%w: task.tenant_id is required", ErrInvalidEvent)
	}
	if !e.Task.Type.Valid() {
		return fmt.Errorf("%w: unknown task.type %q", ErrInvalidEvent, e.Task.Type)
	}
	if e.Task.Status != TaskAccepted {
		return fmt.Errorf("%w: task.status must be accepted, got %q", ErrInvalidEvent, e.Task.Status)
	}
	if _, err := time.Parse(time.RFC3339Nano, e.OccurredAt); err != nil {
		return fmt.Errorf("%w: occurred_at is not RFC 3339: %v", ErrInvalidEvent, err)
	}
	return nil
}

// ResultError is present only on failed results.
type ResultError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ResultEvent is published by the worker and consumed by the API.
type ResultEvent struct {
	SchemaVersion int          `json:"schema_version"`
	EventID       uuid.UUID    `json:"event_id"`
	EventType     string       `json:"event_type"`
	OccurredAt    string       `json:"occurred_at"`
	TaskID        uuid.UUID    `json:"task_id"`
	TenantID      uuid.UUID    `json:"tenant_id"`
	Status        TaskStatus   `json:"status"`
	Attempt       int          `json:"attempt"`
	WorkerID      string       `json:"worker_id"`
	Error         *ResultError `json:"error,omitempty"`
}

// Validate enforces the inbound contract. Every failure here is permanent and
// results in dead-lettering rather than an endless retry.
func (e *ResultEvent) Validate() error {
	if e.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: unsupported schema_version %d", ErrInvalidEvent, e.SchemaVersion)
	}
	if e.EventType != EventTaskResult {
		return fmt.Errorf("%w: unexpected event_type %q", ErrInvalidEvent, e.EventType)
	}
	if e.EventID == uuid.Nil {
		return fmt.Errorf("%w: event_id is required", ErrInvalidEvent)
	}
	if e.TaskID == uuid.Nil {
		return fmt.Errorf("%w: task_id is required", ErrInvalidEvent)
	}
	if e.TenantID == uuid.Nil {
		return fmt.Errorf("%w: tenant_id is required", ErrInvalidEvent)
	}
	if !e.Status.Valid() || e.Status == TaskAccepted {
		return fmt.Errorf("%w: status must be in_progress, done or failed", ErrInvalidEvent)
	}
	if _, err := time.Parse(time.RFC3339Nano, e.OccurredAt); err != nil {
		return fmt.Errorf("%w: occurred_at is not RFC 3339: %v", ErrInvalidEvent, err)
	}
	if e.Status == TaskFailed {
		if e.Error == nil || e.Error.Code == "" {
			return fmt.Errorf("%w: failed results must carry error.code", ErrInvalidEvent)
		}
	} else if e.Error != nil {
		return fmt.Errorf("%w: error is only allowed when status=failed", ErrInvalidEvent)
	}
	return nil
}
