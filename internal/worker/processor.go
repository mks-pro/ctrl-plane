// Package worker simulates task execution. It never touches the database: it
// consumes task.created, sleeps, and publishes task.result events.
package worker

import (
	"context"
	"encoding/binary"
	"hash/fnv"
	"time"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
)

// Options tunes the simulation. Rates are checked by config.ValidateWorker.
type Options struct {
	WorkerID      string
	FailRate      float64
	DuplicateRate float64
	MinDelay      time.Duration
	MaxDelay      time.Duration
	Seed          int64
}

// Processor produces deterministic results for a task id.
type Processor struct {
	opts Options
	now  func() time.Time
}

// NewProcessor builds a processor; now is injectable for tests.
func NewProcessor(opts Options) *Processor {
	return &Processor{opts: opts, now: time.Now}
}

// Plan is the deterministic decision for one task, exposed for tests.
type Plan struct {
	Duplicate bool
	Fail      bool
	Delay     time.Duration
}

// Decide derives fail/duplicate/delay from (seed, task id) only, so redelivery
// of the same task makes the same decision.
func (p *Processor) Decide(taskID uuid.UUID) Plan {
	duplicate := p.roll(taskID, "duplicate") < p.opts.DuplicateRate
	fail := !duplicate && p.roll(taskID, "outcome") < p.opts.FailRate
	span := p.opts.MaxDelay - p.opts.MinDelay
	delay := p.opts.MinDelay
	if span > 0 {
		delay += time.Duration(p.roll(taskID, "delay") * float64(span))
	}
	return Plan{Duplicate: duplicate, Fail: fail, Delay: delay}
}

// Process simulates the work and returns the events to publish, in order.
// A duplicate injection repeats the identical done event (same event_id and
// payload) so the API-side dedupe is genuinely exercised.
func (p *Processor) Process(ctx context.Context, ev domain.TaskCreatedEvent) ([]domain.ResultEvent, error) {
	taskID := ev.Task.ID
	plan := p.Decide(taskID)
	started := p.now().UTC()

	if !sleepCtx(ctx, plan.Delay) {
		return nil, ctx.Err()
	}
	finished := p.now().UTC()

	if plan.Fail {
		return []domain.ResultEvent{
			p.result(ev, domain.TaskFailed, finished, &domain.ResultError{
				Code:    "provisioning_failed",
				Message: "simulated provisioning failure",
			}),
		}, nil
	}

	events := []domain.ResultEvent{
		p.result(ev, domain.TaskInProgress, started, nil),
		p.result(ev, domain.TaskDone, finished, nil),
	}
	if plan.Duplicate {
		events = append(events, events[len(events)-1])
	}
	return events, nil
}

func (p *Processor) result(ev domain.TaskCreatedEvent, status domain.TaskStatus, at time.Time, rerr *domain.ResultError) domain.ResultEvent {
	return domain.ResultEvent{
		SchemaVersion: domain.SchemaVersion,
		EventID:       resultEventID(ev.Task.ID, status),
		EventType:     domain.EventTaskResult,
		OccurredAt:    domain.FormatTime(at),
		TaskID:        ev.Task.ID,
		TenantID:      ev.Task.TenantID,
		Status:        status,
		Attempt:       1,
		WorkerID:      p.opts.WorkerID,
		Error:         rerr,
	}
}

// resultEventID is stable for (task, status): a redelivered task republishes the
// same event ids, which is exactly what the consumer's dedupe keys on.
func resultEventID(taskID uuid.UUID, status domain.TaskStatus) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("ctrlplane/result/"+taskID.String()+"/"+string(status)))
}

// roll maps (seed, task id, purpose) to a stable value in [0,1).
func (p *Processor) roll(taskID uuid.UUID, purpose string) float64 {
	h := fnv.New64a()
	var seed [8]byte
	binary.LittleEndian.PutUint64(seed[:], uint64(p.opts.Seed))
	_, _ = h.Write(seed[:])
	_, _ = h.Write([]byte(taskID.String()))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(purpose))
	return float64(h.Sum64()>>11) / float64(uint64(1)<<53)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
