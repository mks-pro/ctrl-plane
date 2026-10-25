package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/broker"
	"github.com/your-org/ctrlplane/internal/domain"
)

type fakeAck struct {
	acks    int
	nacks   int
	requeue bool
}

func (a *fakeAck) Ack(uint64, bool) error { a.acks++; return nil }

func (a *fakeAck) Nack(_ uint64, _ bool, requeue bool) error {
	a.nacks++
	a.requeue = requeue
	return nil
}

func (a *fakeAck) Reject(_ uint64, requeue bool) error {
	a.nacks++
	a.requeue = requeue
	return nil
}

type fakePublisher struct {
	msgs []broker.Message
	err  error
}

func (p *fakePublisher) Publish(_ context.Context, m broker.Message) error {
	if p.err != nil {
		return p.err
	}
	p.msgs = append(p.msgs, m)
	return nil
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testTaskCreated() domain.TaskCreatedEvent {
	now := time.Now().UTC()
	return domain.NewTaskCreatedEvent(domain.Task{
		ID:        uuid.New(),
		TenantID:  uuid.New(),
		Type:      domain.TaskDeploy,
		Status:    domain.TaskAccepted,
		CreatedAt: now,
		UpdatedAt: now,
	})
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func fastProcessor(workerID string) *Processor {
	return NewProcessor(Options{WorkerID: workerID, Seed: 7})
}

func newTestConsumer(proc *Processor, pub Publisher, maxAttempts int) *Consumer {
	return NewConsumer("amqp://unused", broker.DefaultTopology(), "q", "tag", 1, maxAttempts,
		time.Millisecond, 5*time.Millisecond, proc, pub, quietLogger())
}

func newDelivery(body []byte, ack amqp.Acknowledger) amqp.Delivery {
	return amqp.Delivery{Acknowledger: ack, Body: body, ContentType: "application/json"}
}

func TestDecodeTaskCreated(t *testing.T) {
	valid := testTaskCreated()
	wrongSchema := testTaskCreated()
	wrongSchema.SchemaVersion = 2
	wrongType := testTaskCreated()
	wrongType.Type = "tenant.created"
	mismatchedID := testTaskCreated()
	mismatchedID.Task.ID = uuid.New()
	wrongStatus := testTaskCreated()
	wrongStatus.Task.Status = domain.TaskDone

	tests := []struct {
		name string
		body []byte
		ok   bool
	}{
		{"valid", mustJSON(t, valid), true},
		{"empty", nil, false},
		{"too large", make([]byte, MaxBodyBytes+1), false},
		{"invalid json", []byte("{"), false},
		{"unsupported schema", mustJSON(t, wrongSchema), false},
		{"unexpected type", mustJSON(t, wrongType), false},
		{"task id mismatch", mustJSON(t, mismatchedID), false},
		{"status must be accepted", mustJSON(t, wrongStatus), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeTaskCreated(tc.body)
			if tc.ok {
				if err != nil {
					t.Fatalf("DecodeTaskCreated: %v", err)
				}
				if got.Task.ID != valid.Task.ID {
					t.Fatalf("task id = %s, want %s", got.Task.ID, valid.Task.ID)
				}
				return
			}
			if !errors.Is(err, domain.ErrInvalidEvent) {
				t.Fatalf("err = %v, want ErrInvalidEvent", err)
			}
		})
	}
}

func TestDecideIsDeterministic(t *testing.T) {
	taskID := uuid.New()
	opts := Options{WorkerID: "w", Seed: 42, FailRate: 0.5, DuplicateRate: 0.3,
		MinDelay: 10 * time.Millisecond, MaxDelay: 200 * time.Millisecond}
	first := NewProcessor(opts).Decide(taskID)
	second := NewProcessor(opts).Decide(taskID)
	if first != second {
		t.Fatalf("Decide not deterministic: %+v vs %+v", first, second)
	}
	if first.Delay < opts.MinDelay || first.Delay > opts.MaxDelay {
		t.Fatalf("delay %v outside [%v,%v]", first.Delay, opts.MinDelay, opts.MaxDelay)
	}
	// A duplicate task never also fails, so every task yields one terminal path.
	if first.Duplicate && first.Fail {
		t.Fatal("duplicate and fail are mutually exclusive")
	}
}

func TestProcessEventStream(t *testing.T) {
	ev := testTaskCreated()

	t.Run("success", func(t *testing.T) {
		proc := fastProcessor("worker-a")
		events, err := proc.Process(context.Background(), ev)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(events) != 2 {
			t.Fatalf("len(events) = %d, want 2", len(events))
		}
		if events[0].Status != domain.TaskInProgress || events[1].Status != domain.TaskDone {
			t.Fatalf("statuses = %s,%s", events[0].Status, events[1].Status)
		}
		for _, e := range events {
			if err := e.Validate(); err != nil {
				t.Fatalf("event invalid: %v", err)
			}
			if e.TaskID != ev.Task.ID || e.TenantID != ev.Task.TenantID {
				t.Fatalf("event ids do not match the task: %+v", e)
			}
			if e.WorkerID != "worker-a" {
				t.Fatalf("worker id = %q", e.WorkerID)
			}
		}

		// A redelivery must republish identical event ids for dedupe to work.
		again, err := proc.Process(context.Background(), ev)
		if err != nil {
			t.Fatalf("Process again: %v", err)
		}
		for i := range events {
			if events[i].EventID != again[i].EventID {
				t.Fatalf("event %d id changed across redelivery", i)
			}
		}
	})

	t.Run("failure", func(t *testing.T) {
		proc := NewProcessor(Options{WorkerID: "w", Seed: 1, FailRate: 1})
		events, err := proc.Process(context.Background(), ev)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(events) != 1 || events[0].Status != domain.TaskFailed {
			t.Fatalf("events = %+v, want one failed event", events)
		}
		if events[0].Error == nil || events[0].Error.Code == "" {
			t.Fatalf("failed event missing error code: %+v", events[0])
		}
	})

	t.Run("duplicate injection", func(t *testing.T) {
		proc := NewProcessor(Options{WorkerID: "w", Seed: 1, DuplicateRate: 1})
		events, err := proc.Process(context.Background(), ev)
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(events) != 3 {
			t.Fatalf("len(events) = %d, want 3", len(events))
		}
		if events[1].EventID != events[2].EventID || events[1].OccurredAt != events[2].OccurredAt {
			t.Fatalf("duplicate event differs: %+v vs %+v", events[1], events[2])
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		proc := NewProcessor(Options{WorkerID: "w", Seed: 1, MinDelay: time.Second, MaxDelay: time.Second})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := proc.Process(ctx, ev); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
	})
}

func TestHandlePublishesAndAcks(t *testing.T) {
	ack := &fakeAck{}
	pub := &fakePublisher{}
	ev := testTaskCreated()
	c := newTestConsumer(fastProcessor("worker-b"), pub, 5)

	c.handle(context.Background(), newDelivery(mustJSON(t, ev), ack))

	if ack.acks != 1 || ack.nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want 1/0", ack.acks, ack.nacks)
	}
	if len(pub.msgs) != 2 {
		t.Fatalf("published %d messages, want 2", len(pub.msgs))
	}
	topo := broker.DefaultTopology()
	for i, m := range pub.msgs {
		if m.Exchange != topo.Exchange || m.RoutingKey != broker.RoutingTaskResult {
			t.Fatalf("msg %d routed to %s/%s", i, m.Exchange, m.RoutingKey)
		}
		if m.Type != domain.EventTaskResult || m.AppID != "ctrlplane-worker" {
			t.Fatalf("msg %d type/appid = %s/%s", i, m.Type, m.AppID)
		}
		if got := m.Headers["schema_version"]; got != int32(domain.SchemaVersion) {
			t.Fatalf("msg %d schema_version header = %v", i, got)
		}
		var decoded domain.ResultEvent
		if err := json.Unmarshal(m.Body, &decoded); err != nil {
			t.Fatalf("msg %d body: %v", i, err)
		}
		if decoded.EventID.String() != m.MessageID {
			t.Fatalf("msg %d message id %s != event id %s", i, m.MessageID, decoded.EventID)
		}
	}
}

func TestHandlePublishFailureRetriesThenDeadLetters(t *testing.T) {
	ev := testTaskCreated()
	body := mustJSON(t, ev)

	t.Run("retry", func(t *testing.T) {
		ack := &fakeAck{}
		pub := &fakePublisher{err: errors.New("broker down")}
		newTestConsumer(fastProcessor("w"), pub, 5).handle(context.Background(), newDelivery(body, ack))
		if ack.nacks != 1 || !ack.requeue {
			t.Fatalf("nacks=%d requeue=%v, want 1/true", ack.nacks, ack.requeue)
		}
	})

	t.Run("dead letter", func(t *testing.T) {
		ack := &fakeAck{}
		pub := &fakePublisher{err: errors.New("broker down")}
		newTestConsumer(fastProcessor("w"), pub, 1).handle(context.Background(), newDelivery(body, ack))
		if ack.nacks != 1 || ack.requeue {
			t.Fatalf("nacks=%d requeue=%v, want 1/false", ack.nacks, ack.requeue)
		}
	})
}

func TestHandlePoisonIsDeadLettered(t *testing.T) {
	ack := &fakeAck{}
	pub := &fakePublisher{}
	newTestConsumer(fastProcessor("w"), pub, 5).handle(context.Background(), newDelivery([]byte(`[]`), ack))
	if ack.nacks != 1 || ack.requeue {
		t.Fatalf("nacks=%d requeue=%v, want 1/false", ack.nacks, ack.requeue)
	}
	if len(pub.msgs) != 0 {
		t.Fatalf("published %d messages for poison", len(pub.msgs))
	}
}
