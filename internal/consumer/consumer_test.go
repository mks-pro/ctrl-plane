package consumer

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
	"github.com/your-org/ctrlplane/internal/store"
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

type fakeStore struct {
	out   store.ResultOutcome
	err   error
	calls int
}

func (f *fakeStore) ApplyResult(context.Context, domain.ResultEvent) (store.ResultOutcome, error) {
	f.calls++
	return f.out, f.err
}

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func resultEvent(status domain.TaskStatus) domain.ResultEvent {
	ev := domain.ResultEvent{
		SchemaVersion: domain.SchemaVersion,
		EventID:       uuid.New(),
		EventType:     domain.EventTaskResult,
		OccurredAt:    domain.FormatTime(time.Now()),
		TaskID:        uuid.New(),
		TenantID:      uuid.New(),
		Status:        status,
		Attempt:       1,
		WorkerID:      "worker-test",
	}
	if status == domain.TaskFailed {
		ev.Error = &domain.ResultError{Code: "provisioning_failed", Message: "boom"}
	}
	return ev
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func newDelivery(body []byte, ack amqp.Acknowledger) amqp.Delivery {
	return amqp.Delivery{Acknowledger: ack, Body: body, ContentType: "application/json"}
}

func newTestConsumer(st Store, maxAttempts int) *Consumer {
	return New("amqp://unused", broker.DefaultTopology(), "q", "tag", 1, maxAttempts,
		time.Millisecond, 5*time.Millisecond, st, quietLogger())
}

func TestDecodeResultValidation(t *testing.T) {
	valid := resultEvent(domain.TaskInProgress)
	noError := resultEvent(domain.TaskFailed)
	noError.Error = nil
	wrongSchema := resultEvent(domain.TaskDone)
	wrongSchema.SchemaVersion = 2

	tests := []struct {
		name string
		body []byte
		ok   bool
	}{
		{"valid in_progress", mustJSON(t, valid), true},
		{"valid failed", mustJSON(t, resultEvent(domain.TaskFailed)), true},
		{"empty", nil, false},
		{"too large", make([]byte, MaxBodyBytes+1), false},
		{"invalid json", []byte("{"), false},
		{"unsupported schema", mustJSON(t, wrongSchema), false},
		{"failed without error", mustJSON(t, noError), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := DecodeResult(tc.body)
			if tc.ok {
				if err != nil {
					t.Fatalf("DecodeResult: %v", err)
				}
				if got.EventID == uuid.Nil {
					t.Fatal("event id not decoded")
				}
				return
			}
			if !errors.Is(err, domain.ErrInvalidEvent) {
				t.Fatalf("err = %v, want ErrInvalidEvent", err)
			}
		})
	}
}

func TestHandleAckPolicy(t *testing.T) {
	tests := []struct {
		name        string
		storeErr    error
		maxAttempts int
		wantAcks    int
		wantNacks   int
		wantRequeue bool
	}{
		{"applied", nil, 5, 1, 0, false},
		{"permanent", domain.ErrTaskNotFound, 5, 0, 1, false},
		{"transient retries", errors.New("db unavailable"), 5, 0, 1, true},
		{"transient exhausted", errors.New("db unavailable"), 1, 0, 1, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ack := &fakeAck{}
			st := &fakeStore{out: store.ResultOutcome{Outcome: domain.OutcomeInProgress}, err: tc.storeErr}
			c := newTestConsumer(st, tc.maxAttempts)
			c.handle(context.Background(), newDelivery(mustJSON(t, resultEvent(domain.TaskInProgress)), ack))
			if ack.acks != tc.wantAcks || ack.nacks != tc.wantNacks {
				t.Fatalf("acks=%d nacks=%d, want %d/%d", ack.acks, ack.nacks, tc.wantAcks, tc.wantNacks)
			}
			if tc.wantNacks > 0 && ack.requeue != tc.wantRequeue {
				t.Fatalf("requeue=%v, want %v", ack.requeue, tc.wantRequeue)
			}
			if st.calls != 1 {
				t.Fatalf("store calls = %d, want 1", st.calls)
			}
		})
	}
}

func TestHandleDuplicateIsAcknowledged(t *testing.T) {
	ack := &fakeAck{}
	st := &fakeStore{out: store.ResultOutcome{Duplicate: true, Outcome: domain.OutcomeDone}}
	newTestConsumer(st, 5).handle(context.Background(), newDelivery(mustJSON(t, resultEvent(domain.TaskDone)), ack))
	if ack.acks != 1 || ack.nacks != 0 {
		t.Fatalf("acks=%d nacks=%d, want 1/0", ack.acks, ack.nacks)
	}
}

func TestHandlePoisonIsDeadLetteredWithoutStoreCall(t *testing.T) {
	ack := &fakeAck{}
	st := &fakeStore{}
	newTestConsumer(st, 5).handle(context.Background(), newDelivery([]byte(`{"schema_version":1}`), ack))
	if ack.nacks != 1 || ack.requeue {
		t.Fatalf("nack requeue = %v (nacks=%d)", ack.requeue, ack.nacks)
	}
	if st.calls != 0 {
		t.Fatalf("store called %d times for poison", st.calls)
	}
}

func TestDeliveryAttempt(t *testing.T) {
	tests := []struct {
		name string
		d    amqp.Delivery
		want int
	}{
		{"first delivery", amqp.Delivery{}, 1},
		{"redelivered", amqp.Delivery{Redelivered: true}, 2},
		{"header int64", amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int64(2)}}, 3},
		{"header int32", amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int32(0)}}, 1},
		{"header int", amqp.Delivery{Headers: amqp.Table{"x-delivery-count": 4}}, 5},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := deliveryAttempt(tc.d); got != tc.want {
				t.Fatalf("deliveryAttempt = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestBackoffIsBoundedAndJittered(t *testing.T) {
	base, max := 10*time.Millisecond, 50*time.Millisecond
	for attempt := 1; attempt <= 6; attempt++ {
		want := base << (attempt - 1)
		if want > max {
			want = max
		}
		for i := 0; i < 20; i++ {
			got := backoff(base, max, attempt)
			if got < time.Duration(float64(want)*0.8) || got > time.Duration(float64(want)*1.2) {
				t.Fatalf("backoff(attempt=%d) = %v, want %v +/-20%%", attempt, got, want)
			}
		}
	}
}
