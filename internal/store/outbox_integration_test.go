package store_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
	"github.com/your-org/ctrlplane/internal/store"
)

func TestCreateTenantWritesTaskAndOutboxAtomically(t *testing.T) {
	env := newTestEnv(t)
	tenant, task := mustCreateTenant(t, env, "acme-corp")

	if tenant.Status != domain.TenantProvisioning || tenant.Version != 1 {
		t.Fatalf("tenant = %+v", tenant)
	}
	if task.TenantID != tenant.ID || task.Type != domain.TaskDeploy || task.Status != domain.TaskAccepted {
		t.Fatalf("task = %+v", task)
	}

	var topic, routingKey string
	var payload []byte
	if err := env.db.QueryRow(context.Background(),
		`SELECT topic, routing_key, payload FROM outbox_events WHERE id = $1`, task.ID).
		Scan(&topic, &routingKey, &payload); err != nil {
		t.Fatalf("select outbox row: %v", err)
	}
	if topic != "ctrlplane.events" || routingKey != domain.EventTaskCreated {
		t.Fatalf("outbox routing = %s/%s", topic, routingKey)
	}
	var event domain.TaskCreatedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("outbox payload invalid: %v", err)
	}
	if event.ID != task.ID || event.Task.TenantID != tenant.ID {
		t.Fatalf("event identity = %+v", event)
	}
}

func TestOutboxRelayPublishesOnceInOrder(t *testing.T) {
	env := newTestEnv(t)
	ctx := context.Background()
	first, firstTask := mustCreateTenant(t, env, "alpha-tenant")
	second, secondTask := mustCreateTenant(t, env, "beta-tenant")

	var published []store.OutboxEvent
	publish := func(_ context.Context, e store.OutboxEvent) error {
		published = append(published, e)
		return nil
	}
	n, full, err := env.st.ProcessOutboxBatch(ctx, 10, publish)
	if err != nil || n != 2 || full {
		t.Fatalf("batch = %d/%v/%v", n, full, err)
	}
	want := map[uuid.UUID]bool{firstTask.ID: true, secondTask.ID: true}
	payloadTenants := map[uuid.UUID]bool{}
	for _, e := range published {
		var ev domain.TaskCreatedEvent
		if err := json.Unmarshal(e.Payload, &ev); err != nil {
			t.Fatalf("decode published payload: %v", err)
		}
		if !want[ev.Task.ID] {
			t.Fatalf("unexpected task published: %s", ev.Task.ID)
		}
		payloadTenants[ev.Task.TenantID] = true
	}
	if !payloadTenants[first.ID] || !payloadTenants[second.ID] {
		t.Fatalf("published tenants = %v", payloadTenants)
	}
	var firstEv domain.TaskCreatedEvent
	if err := json.Unmarshal(published[0].Payload, &firstEv); err != nil || firstEv.Task.ID != firstTask.ID {
		t.Fatalf("first published event = %v/%v, want task %s", firstEv.Task.ID, err, firstTask.ID)
	}
	if n := scalarInt(t, env, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); n != 0 {
		t.Fatalf("unpublished rows = %d, want 0", n)
	}
	if n := scalarInt(t, env, `SELECT max(attempts) FROM outbox_events`); n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}

	if n, _, err := env.st.ProcessOutboxBatch(ctx, 10, publish); err != nil || n != 0 {
		t.Fatalf("second batch = %d/%v", n, err)
	}
}
