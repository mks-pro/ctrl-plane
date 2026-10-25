package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/your-org/ctrlplane/internal/domain"
)

// OutboxEvent is one unpublished row claimed by the relay.
type OutboxEvent struct {
	ID         uuid.UUID
	Topic      string
	RoutingKey string
	Payload    []byte
	Headers    []byte
}

// PublishFunc publishes one event and returns nil only after a broker confirm.
type PublishFunc func(ctx context.Context, e OutboxEvent) error

// insertTaskCreatedEvent writes the outbox row in the same transaction as the
// tenant and task rows. The event id equals the task id.
func insertTaskCreatedEvent(ctx context.Context, tx pgx.Tx, exchange string, t domain.Task) error {
	payload, err := json.Marshal(domain.NewTaskCreatedEvent(t))
	if err != nil {
		return fmt.Errorf("marshal task.created: %w", err)
	}
	headers, err := json.Marshal(map[string]int{"schema_version": domain.SchemaVersion})
	if err != nil {
		return fmt.Errorf("marshal headers: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events (id, topic, routing_key, payload, headers, created_at)
		VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6)`,
		t.ID, exchange, domain.EventTaskCreated, string(payload), string(headers), t.CreatedAt); err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

// ProcessOutboxBatch claims up to limit rows with FOR UPDATE SKIP LOCKED,
// publishes them in created_at order, and commits. The first publish failure
// stops the batch so later events cannot overtake it; the row stays
// unpublished for the next attempt and the successful prefix is committed.
func (s *Store) ProcessOutboxBatch(ctx context.Context, limit int, publish PublishFunc) (published int, full bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, false, fmt.Errorf("begin outbox batch: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	rows, err := tx.Query(ctx, `
		SELECT id, topic, routing_key, payload, headers
		  FROM outbox_events
		 WHERE published_at IS NULL
		 ORDER BY created_at, id
		 FOR UPDATE SKIP LOCKED
		 LIMIT $1`, limit)
	if err != nil {
		return 0, false, fmt.Errorf("claim outbox rows: %w", err)
	}
	events := make([]OutboxEvent, 0, limit)
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.Topic, &e.RoutingKey, &e.Payload, &e.Headers); err != nil {
			rows.Close()
			return 0, false, fmt.Errorf("scan outbox row: %w", err)
		}
		events = append(events, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, false, fmt.Errorf("iterate outbox: %w", err)
	}
	full = len(events) == limit

	for _, e := range events {
		if perr := publish(ctx, e); perr != nil {
			if _, uerr := tx.Exec(ctx, `
				UPDATE outbox_events SET attempts = attempts + 1, last_error = $2 WHERE id = $1`,
				e.ID, truncate(perr.Error(), 1000)); uerr != nil {
				return published, full, fmt.Errorf("record publish failure: %w", uerr)
			}
			if cerr := tx.Commit(ctx); cerr != nil {
				return published, full, fmt.Errorf("commit outbox batch: %w", cerr)
			}
			return published, full, fmt.Errorf("publish event %s: %w", e.ID, perr)
		}
		if _, uerr := tx.Exec(ctx, `
			UPDATE outbox_events
			   SET published_at = $2, attempts = attempts + 1, last_error = NULL
			 WHERE id = $1`, e.ID, s.now()); uerr != nil {
			return published, full, fmt.Errorf("mark published: %w", uerr)
		}
		published++
	}
	if err := tx.Commit(ctx); err != nil {
		return published, full, fmt.Errorf("commit outbox batch: %w", err)
	}
	return published, full, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
