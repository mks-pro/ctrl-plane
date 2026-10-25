// Package store owns every SQL statement in the system. Handlers and the
// consumer see domain types and typed errors; only this package imports pgx.
package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/your-org/ctrlplane/internal/domain"
)

// Store is a PostgreSQL-backed repository.
type Store struct {
	pool     *pgxpool.Pool
	clock    domain.Clock
	logger   *slog.Logger
	exchange string
}

// Open parses the DSN, creates the pool and verifies connectivity. exchange is
// the topic exchange recorded on outbox rows; the relay publishes there.
func Open(ctx context.Context, dsn string, maxConns int32, exchange string, clock domain.Clock, logger *slog.Logger) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{pool: pool, clock: clock, logger: logger, exchange: exchange}, nil
}

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// Ping verifies the database is reachable; used by /readyz.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) now() time.Time {
	return s.clock.Now().UTC().Truncate(time.Microsecond)
}

// withTx runs fn in a transaction, committing on nil and rolling back on error.
func (s *Store) withTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// Position is a keyset cursor: the (created_at, id) tuple of the last row of a
// page. Microsecond precision is required because millisecond ties are common.
type Position struct {
	At time.Time
	ID uuid.UUID
}

// TenantFilter drives tenant listing. Nil fields mean "no filter".
type TenantFilter struct {
	Status *domain.TenantStatus
	Slug   *string
	After  *Position
	Limit  int
}

// TaskFilter drives task listing.
type TaskFilter struct {
	TenantID *uuid.UUID
	Type     *domain.TaskType
	Status   *domain.TaskStatus
	After    *Position
	Limit    int
}

// Page is a keyset-paginated result. Next is nil on the last page.
type Page[T any] struct {
	Items []T
	Next  *Position
}

// ResultOutcome describes what the consumer did with one result event.
type ResultOutcome struct {
	Outcome       domain.Outcome
	Duplicate     bool
	TenantUpdated bool
}

var errDuplicateEvent = fmt.Errorf("duplicate event")
