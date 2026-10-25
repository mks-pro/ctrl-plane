// Command migrate applies the embedded SQL migrations exactly once each and
// records them in schema_migrations. It is idempotent and safe to re-run.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/your-org/ctrlplane/migrations"
)

// advisoryLockKey serialises concurrent migrate containers ("ctrl" in ASCII).
const advisoryLockKey int64 = 0x6374726c

func main() {
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection string (default $DATABASE_URL)")
	down := flag.Bool("down", false, "roll back all applied migrations (destructive)")
	timeout := flag.Duration("timeout", 60*time.Second, "overall timeout")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	logger = logger.With("service", "migrate")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *timeout)
	defer cancel()

	if *databaseURL == "" {
		logger.Error("missing database url", "error", "DATABASE_URL is required")
		os.Exit(1)
	}
	if err := run(ctx, *databaseURL, *down, logger); err != nil {
		logger.Error("migration failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, databaseURL string, down bool, logger *slog.Logger) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return fmt.Errorf("acquire advisory lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", advisoryLockKey)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version    text        PRIMARY KEY,
		name       text        NOT NULL,
		applied_at timestamptz NOT NULL
	)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	all, err := migrations.All()
	if err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, conn)
	if err != nil {
		return err
	}

	if down {
		return rollback(ctx, conn, all, applied, logger)
	}
	return up(ctx, conn, all, applied, logger)
}

func appliedVersions(ctx context.Context, conn *pgx.Conn) (map[string]bool, error) {
	rows, err := conn.Query(ctx, "SELECT version FROM schema_migrations")
	if err != nil {
		return nil, fmt.Errorf("read schema_migrations: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out[v] = true
	}
	return out, rows.Err()
}

func up(ctx context.Context, conn *pgx.Conn, all []migrations.Migration, applied map[string]bool, logger *slog.Logger) error {
	for _, m := range all {
		if applied[m.Version] {
			continue
		}
		if err := applyOne(ctx, conn, m); err != nil {
			return err
		}
		logger.Info("applied migration", "version", m.Version, "name", m.Name)
	}
	return nil
}

func applyOne(ctx context.Context, conn *pgx.Conn, m migrations.Migration) error {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin %s: %w", m.Version, err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// Simple-protocol execution allows a migration file to contain several
	// statements while still running inside this transaction.
	if _, err := tx.Conn().PgConn().Exec(ctx, m.Up).ReadAll(); err != nil {
		return fmt.Errorf("apply %s_%s: %w", m.Version, m.Name, err)
	}
	if _, err := tx.Exec(ctx,
		"INSERT INTO schema_migrations (version, name, applied_at) VALUES ($1, $2, $3)",
		m.Version, m.Name, time.Now().UTC(),
	); err != nil {
		return fmt.Errorf("record %s: %w", m.Version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit %s: %w", m.Version, err)
	}
	return nil
}

func rollback(ctx context.Context, conn *pgx.Conn, all []migrations.Migration, applied map[string]bool, logger *slog.Logger) error {
	for i := len(all) - 1; i >= 0; i-- {
		m := all[i]
		if !applied[m.Version] {
			continue
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin down %s: %w", m.Version, err)
		}
		if _, err := tx.Conn().PgConn().Exec(ctx, m.Down).ReadAll(); err != nil {
			_ = tx.Rollback(context.Background())
			return fmt.Errorf("rollback %s_%s: %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx, "DELETE FROM schema_migrations WHERE version = $1", m.Version); err != nil {
			_ = tx.Rollback(context.Background())
			return fmt.Errorf("unrecord %s: %w", m.Version, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit down %s: %w", m.Version, err)
		}
		logger.Warn("rolled back migration", "version", m.Version, "name", m.Name)
	}
	return nil
}
