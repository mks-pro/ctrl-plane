package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/your-org/ctrlplane/internal/domain"
)

const taskColumns = `id, tenant_id, type, status, created_at, updated_at`

func scanTask(row pgx.Row) (domain.Task, error) {
	var t domain.Task
	var taskType, status string
	if err := row.Scan(&t.ID, &t.TenantID, &taskType, &status, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return domain.Task{}, err
	}
	t.Type = domain.TaskType(taskType)
	t.Status = domain.TaskStatus(status)
	return t, nil
}

func insertTask(ctx context.Context, tx pgx.Tx, t domain.Task) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO tasks (id, tenant_id, type, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		t.ID, t.TenantID, string(t.Type), string(t.Status), t.CreatedAt, t.UpdatedAt)
	if err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	return nil
}

// GetTask loads one task, returning ErrTaskNotFound when absent.
func (s *Store) GetTask(ctx context.Context, id uuid.UUID) (domain.Task, error) {
	t, err := scanTask(s.pool.QueryRow(ctx, `SELECT `+taskColumns+` FROM tasks WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Task{}, domain.ErrTaskNotFound
	}
	if err != nil {
		return domain.Task{}, fmt.Errorf("select task: %w", err)
	}
	return t, nil
}

// ListTasks returns one page ordered by (created_at, id) DESC.
func (s *Store) ListTasks(ctx context.Context, f TaskFilter) (Page[domain.Task], error) {
	limit := f.Limit
	if limit <= 0 {
		limit = domain.DefaultLimit
	}
	var tenantID *uuid.UUID
	if f.TenantID != nil {
		tenantID = f.TenantID
	}
	var taskType, status *string
	if f.Type != nil {
		v := string(*f.Type)
		taskType = &v
	}
	if f.Status != nil {
		v := string(*f.Status)
		status = &v
	}
	afterID := uuid.Nil
	var at *time.Time
	if f.After != nil {
		at = &f.After.At
		afterID = f.After.ID
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+taskColumns+`
		  FROM tasks
		 WHERE ($1::uuid IS NULL OR tenant_id = $1)
		   AND ($2::text IS NULL OR type = $2)
		   AND ($3::text IS NULL OR status = $3)
		   AND ($4::timestamptz IS NULL OR (created_at, id) < ($4, $5))
		 ORDER BY created_at DESC, id DESC
		 LIMIT $6`,
		tenantID, taskType, status, at, afterID, limit+1)
	if err != nil {
		return Page[domain.Task]{}, fmt.Errorf("list tasks: %w", err)
	}
	defer rows.Close()

	items := make([]domain.Task, 0, limit)
	for rows.Next() {
		t, err := scanTask(rows)
		if err != nil {
			return Page[domain.Task]{}, fmt.Errorf("scan task: %w", err)
		}
		items = append(items, t)
	}
	if err := rows.Err(); err != nil {
		return Page[domain.Task]{}, fmt.Errorf("iterate tasks: %w", err)
	}
	page := Page[domain.Task]{Items: items}
	if len(items) > limit {
		last := items[limit-1]
		page.Items = items[:limit]
		page.Next = &Position{At: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

// ApplyResult applies one task.result event atomically: dedupe insert, task
// transition and (when terminal) the guarded tenant transition share one
// transaction. A duplicate event rolls back and reports Duplicate.
func (s *Store) ApplyResult(ctx context.Context, ev domain.ResultEvent) (ResultOutcome, error) {
	now := s.now()
	var out ResultOutcome
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		var tenantID uuid.UUID
		var taskType, status string
		err := tx.QueryRow(ctx, `
			SELECT tenant_id, type, status FROM tasks WHERE id = $1 FOR UPDATE`,
			ev.TaskID).Scan(&tenantID, &taskType, &status)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrTaskNotFound
		}
		if err != nil {
			return fmt.Errorf("lock task: %w", err)
		}
		if tenantID != ev.TenantID {
			return domain.ErrTenantMismatch
		}
		transition, err := domain.EvaluateResult(domain.TaskStatus(status), ev.Status)
		if err != nil {
			return err
		}
		out.Outcome = transition.Outcome

		tag, err := tx.Exec(ctx, `
			INSERT INTO processed_updates (event_id, task_id, outcome, applied_at)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT DO NOTHING`,
			ev.EventID, ev.TaskID, string(transition.Outcome), now)
		if err != nil {
			return fmt.Errorf("record processed update: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return errDuplicateEvent
		}
		if !transition.Apply {
			return nil
		}
		if _, err := tx.Exec(ctx, `UPDATE tasks SET status = $2, updated_at = $3 WHERE id = $1`,
			ev.TaskID, string(transition.Next), now); err != nil {
			return fmt.Errorf("update task: %w", err)
		}
		if !transition.Terminal {
			return nil
		}
		return s.applyTenantOutcome(ctx, tx, tenantID, domain.TaskType(taskType), ev.Status, now, &out)
	})
	if errors.Is(err, errDuplicateEvent) {
		return ResultOutcome{Duplicate: true}, nil
	}
	if err != nil {
		return ResultOutcome{}, err
	}
	return out, nil
}

func (s *Store) applyTenantOutcome(ctx context.Context, tx pgx.Tx, tenantID uuid.UUID, taskType domain.TaskType, result domain.TaskStatus, now time.Time, out *ResultOutcome) error {
	target, ok := domain.TenantTarget(taskType, result)
	if !ok {
		return fmt.Errorf("%w: no tenant target for %s/%s", domain.ErrInvalidEvent, taskType, result)
	}
	source, ok := domain.ExpectedSourceStatus(taskType)
	if !ok {
		return fmt.Errorf("%w: unknown task type %q", domain.ErrInvalidEvent, taskType)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE tenants
		   SET status = $2, version = version + 1, updated_at = $3
		 WHERE id = $1 AND status = $4`,
		tenantID, string(target), now, string(source))
	if err != nil {
		return fmt.Errorf("update tenant from task outcome: %w", err)
	}
	if tag.RowsAffected() == 0 {
		if s.logger != nil {
			s.logger.Warn("tenant transition skipped",
				"tenant_id", tenantID, "from_status", source, "to_status", target,
				"task_type", taskType)
		}
		return nil
	}
	out.TenantUpdated = true
	return nil
}
