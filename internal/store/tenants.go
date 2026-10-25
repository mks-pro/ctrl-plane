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

const tenantColumns = `id, slug, name, status, version, created_at, updated_at`

func scanTenant(row pgx.Row) (domain.Tenant, error) {
	var t domain.Tenant
	var status string
	if err := row.Scan(&t.ID, &t.Slug, &t.Name, &status, &t.Version, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return domain.Tenant{}, err
	}
	t.Status = domain.TenantStatus(status)
	return t, nil
}

// CreateTenant inserts the tenant, its deploy task and the outbox event in one
// transaction. The unique index (not application code) decides slug races.
func (s *Store) CreateTenant(ctx context.Context, slug, name string) (domain.Tenant, domain.Task, error) {
	now := s.now()
	tenant := domain.Tenant{
		ID: uuid.New(), Slug: slug, Name: name, Status: domain.TenantProvisioning,
		Version: 1, CreatedAt: now, UpdatedAt: now,
	}
	task := domain.Task{
		ID: uuid.New(), TenantID: tenant.ID, Type: domain.TaskDeploy,
		Status: domain.TaskAccepted, CreatedAt: now, UpdatedAt: now,
	}
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			INSERT INTO tenants (id, slug, name, status, version, created_at, updated_at)
			VALUES ($1, $2, $3, 'provisioning', 1, $4, $4)
			ON CONFLICT (slug) DO NOTHING
			RETURNING `+tenantColumns,
			tenant.ID, slug, name, now)
		inserted, err := scanTenant(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return domain.ErrSlugTaken
		}
		if err != nil {
			return fmt.Errorf("insert tenant: %w", err)
		}
		tenant = inserted
		if err := insertTask(ctx, tx, task); err != nil {
			return err
		}
		return insertTaskCreatedEvent(ctx, tx, s.exchange, task)
	})
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	return tenant, task, nil
}

// GetTenant loads one tenant, returning ErrTenantNotFound when absent.
func (s *Store) GetTenant(ctx context.Context, id uuid.UUID) (domain.Tenant, error) {
	t, err := scanTenant(s.pool.QueryRow(ctx, `SELECT `+tenantColumns+` FROM tenants WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.Tenant{}, domain.ErrTenantNotFound
	}
	if err != nil {
		return domain.Tenant{}, fmt.Errorf("select tenant: %w", err)
	}
	return t, nil
}

// ListTenants returns one page ordered by (created_at, id) DESC.
func (s *Store) ListTenants(ctx context.Context, f TenantFilter) (Page[domain.Tenant], error) {
	limit := f.Limit
	if limit <= 0 {
		limit = domain.DefaultLimit
	}
	var status, slug *string
	if f.Status != nil {
		v := string(*f.Status)
		status = &v
	}
	if f.Slug != nil {
		slug = f.Slug
	}
	var at *time.Time
	afterID := uuid.Nil
	if f.After != nil {
		at = &f.After.At
		afterID = f.After.ID
	}
	rows, err := s.pool.Query(ctx, `
		SELECT `+tenantColumns+`
		  FROM tenants
		 WHERE ($1::text IS NULL OR status = $1)
		   AND ($2::text IS NULL OR slug = $2)
		   AND ($3::timestamptz IS NULL OR (created_at, id) < ($3, $4))
		 ORDER BY created_at DESC, id DESC
		 LIMIT $5`,
		status, slug, at, afterID, limit+1)
	if err != nil {
		return Page[domain.Tenant]{}, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()

	items := make([]domain.Tenant, 0, limit)
	for rows.Next() {
		t, err := scanTenant(rows)
		if err != nil {
			return Page[domain.Tenant]{}, fmt.Errorf("scan tenant: %w", err)
		}
		items = append(items, t)
	}
	if err := rows.Err(); err != nil {
		return Page[domain.Tenant]{}, fmt.Errorf("iterate tenants: %w", err)
	}
	page := Page[domain.Tenant]{Items: items}
	if len(items) > limit {
		last := items[limit-1]
		page.Items = items[:limit]
		page.Next = &Position{At: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

// UpdateTenant applies the PATCH transition: active -> updating, version+1,
// one new update task and one outbox event. Status, version and name are
// guarded and mutated in a single statement.
func (s *Store) UpdateTenant(ctx context.Context, id uuid.UUID, version int, name string) (domain.Tenant, domain.Task, error) {
	now := s.now()
	var tenant domain.Tenant
	task := domain.Task{
		ID: uuid.New(), TenantID: id, Type: domain.TaskUpdate,
		Status: domain.TaskAccepted, CreatedAt: now, UpdatedAt: now,
	}
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE tenants
			   SET name = $2, status = 'updating', version = version + 1, updated_at = $3
			 WHERE id = $1 AND status = 'active' AND version = $4
			RETURNING `+tenantColumns,
			id, name, now, version)
		updated, err := scanTenant(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return classifyUpdate(ctx, tx, id, version)
		}
		if err != nil {
			return fmt.Errorf("update tenant: %w", err)
		}
		tenant = updated
		if err := insertTask(ctx, tx, task); err != nil {
			return err
		}
		return insertTaskCreatedEvent(ctx, tx, s.exchange, task)
	})
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	return tenant, task, nil
}

// DeleteTenant applies the DELETE transition: active|failed -> destroying.
func (s *Store) DeleteTenant(ctx context.Context, id uuid.UUID) (domain.Tenant, domain.Task, error) {
	now := s.now()
	var tenant domain.Tenant
	task := domain.Task{
		ID: uuid.New(), TenantID: id, Type: domain.TaskDestroy,
		Status: domain.TaskAccepted, CreatedAt: now, UpdatedAt: now,
	}
	err := s.withTx(ctx, func(tx pgx.Tx) error {
		row := tx.QueryRow(ctx, `
			UPDATE tenants
			   SET status = 'destroying', version = version + 1, updated_at = $2
			 WHERE id = $1 AND status IN ('active','failed')
			RETURNING `+tenantColumns,
			id, now)
		updated, err := scanTenant(row)
		if errors.Is(err, pgx.ErrNoRows) {
			return classifyDelete(ctx, tx, id)
		}
		if err != nil {
			return fmt.Errorf("delete tenant: %w", err)
		}
		tenant = updated
		if err := insertTask(ctx, tx, task); err != nil {
			return err
		}
		return insertTaskCreatedEvent(ctx, tx, s.exchange, task)
	})
	if err != nil {
		return domain.Tenant{}, domain.Task{}, err
	}
	return tenant, task, nil
}

// classifyUpdate distinguishes 404 from 409 after a zero-row UPDATE. It runs
// inside the same transaction, which is always rolled back afterwards.
func classifyUpdate(ctx context.Context, tx pgx.Tx, id uuid.UUID, version int) error {
	var status string
	var current int
	err := tx.QueryRow(ctx, `SELECT status, version FROM tenants WHERE id = $1`, id).Scan(&status, &current)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrTenantNotFound
	}
	if err != nil {
		return fmt.Errorf("classify update: %w", err)
	}
	ts := domain.TenantStatus(status)
	// Version is checked first so a concurrent PATCH that lost the CAS reports
	// tenant_version_conflict (AC-4) rather than the status the winner moved to.
	if current != version {
		return &domain.VersionConflictError{Expected: version, Current: current}
	}
	if !ts.AllowsPatch() {
		return &domain.UpdateNotAllowedError{Status: ts}
	}
	return &domain.VersionConflictError{Expected: version, Current: current}
}

func classifyDelete(ctx context.Context, tx pgx.Tx, id uuid.UUID) error {
	var status string
	err := tx.QueryRow(ctx, `SELECT status FROM tenants WHERE id = $1`, id).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return domain.ErrTenantNotFound
	}
	if err != nil {
		return fmt.Errorf("classify delete: %w", err)
	}
	ts := domain.TenantStatus(status)
	if !ts.AllowsDelete() {
		return &domain.UpdateNotAllowedError{Status: ts}
	}
	return nil
}
