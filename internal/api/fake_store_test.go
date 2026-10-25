package api_test

import (
	"bytes"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
	"github.com/your-org/ctrlplane/internal/store"
)

// fakeStore is an in-memory Store that mirrors the real transition rules. It
// exists to test HTTP behavior; SQL behavior is covered by store integration
// tests.
type fakeStore struct {
	mu            sync.Mutex
	tenants       map[uuid.UUID]domain.Tenant
	tasks         []domain.Task
	forcedErr     error
	pingErr       error
	tenantFilters []store.TenantFilter
	taskFilters   []store.TaskFilter
}

func newFakeStore() *fakeStore {
	return &fakeStore{tenants: map[uuid.UUID]domain.Tenant{}}
}

func now() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

func (f *fakeStore) CreateTenant(_ context.Context, slug, name string) (domain.Tenant, domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forcedErr != nil {
		return domain.Tenant{}, domain.Task{}, f.forcedErr
	}
	for _, t := range f.tenants {
		if t.Slug == slug {
			return domain.Tenant{}, domain.Task{}, domain.ErrSlugTaken
		}
	}
	at := now()
	tenant := domain.Tenant{
		ID: uuid.New(), Slug: slug, Name: name, Status: domain.TenantProvisioning,
		Version: 1, CreatedAt: at, UpdatedAt: at,
	}
	task := domain.Task{
		ID: uuid.New(), TenantID: tenant.ID, Type: domain.TaskDeploy,
		Status: domain.TaskAccepted, CreatedAt: at, UpdatedAt: at,
	}
	f.tenants[tenant.ID] = tenant
	f.tasks = append(f.tasks, task)
	return tenant, task, nil
}

func (f *fakeStore) GetTenant(_ context.Context, id uuid.UUID) (domain.Tenant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forcedErr != nil {
		return domain.Tenant{}, f.forcedErr
	}
	t, ok := f.tenants[id]
	if !ok {
		return domain.Tenant{}, domain.ErrTenantNotFound
	}
	return t, nil
}

func (f *fakeStore) ListTenants(_ context.Context, filter store.TenantFilter) (store.Page[domain.Tenant], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tenantFilters = append(f.tenantFilters, filter)
	if f.forcedErr != nil {
		return store.Page[domain.Tenant]{}, f.forcedErr
	}
	list := make([]domain.Tenant, 0, len(f.tenants))
	for _, t := range f.tenants {
		if filter.Status != nil && t.Status != *filter.Status {
			continue
		}
		if filter.Slug != nil && t.Slug != *filter.Slug {
			continue
		}
		if filter.After != nil && !before(t, *filter.After) {
			continue
		}
		list = append(list, t)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt.Equal(list[j].CreatedAt) {
			return bytes.Compare(list[i].ID[:], list[j].ID[:]) > 0
		}
		return list[i].CreatedAt.After(list[j].CreatedAt)
	})
	limit := filter.Limit
	if limit <= 0 {
		limit = domain.DefaultLimit
	}
	page := store.Page[domain.Tenant]{Items: list}
	if len(list) > limit {
		last := list[limit-1]
		page.Items = list[:limit]
		page.Next = &store.Position{At: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

func before(t domain.Tenant, pos store.Position) bool {
	if t.CreatedAt.Before(pos.At) {
		return true
	}
	return t.CreatedAt.Equal(pos.At) && bytes.Compare(t.ID[:], pos.ID[:]) < 0
}

func (f *fakeStore) UpdateTenant(_ context.Context, id uuid.UUID, version int, name string) (domain.Tenant, domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forcedErr != nil {
		return domain.Tenant{}, domain.Task{}, f.forcedErr
	}
	tenant, ok := f.tenants[id]
	if !ok {
		return domain.Tenant{}, domain.Task{}, domain.ErrTenantNotFound
	}
	if tenant.Version != version {
		return domain.Tenant{}, domain.Task{}, &domain.VersionConflictError{Expected: version, Current: tenant.Version}
	}
	if tenant.Status != domain.TenantActive {
		return domain.Tenant{}, domain.Task{}, &domain.UpdateNotAllowedError{Status: tenant.Status}
	}
	at := now()
	tenant.Name = name
	tenant.Status = domain.TenantUpdating
	tenant.Version++
	tenant.UpdatedAt = at
	f.tenants[id] = tenant
	task := domain.Task{
		ID: uuid.New(), TenantID: id, Type: domain.TaskUpdate,
		Status: domain.TaskAccepted, CreatedAt: at, UpdatedAt: at,
	}
	f.tasks = append(f.tasks, task)
	return tenant, task, nil
}

func (f *fakeStore) DeleteTenant(_ context.Context, id uuid.UUID) (domain.Tenant, domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forcedErr != nil {
		return domain.Tenant{}, domain.Task{}, f.forcedErr
	}
	tenant, ok := f.tenants[id]
	if !ok {
		return domain.Tenant{}, domain.Task{}, domain.ErrTenantNotFound
	}
	if !tenant.Status.AllowsDelete() {
		return domain.Tenant{}, domain.Task{}, &domain.UpdateNotAllowedError{Status: tenant.Status}
	}
	at := now()
	tenant.Status = domain.TenantDestroying
	tenant.Version++
	tenant.UpdatedAt = at
	f.tenants[id] = tenant
	task := domain.Task{
		ID: uuid.New(), TenantID: id, Type: domain.TaskDestroy,
		Status: domain.TaskAccepted, CreatedAt: at, UpdatedAt: at,
	}
	f.tasks = append(f.tasks, task)
	return tenant, task, nil
}

func (f *fakeStore) GetTask(_ context.Context, id uuid.UUID) (domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.forcedErr != nil {
		return domain.Task{}, f.forcedErr
	}
	for _, t := range f.tasks {
		if t.ID == id {
			return t, nil
		}
	}
	return domain.Task{}, domain.ErrTaskNotFound
}

func (f *fakeStore) ListTasks(_ context.Context, filter store.TaskFilter) (store.Page[domain.Task], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.taskFilters = append(f.taskFilters, filter)
	if f.forcedErr != nil {
		return store.Page[domain.Task]{}, f.forcedErr
	}
	list := make([]domain.Task, 0, len(f.tasks))
	for _, t := range f.tasks {
		if filter.TenantID != nil && t.TenantID != *filter.TenantID {
			continue
		}
		if filter.Type != nil && t.Type != *filter.Type {
			continue
		}
		if filter.Status != nil && t.Status != *filter.Status {
			continue
		}
		list = append(list, t)
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].CreatedAt.Equal(list[j].CreatedAt) {
			return bytes.Compare(list[i].ID[:], list[j].ID[:]) > 0
		}
		return list[i].CreatedAt.After(list[j].CreatedAt)
	})
	limit := filter.Limit
	if limit <= 0 {
		limit = domain.DefaultLimit
	}
	page := store.Page[domain.Task]{Items: list}
	if len(list) > limit {
		last := list[limit-1]
		page.Items = list[:limit]
		page.Next = &store.Position{At: last.CreatedAt, ID: last.ID}
	}
	return page, nil
}

func (f *fakeStore) Ping(context.Context) error { return f.pingErr }

type fakeBroker struct{ open bool }

func (b fakeBroker) IsOpen() bool { return b.open }
