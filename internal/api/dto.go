package api

import "github.com/your-org/ctrlplane/internal/domain"

// tenantDTO is the public tenant representation. Timestamps are always UTC
// with millisecond precision.
type tenantDTO struct {
	ID        string `json:"id"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Version   int    `json:"version"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func newTenantDTO(t domain.Tenant) tenantDTO {
	return tenantDTO{
		ID:        t.ID.String(),
		Slug:      t.Slug,
		Name:      t.Name,
		Status:    string(t.Status),
		Version:   t.Version,
		CreatedAt: domain.FormatTime(t.CreatedAt),
		UpdatedAt: domain.FormatTime(t.UpdatedAt),
	}
}

// taskDTO is the public task representation.
type taskDTO struct {
	ID        string `json:"id"`
	TenantID  string `json:"tenant_id"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

func newTaskDTO(t domain.Task) taskDTO {
	return taskDTO{
		ID:        t.ID.String(),
		TenantID:  t.TenantID.String(),
		Type:      string(t.Type),
		Status:    string(t.Status),
		CreatedAt: domain.FormatTime(t.CreatedAt),
		UpdatedAt: domain.FormatTime(t.UpdatedAt),
	}
}

// mutationResponse is used by create, update and delete.
type mutationResponse struct {
	Tenant tenantDTO `json:"tenant"`
	Task   taskDTO   `json:"task"`
}

// listResponse is the single list shape; next_cursor is null on the last page.
type listResponse[T any] struct {
	Items      []T     `json:"items"`
	NextCursor *string `json:"next_cursor"`
}

func newListResponse[T any](items []T, next *string) listResponse[T] {
	if items == nil {
		items = []T{}
	}
	return listResponse[T]{Items: items, NextCursor: next}
}
