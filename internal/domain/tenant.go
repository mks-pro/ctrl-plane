package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TenantStatus is the lifecycle state of a tenant environment.
type TenantStatus string

const (
	TenantProvisioning TenantStatus = "provisioning"
	TenantActive       TenantStatus = "active"
	TenantUpdating     TenantStatus = "updating"
	TenantDestroying   TenantStatus = "destroying"
	TenantDestroyed    TenantStatus = "destroyed"
	TenantFailed       TenantStatus = "failed"
)

var tenantStatuses = []TenantStatus{
	TenantProvisioning, TenantActive, TenantUpdating,
	TenantDestroying, TenantDestroyed, TenantFailed,
}

// Valid reports whether s is a known tenant status.
func (s TenantStatus) Valid() bool {
	for _, v := range tenantStatuses {
		if s == v {
			return true
		}
	}
	return false
}

// ParseTenantStatus validates a status coming from a query parameter.
func ParseTenantStatus(s string) (TenantStatus, error) {
	st := TenantStatus(s)
	if !st.Valid() {
		return "", Invalid("status", fmt.Sprintf("unknown tenant status %q", s))
	}
	return st, nil
}

// AllowsPatch encodes the tenant state machine: only active tenants are mutable.
func (s TenantStatus) AllowsPatch() bool { return s == TenantActive }

// AllowsDelete encodes the tenant state machine: active and failed tenants can be
// destroyed; failed is included so a failed tenant never becomes unremovable.
func (s TenantStatus) AllowsDelete() bool { return s == TenantActive || s == TenantFailed }

// Tenant is one customer environment. Version is the optimistic-locking token.
type Tenant struct {
	ID        uuid.UUID
	Slug      string
	Name      string
	Status    TenantStatus
	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}
