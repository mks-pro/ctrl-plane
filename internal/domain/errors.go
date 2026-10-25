// Package domain holds the business rules: entities, state machines, validation
// and the event contracts. It imports only the standard library so it stays
// testable without a database or a broker.
package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors. Store and API layers classify against these; they never
// compare error strings.
var (
	ErrTenantNotFound         = errors.New("tenant not found")
	ErrTaskNotFound           = errors.New("task not found")
	ErrSlugTaken              = errors.New("slug already taken")
	ErrMalformedBody          = errors.New("malformed request body")
	ErrRouteNotFound          = errors.New("route not found")
	ErrMethodNotAllowed       = errors.New("method not allowed")
	ErrInvalid                = errors.New("validation failed")
	ErrTenantUpdateNotAllowed = errors.New("tenant update not allowed")
	ErrVersionConflict        = errors.New("tenant version conflict")
	ErrTenantMismatch         = errors.New("result tenant does not match task tenant")
	ErrInvalidEvent           = errors.New("invalid result event")
)

// ValidationError marks input that violates a field constraint (HTTP 422).
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string { return e.Field + ": " + e.Message }

// Is lets errors.Is(err, ErrInvalid) succeed for every validation failure.
func (e *ValidationError) Is(target error) bool { return target == ErrInvalid }

// Invalid builds a ValidationError.
func Invalid(field, message string) error {
	return &ValidationError{Field: field, Message: message}
}

// UpdateNotAllowedError carries the current status for the operator.
type UpdateNotAllowedError struct {
	Status TenantStatus
}

func (e *UpdateNotAllowedError) Error() string {
	return fmt.Sprintf("tenant in status %q cannot be updated", e.Status)
}

func (e *UpdateNotAllowedError) Is(target error) bool { return target == ErrTenantUpdateNotAllowed }

// VersionConflictError reports the optimistic-locking mismatch.
type VersionConflictError struct {
	Expected int
	Current  int
}

func (e *VersionConflictError) Error() string {
	return fmt.Sprintf("tenant version %d does not match current version %d", e.Expected, e.Current)
}

func (e *VersionConflictError) Is(target error) bool { return target == ErrVersionConflict }
