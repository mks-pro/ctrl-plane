// Package api owns HTTP concerns only: routing, decoding, status codes and the
// single error envelope. It never imports pgx or amqp.
package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/your-org/ctrlplane/internal/domain"
)

type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

// notReadyError names the dependency that failed a readiness check.
type notReadyError struct{ Check string }

func (e *notReadyError) Error() string { return "service not ready: " + e.Check }

// mapping is the only place HTTP status codes are decided.
type mapping struct {
	status  int
	code    string
	message string
	details map[string]any
}

func mapError(err error) mapping {
	var invalid *domain.ValidationError
	var notAllowed *domain.UpdateNotAllowedError
	var conflict *domain.VersionConflictError
	var notReady *notReadyError

	switch {
	case errors.As(err, &invalid):
		return mapping{http.StatusUnprocessableEntity, "validation_error", invalid.Error(),
			map[string]any{"field": invalid.Field}}
	case errors.As(err, &conflict):
		return mapping{http.StatusConflict, "tenant_version_conflict", conflict.Error(),
			map[string]any{"expected_version": conflict.Expected, "current_version": conflict.Current}}
	case errors.As(err, &notAllowed):
		return mapping{http.StatusConflict, "tenant_update_not_allowed", notAllowed.Error(),
			map[string]any{"status": string(notAllowed.Status)}}
	case errors.As(err, &notReady):
		return mapping{http.StatusServiceUnavailable, "service_unavailable", notReady.Error(),
			map[string]any{"check": notReady.Check}}
	case errors.Is(err, domain.ErrTenantNotFound):
		return mapping{http.StatusNotFound, "tenant_not_found", "tenant not found", nil}
	case errors.Is(err, domain.ErrTaskNotFound):
		return mapping{http.StatusNotFound, "task_not_found", "task not found", nil}
	case errors.Is(err, domain.ErrSlugTaken):
		return mapping{http.StatusConflict, "tenant_already_exists", "tenant slug is already in use", nil}
	case errors.Is(err, domain.ErrMalformedBody):
		return mapping{http.StatusBadRequest, "invalid_request", "request body is malformed or has the wrong content type", nil}
	case errors.Is(err, domain.ErrRouteNotFound):
		return mapping{http.StatusNotFound, "route_not_found", "route not found", nil}
	case errors.Is(err, domain.ErrMethodNotAllowed):
		return mapping{http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed for this route", nil}
	case errors.Is(err, domain.ErrInvalid):
		return mapping{http.StatusUnprocessableEntity, "validation_error", err.Error(), nil}
	default:
		return mapping{http.StatusInternalServerError, "internal_error", "internal server error", nil}
	}
}

// writeError renders the one and only error envelope and logs server faults.
func (h *Handler) writeError(w http.ResponseWriter, r *http.Request, err error) {
	m := mapError(err)
	if m.status >= http.StatusInternalServerError {
		h.logger.Error("request failed", "error", err, "method", r.Method, "path", r.URL.Path,
			"request_id", requestID(r))
	}
	writeJSON(w, h.logger, m.status, errorEnvelope{Error: errorBody{
		Code: m.code, Message: m.message, Details: m.details,
	}})
}

func writeJSON(w http.ResponseWriter, logger *slog.Logger, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.Error("encode response", "error", err)
	}
}
