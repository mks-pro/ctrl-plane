package api

import (
	"net/http"

	"github.com/your-org/ctrlplane/internal/domain"
)

type createTenantRequest struct {
	Slug string `json:"slug"`
	Name string `json:"name"`
}

type patchTenantRequest struct {
	Version int    `json:"version"`
	Name    string `json:"name"`
}

func (h *Handler) createTenant(w http.ResponseWriter, r *http.Request) {
	var req createTenantRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, err)
		return
	}
	slug, err := domain.ValidateSlug(req.Slug)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	name, err := domain.ValidateName(req.Name)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	tenant, task, err := h.store.CreateTenant(r.Context(), slug, name)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/tenants/"+tenant.ID.String())
	writeJSON(w, h.logger, http.StatusCreated, mutationResponse{
		Tenant: newTenantDTO(tenant),
		Task:   newTaskDTO(task),
	})
}

func (h *Handler) listTenants(w http.ResponseWriter, r *http.Request) {
	filter, err := parseTenantFilter(r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	page, err := h.store.ListTenants(r.Context(), filter)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	items := make([]tenantDTO, 0, len(page.Items))
	for _, t := range page.Items {
		items = append(items, newTenantDTO(t))
	}
	var next *string
	if page.Next != nil {
		cursor := encodeCursor(*page.Next)
		next = &cursor
	}
	writeJSON(w, h.logger, http.StatusOK, newListResponse(items, next))
}

func (h *Handler) getTenant(w http.ResponseWriter, r *http.Request) {
	id, err := parseIDParam(r, "id", domain.ErrTenantNotFound)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	tenant, err := h.store.GetTenant(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, h.logger, http.StatusOK, newTenantDTO(tenant))
}

// updateTenant enforces the documented precedence: 400 (body) -> 404 (tenant)
// -> 422 (semantic validation) -> 409 (state/version).
func (h *Handler) updateTenant(w http.ResponseWriter, r *http.Request) {
	id, err := parseIDParam(r, "id", domain.ErrTenantNotFound)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	var req patchTenantRequest
	if err := decodeJSON(w, r, &req); err != nil {
		h.writeError(w, r, err)
		return
	}
	trimmed, verr := validatePatch(req)
	if verr != nil {
		// 404 wins over 422, so existence is checked before surfacing the
		// validation failure. The extra read only happens on invalid input.
		if _, gerr := h.store.GetTenant(r.Context(), id); gerr != nil {
			h.writeError(w, r, gerr)
			return
		}
		h.writeError(w, r, verr)
		return
	}
	tenant, task, err := h.store.UpdateTenant(r.Context(), id, req.Version, trimmed)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/tenants/"+tenant.ID.String())
	writeJSON(w, h.logger, http.StatusAccepted, mutationResponse{
		Tenant: newTenantDTO(tenant),
		Task:   newTaskDTO(task),
	})
}

func (h *Handler) deleteTenant(w http.ResponseWriter, r *http.Request) {
	id, err := parseIDParam(r, "id", domain.ErrTenantNotFound)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	tenant, task, err := h.store.DeleteTenant(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/tenants/"+tenant.ID.String())
	writeJSON(w, h.logger, http.StatusAccepted, mutationResponse{
		Tenant: newTenantDTO(tenant),
		Task:   newTaskDTO(task),
	})
}

func validatePatch(req patchTenantRequest) (string, error) {
	if req.Version < 1 {
		return "", domain.Invalid("version", "must be a positive integer")
	}
	return domain.ValidateName(req.Name)
}
