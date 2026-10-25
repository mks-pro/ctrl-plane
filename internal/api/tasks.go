package api

import (
	"context"
	"net/http"
	"time"

	"github.com/your-org/ctrlplane/internal/domain"
)

func (h *Handler) listTasks(w http.ResponseWriter, r *http.Request) {
	filter, err := parseTaskFilter(r)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	page, err := h.store.ListTasks(r.Context(), filter)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	items := make([]taskDTO, 0, len(page.Items))
	for _, t := range page.Items {
		items = append(items, newTaskDTO(t))
	}
	var next *string
	if page.Next != nil {
		cursor := encodeCursor(*page.Next)
		next = &cursor
	}
	writeJSON(w, h.logger, http.StatusOK, newListResponse(items, next))
}

func (h *Handler) getTask(w http.ResponseWriter, r *http.Request) {
	id, err := parseIDParam(r, "id", domain.ErrTaskNotFound)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	task, err := h.store.GetTask(r.Context(), id)
	if err != nil {
		h.writeError(w, r, err)
		return
	}
	writeJSON(w, h.logger, http.StatusOK, newTaskDTO(task))
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, h.logger, http.StatusOK, map[string]string{"status": "ok"})
}

// ready checks both dependencies. It never mutates state; a closed publisher
// connection is reported as unavailable and the relay reconnects on its own.
func (h *Handler) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.store.Ping(ctx); err != nil {
		h.writeError(w, r, &notReadyError{Check: "database"})
		return
	}
	if h.broker == nil || !h.broker.IsOpen() {
		h.writeError(w, r, &notReadyError{Check: "broker"})
		return
	}
	writeJSON(w, h.logger, http.StatusOK, map[string]string{"status": "ready"})
}
