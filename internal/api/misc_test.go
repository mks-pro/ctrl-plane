package api_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
)

func TestListAndGetTasks(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})
	rec := post(t, h, "/v1/tenants", `{"slug":"acme-corp","name":"Acme"}`)
	var created mutationJSON
	decodeInto(t, rec, &created)

	rec = request(t, h, http.MethodGet, "/v1/tasks?tenant_id="+created.Tenant.ID+"&type=deploy&status=accepted", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	var page struct {
		Items      []taskJSON `json:"items"`
		NextCursor *string    `json:"next_cursor"`
	}
	decodeInto(t, rec, &page)
	if len(page.Items) != 1 || page.Items[0].ID != created.Task.ID {
		t.Fatalf("unexpected tasks: %+v", page)
	}
	filter := st.taskFilters[len(st.taskFilters)-1]
	if filter.TenantID == nil || filter.TenantID.String() != created.Tenant.ID ||
		filter.Type == nil || *filter.Type != domain.TaskDeploy ||
		filter.Status == nil || *filter.Status != domain.TaskAccepted {
		t.Fatalf("filters not forwarded: %+v", filter)
	}

	rec = request(t, h, http.MethodGet, "/v1/tasks/"+created.Task.ID, "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("get task: %d; body=%s", rec.Code, rec.Body.String())
	}
	var task taskJSON
	decodeInto(t, rec, &task)
	if task != created.Task {
		t.Fatalf("task = %+v, want %+v", task, created.Task)
	}

	requireError(t, request(t, h, http.MethodGet, "/v1/tasks/"+uuid.NewString(), "", ""),
		http.StatusNotFound, "task_not_found")
	requireError(t, request(t, h, http.MethodGet, "/v1/tasks/nope", "", ""),
		http.StatusNotFound, "task_not_found")
}

func TestTaskFilterValidation(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})
	cases := []struct{ query, field string }{
		{"tenant_id=nope", "tenant_id"},
		{"type=bogus", "type"},
		{"status=bogus", "status"},
		{"limit=1000", "limit"},
		{"cursor=bogus", "cursor"},
	}
	for _, c := range cases {
		body := requireError(t, request(t, h, http.MethodGet, "/v1/tasks?"+c.query, "", ""),
			http.StatusUnprocessableEntity, "validation_error")
		if body.Error.Details["field"] != c.field {
			t.Fatalf("%s: field = %v, want %s", c.query, body.Error.Details["field"], c.field)
		}
	}
}

func TestRoutingFallbacks(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})
	requireError(t, request(t, h, http.MethodGet, "/v1/unknown", "", ""),
		http.StatusNotFound, "route_not_found")
	requireError(t, request(t, h, http.MethodPut, "/v1/tenants", "", ""),
		http.StatusMethodNotAllowed, "method_not_allowed")
}

func TestHealthAndReadiness(t *testing.T) {
	st := newFakeStore()
	rec := request(t, newHandler(t, st, fakeBroker{open: true}), http.MethodGet, "/healthz", "", "")
	if rec.Code != http.StatusOK || rec.Body.String() != "{\"status\":\"ok\"}\n" {
		t.Fatalf("healthz: %d %q", rec.Code, rec.Body.String())
	}

	rec = request(t, newHandler(t, st, fakeBroker{open: true}), http.MethodGet, "/readyz", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz: %d %s", rec.Code, rec.Body.String())
	}

	rec = request(t, newHandler(t, st, fakeBroker{open: false}), http.MethodGet, "/readyz", "", "")
	if body := requireError(t, rec, http.StatusServiceUnavailable, "service_unavailable"); body.Error.Details["check"] != "broker" {
		t.Fatalf("broker readiness details: %+v", body.Error.Details)
	}

	st.pingErr = errDial
	rec = request(t, newHandler(t, st, fakeBroker{open: true}), http.MethodGet, "/readyz", "", "")
	if body := requireError(t, rec, http.StatusServiceUnavailable, "service_unavailable"); body.Error.Details["check"] != "database" {
		t.Fatalf("db readiness details: %+v", body.Error.Details)
	}
}
