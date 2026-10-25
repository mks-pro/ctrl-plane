package api_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
)

func activate(t *testing.T, st *fakeStore, id string) tenantJSON {
	t.Helper()
	uid, err := uuid.Parse(id)
	if err != nil {
		t.Fatalf("parse id: %v", err)
	}
	tenant, ok := st.tenants[uid]
	if !ok {
		t.Fatalf("tenant %s missing", id)
	}
	tenant.Status = domain.TenantActive
	st.tenants[uid] = tenant
	return tenantJSON{ID: id, Status: "active", Version: tenant.Version}
}

func TestUpdateTenantLifecycle(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})
	rec := post(t, h, "/v1/tenants", `{"slug":"acme-corp","name":"Acme"}`)
	var created mutationJSON
	decodeInto(t, rec, &created)

	requireError(t, patch(t, h, "/v1/tenants/"+created.Tenant.ID, `{"version":1,"name":"Renamed"}`),
		http.StatusConflict, "tenant_update_not_allowed")

	activate(t, st, created.Tenant.ID)
	uid := uuid.MustParse(created.Tenant.ID)
	stale := st.tenants[uid]
	stale.Version = 2
	st.tenants[uid] = stale
	body := requireError(t, patch(t, h, "/v1/tenants/"+created.Tenant.ID, `{"version":1,"name":"Renamed"}`),
		http.StatusConflict, "tenant_version_conflict")
	if body.Error.Details["expected_version"] != float64(1) || body.Error.Details["current_version"] != float64(2) {
		t.Fatalf("conflict details = %+v", body.Error.Details)
	}

	rec = patch(t, h, "/v1/tenants/"+created.Tenant.ID, `{"version":2,"name":" Renamed "}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("patch status = %d; body=%s", rec.Code, rec.Body.String())
	}
	if got, want := rec.Header().Get("Location"), "/v1/tenants/"+created.Tenant.ID; got != want {
		t.Fatalf("patch Location = %q, want %q", got, want)
	}
	var updated mutationJSON
	decodeInto(t, rec, &updated)
	if updated.Tenant.Name != "Renamed" || updated.Tenant.Status != "updating" || updated.Tenant.Version != 3 {
		t.Fatalf("unexpected tenant after patch: %+v", updated.Tenant)
	}
	if updated.Task.Type != "update" || updated.Task.Status != "accepted" {
		t.Fatalf("unexpected task after patch: %+v", updated.Task)
	}

	body = requireError(t, patch(t, h, "/v1/tenants/"+created.Tenant.ID, `{"version":3,"name":"Again"}`),
		http.StatusConflict, "tenant_update_not_allowed")
	if body.Error.Details["status"] != "updating" {
		t.Fatalf("status detail = %+v", body.Error.Details)
	}
}

func TestPatchValidationAndPrecedence(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})
	rec := post(t, h, "/v1/tenants", `{"slug":"acme-corp","name":"Acme"}`)
	var created mutationJSON
	decodeInto(t, rec, &created)
	activate(t, st, created.Tenant.ID)

	cases := []struct {
		body  string
		field string
	}{
		{`{"version":0,"name":"Name"}`, "version"},
		{`{"name":"Name"}`, "version"},
		{`{"version":1}`, "name"},
		{`{"version":1,"name":"   "}`, "name"},
	}
	for _, c := range cases {
		body := requireError(t, patch(t, h, "/v1/tenants/"+created.Tenant.ID, c.body),
			http.StatusUnprocessableEntity, "validation_error")
		if body.Error.Details["field"] != c.field {
			t.Fatalf("body %s: field = %v, want %s", c.body, body.Error.Details["field"], c.field)
		}
	}

	requireError(t, patch(t, h, "/v1/tenants/"+uuid.NewString(), `{"version":0,"name":""}`),
		http.StatusNotFound, "tenant_not_found")
	requireError(t, patch(t, h, "/v1/tenants/"+created.Tenant.ID, `{"version":`),
		http.StatusBadRequest, "invalid_request")
}

func TestDeleteTenant(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})
	rec := post(t, h, "/v1/tenants", `{"slug":"acme-corp","name":"Acme"}`)
	var created mutationJSON
	decodeInto(t, rec, &created)

	requireError(t, request(t, h, http.MethodDelete, "/v1/tenants/"+created.Tenant.ID, "", ""),
		http.StatusConflict, "tenant_update_not_allowed")

	activate(t, st, created.Tenant.ID)
	rec = request(t, h, http.MethodDelete, "/v1/tenants/"+created.Tenant.ID, "", "")
	if rec.Code != http.StatusAccepted {
		t.Fatalf("delete status = %d; body=%s", rec.Code, rec.Body.String())
	}
	if got, want := rec.Header().Get("Location"), "/v1/tenants/"+created.Tenant.ID; got != want {
		t.Fatalf("delete Location = %q, want %q", got, want)
	}
	var deleted mutationJSON
	decodeInto(t, rec, &deleted)
	if deleted.Tenant.Status != "destroying" || deleted.Tenant.Version != 2 {
		t.Fatalf("unexpected tenant after delete: %+v", deleted.Tenant)
	}
	if deleted.Task.Type != "destroy" {
		t.Fatalf("unexpected task after delete: %+v", deleted.Task)
	}

	requireError(t, request(t, h, http.MethodDelete, "/v1/tenants/"+created.Tenant.ID, "", ""),
		http.StatusConflict, "tenant_update_not_allowed")
	requireError(t, request(t, h, http.MethodDelete, "/v1/tenants/"+uuid.NewString(), "", ""),
		http.StatusNotFound, "tenant_not_found")

	// A failed tenant is deletable: failed -> destroying is allowed (AC-9).
	other := post(t, h, "/v1/tenants", `{"slug":"beta-corp","name":"Beta"}`)
	var second mutationJSON
	decodeInto(t, other, &second)
	uid := uuid.MustParse(second.Tenant.ID)
	failed := st.tenants[uid]
	failed.Status = domain.TenantFailed
	st.tenants[uid] = failed
	if rec := request(t, h, http.MethodDelete, "/v1/tenants/"+second.Tenant.ID, "", ""); rec.Code != http.StatusAccepted {
		t.Fatalf("delete failed tenant = %d; body=%s", rec.Code, rec.Body.String())
	}
}

func TestInternalErrorsAreNotLeaked(t *testing.T) {
	st := newFakeStore()
	st.forcedErr = errors.New("pq: relation tenants does not exist")
	h := newHandler(t, st, fakeBroker{open: true})
	body := requireError(t, request(t, h, http.MethodGet, "/v1/tenants", "", ""),
		http.StatusInternalServerError, "internal_error")
	if body.Error.Message != "internal server error" {
		t.Fatalf("message leaked: %q", body.Error.Message)
	}
}
