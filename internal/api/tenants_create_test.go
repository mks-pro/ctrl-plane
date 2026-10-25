package api_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestCreateTenant(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})

	rec := post(t, h, "/v1/tenants", `{"slug":"  acme-corp ","name":"  Acme Corp "}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	var body mutationJSON
	decodeInto(t, rec, &body)
	if body.Tenant.Slug != "acme-corp" || body.Tenant.Name != "Acme Corp" {
		t.Fatalf("values not trimmed: %+v", body.Tenant)
	}
	if body.Tenant.Status != "provisioning" || body.Tenant.Version != 1 {
		t.Fatalf("unexpected tenant: %+v", body.Tenant)
	}
	if body.Task.Type != "deploy" || body.Task.Status != "accepted" {
		t.Fatalf("unexpected task: %+v", body.Task)
	}
	if got, want := rec.Header().Get("Location"), "/v1/tenants/"+body.Tenant.ID; got != want {
		t.Fatalf("Location = %q, want %q", got, want)
	}
	if body.Task.ID == "" || body.Task.ID == body.Tenant.ID {
		t.Fatalf("task must have its own id: %+v", body.Task)
	}
	if _, err := uuid.Parse(body.Tenant.ID); err != nil {
		t.Fatalf("tenant id: %v", err)
	}
	if !timestampPattern.MatchString(body.Tenant.CreatedAt) || !timestampPattern.MatchString(body.Tenant.UpdatedAt) {
		t.Fatalf("timestamps not UTC millis: %q %q", body.Tenant.CreatedAt, body.Tenant.UpdatedAt)
	}
}

func TestCreateTenantDuplicatesAndBodies(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})
	if rec := post(t, h, "/v1/tenants", `{"slug":"acme-corp","name":"Acme"}`); rec.Code != http.StatusCreated {
		t.Fatalf("seed: %d", rec.Code)
	}

	requireError(t, post(t, h, "/v1/tenants", `{"slug":"acme-corp","name":"Other"}`),
		http.StatusConflict, "tenant_already_exists")
	requireError(t, post(t, h, "/v1/tenants", `{"slug":"acme","name":`),
		http.StatusBadRequest, "invalid_request")
	requireError(t, post(t, h, "/v1/tenants", `{"slug":"acme","name":"A","extra":true}`),
		http.StatusBadRequest, "invalid_request")
	requireError(t, post(t, h, "/v1/tenants", `{"slug":42,"name":"A"}`),
		http.StatusBadRequest, "invalid_request")
	requireError(t, request(t, h, http.MethodPost, "/v1/tenants", `{"slug":"acme","name":"A"}`, "text/plain"),
		http.StatusBadRequest, "invalid_request")
	requireError(t, request(t, h, http.MethodPost, "/v1/tenants", `{"slug":"acme","name":"A"}`, ""),
		http.StatusBadRequest, "invalid_request")
	requireError(t, post(t, h, "/v1/tenants", `{"slug":"acme","name":"A"} {"x":1}`),
		http.StatusBadRequest, "invalid_request")
}

func TestCreateTenantValidation(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})

	cases := []struct {
		body  string
		field string
	}{
		{`{"slug":"A","name":"Acme"}`, "slug"},
		{`{"slug":"1acme","name":"Acme"}`, "slug"},
		{`{"slug":"ac me","name":"Acme"}`, "slug"},
		{`{"slug":"","name":""}`, "slug"},
		{`{"slug":"acme","name":""}`, "name"},
		{`{"slug":"acme","name":"   "}`, "name"},
		{`{"slug":"acme","name":"` + strings.Repeat("x", 65) + `"}`, "name"},
	}
	for _, c := range cases {
		body := requireError(t, post(t, h, "/v1/tenants", c.body), http.StatusUnprocessableEntity, "validation_error")
		if body.Error.Details["field"] != c.field {
			t.Fatalf("body %s: field = %v, want %s", c.body, body.Error.Details["field"], c.field)
		}
	}
}

func TestGetTenant(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})
	rec := post(t, h, "/v1/tenants", `{"slug":"acme-corp","name":"Acme"}`)
	var created mutationJSON
	decodeInto(t, rec, &created)

	rec = request(t, h, http.MethodGet, "/v1/tenants/"+created.Tenant.ID, "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	var got tenantJSON
	decodeInto(t, rec, &got)
	if got != created.Tenant {
		t.Fatalf("got %+v, want %+v", got, created.Tenant)
	}

	requireError(t, request(t, h, http.MethodGet, "/v1/tenants/"+uuid.NewString(), "", ""),
		http.StatusNotFound, "tenant_not_found")
	requireError(t, request(t, h, http.MethodGet, "/v1/tenants/not-a-uuid", "", ""),
		http.StatusNotFound, "tenant_not_found")
}
