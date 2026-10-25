package api_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
)

func TestListTenantFiltersAndPaging(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})

	base := time.Now().UTC().Truncate(time.Microsecond)
	for i := 0; i < 3; i++ {
		id := uuid.New()
		status := domain.TenantProvisioning
		if i == 2 {
			status = domain.TenantActive
		}
		st.tenants[id] = domain.Tenant{
			ID: id, Slug: "slug-" + string(rune('a'+i)), Name: "N",
			Status: status, Version: 1,
			CreatedAt: base.Add(time.Duration(i) * time.Minute),
			UpdatedAt: base.Add(time.Duration(i) * time.Minute),
		}
	}

	rec := request(t, h, http.MethodGet, "/v1/tenants?status=active", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", rec.Code, rec.Body.String())
	}
	var page tenantListJSON
	decodeInto(t, rec, &page)
	if len(page.Items) != 1 || page.Items[0].Status != "active" || page.NextCursor != nil {
		t.Fatalf("unexpected page: %+v", page)
	}
	if f := st.tenantFilters[len(st.tenantFilters)-1]; f.Status == nil || *f.Status != domain.TenantActive {
		t.Fatalf("status filter not forwarded: %+v", f)
	}

	rec = request(t, h, http.MethodGet, "/v1/tenants?limit=2", "", "")
	decodeInto(t, rec, &page)
	if rec.Code != http.StatusOK || len(page.Items) != 2 || page.NextCursor == nil {
		t.Fatalf("first page: %d %+v", rec.Code, page)
	}
	first := page.Items[0].ID

	rec = request(t, h, http.MethodGet, "/v1/tenants?limit=2&cursor="+*page.NextCursor, "", "")
	decodeInto(t, rec, &page)
	if rec.Code != http.StatusOK || len(page.Items) != 1 || page.NextCursor != nil {
		t.Fatalf("second page: %d %+v", rec.Code, page)
	}
	if page.Items[0].ID == first {
		t.Fatalf("cursor did not advance")
	}

	rec = request(t, h, http.MethodGet, "/v1/tenants?slug=missing-slug", "", "")
	decodeInto(t, rec, &page)
	if rec.Code != http.StatusOK || len(page.Items) != 0 {
		t.Fatalf("empty page: %d %+v", rec.Code, page)
	}
	if !strings.Contains(rec.Body.String(), `"items":[]`) {
		t.Fatalf("items must encode as []: %s", rec.Body.String())
	}
}

func TestListTenantBadParameters(t *testing.T) {
	st := newFakeStore()
	h := newHandler(t, st, fakeBroker{open: true})
	cases := []struct{ query, field string }{
		{"limit=0", "limit"},
		{"limit=abc", "limit"},
		{"limit=-1", "limit"},
		{"limit=101", "limit"},
		{"status=bogus", "status"},
		{"cursor=!!", "cursor"},
	}
	for _, c := range cases {
		body := requireError(t, request(t, h, http.MethodGet, "/v1/tenants?"+c.query, "", ""),
			http.StatusUnprocessableEntity, "validation_error")
		if body.Error.Details["field"] != c.field {
			t.Fatalf("%s: field = %v, want %s", c.query, body.Error.Details["field"], c.field)
		}
	}
}
