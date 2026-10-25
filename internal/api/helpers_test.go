package api_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/your-org/ctrlplane/internal/api"
	"github.com/your-org/ctrlplane/internal/domain"
)

// apiError is the decoded error envelope.
type apiError struct {
	Error struct {
		Code    string         `json:"code"`
		Message string         `json:"message"`
		Details map[string]any `json:"details"`
	} `json:"error"`
}

func newHandler(t *testing.T, st *fakeStore, broker api.Broker) http.Handler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return api.New(st, broker, domain.SystemClock{}, logger).Routes()
}

func request(t *testing.T, h http.Handler, method, path, body, contentType string) *httptest.ResponseRecorder {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func post(t *testing.T, h http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return request(t, h, http.MethodPost, path, body, "application/json")
}

func patch(t *testing.T, h http.Handler, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	return request(t, h, http.MethodPatch, path, body, "application/json")
}

type tenantJSON struct {
	ID        string `json:"id"`
	Slug      string `json:"slug"`
	Name      string `json:"name"`
	Status    string `json:"status"`
	Version   int    `json:"version"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type taskJSON struct {
	ID        string `json:"id"`
	TenantID  string `json:"tenant_id"`
	Type      string `json:"type"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

type mutationJSON struct {
	Tenant tenantJSON `json:"tenant"`
	Task   taskJSON   `json:"task"`
}

type tenantListJSON struct {
	Items      []tenantJSON `json:"items"`
	NextCursor *string      `json:"next_cursor"`
}

var timestampPattern = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$`)

func decodeInto(t *testing.T, rec *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), dst); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
}

func decodeAPIError(t *testing.T, rec *httptest.ResponseRecorder) apiError {
	t.Helper()
	var body apiError
	decodeInto(t, rec, &body)
	return body
}

func requireError(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) apiError {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, status, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type = %q", ct)
	}
	body := decodeAPIError(t, rec)
	if body.Error.Code != code {
		t.Fatalf("code = %q, want %q; body=%s", body.Error.Code, code, rec.Body.String())
	}
	if body.Error.Message == "" {
		t.Fatalf("error message must not be empty")
	}
	return body
}
