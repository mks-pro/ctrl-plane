package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
	"github.com/your-org/ctrlplane/internal/store"
)

const maxBodyBytes = 1 << 20

// decodeJSON enforces content type, body size, unknown-field rejection and
// exactly-one-JSON-value. All failures map to invalid_request (400).
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return domain.ErrMalformedBody
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return domain.ErrMalformedBody
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return domain.ErrMalformedBody
	}
	return nil
}

func parseIDParam(r *http.Request, name string, notFound error) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue(name))
	if err != nil || r.PathValue(name) == "" {
		return uuid.Nil, notFound
	}
	return id, nil
}

// encodeCursor renders base64url("<unix_micros>:<uuid>").
func encodeCursor(p store.Position) string {
	raw := fmt.Sprintf("%d:%s", p.At.UTC().UnixMicro(), p.ID)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func parseCursor(raw string) (*store.Position, error) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil {
		return nil, domain.Invalid("cursor", "is not valid base64url")
	}
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 {
		return nil, domain.Invalid("cursor", "is malformed")
	}
	micros, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || micros <= 0 {
		return nil, domain.Invalid("cursor", "is malformed")
	}
	id, err := uuid.Parse(parts[1])
	if err != nil {
		return nil, domain.Invalid("cursor", "is malformed")
	}
	return &store.Position{At: time.UnixMicro(micros).UTC(), ID: id}, nil
}

func parseTenantFilter(r *http.Request) (store.TenantFilter, error) {
	q := r.URL.Query()
	limit, err := domain.ParseLimit(q.Get("limit"))
	if err != nil {
		return store.TenantFilter{}, err
	}
	f := store.TenantFilter{Limit: limit}
	if v := q.Get("status"); v != "" {
		st, err := domain.ParseTenantStatus(v)
		if err != nil {
			return store.TenantFilter{}, err
		}
		f.Status = &st
	}
	if v := q.Get("slug"); v != "" {
		f.Slug = &v
	}
	if v := q.Get("cursor"); v != "" {
		pos, err := parseCursor(v)
		if err != nil {
			return store.TenantFilter{}, err
		}
		f.After = pos
	}
	return f, nil
}

func parseTaskFilter(r *http.Request) (store.TaskFilter, error) {
	q := r.URL.Query()
	limit, err := domain.ParseLimit(q.Get("limit"))
	if err != nil {
		return store.TaskFilter{}, err
	}
	f := store.TaskFilter{Limit: limit}
	if v := q.Get("tenant_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			return store.TaskFilter{}, domain.Invalid("tenant_id", "must be a UUID")
		}
		f.TenantID = &id
	}
	if v := q.Get("type"); v != "" {
		tt, err := domain.ParseTaskType(v)
		if err != nil {
			return store.TaskFilter{}, err
		}
		f.Type = &tt
	}
	if v := q.Get("status"); v != "" {
		st, err := domain.ParseTaskStatus(v)
		if err != nil {
			return store.TaskFilter{}, err
		}
		f.Status = &st
	}
	if v := q.Get("cursor"); v != "" {
		pos, err := parseCursor(v)
		if err != nil {
			return store.TaskFilter{}, err
		}
		f.After = pos
	}
	return f, nil
}
