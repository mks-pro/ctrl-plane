package api

import (
	"context"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/google/uuid"

	"github.com/your-org/ctrlplane/internal/domain"
	"github.com/your-org/ctrlplane/internal/store"
)

// Store is the persistence contract the handlers depend on. It is satisfied by
// *store.Store and by fakes in tests; no pgx type appears here.
type Store interface {
	CreateTenant(ctx context.Context, slug, name string) (domain.Tenant, domain.Task, error)
	GetTenant(ctx context.Context, id uuid.UUID) (domain.Tenant, error)
	ListTenants(ctx context.Context, f store.TenantFilter) (store.Page[domain.Tenant], error)
	UpdateTenant(ctx context.Context, id uuid.UUID, version int, name string) (domain.Tenant, domain.Task, error)
	DeleteTenant(ctx context.Context, id uuid.UUID) (domain.Tenant, domain.Task, error)
	GetTask(ctx context.Context, id uuid.UUID) (domain.Task, error)
	ListTasks(ctx context.Context, f store.TaskFilter) (store.Page[domain.Task], error)
	Ping(ctx context.Context) error
}

// Broker reports publisher connectivity for /readyz.
type Broker interface {
	IsOpen() bool
}

// Handler holds the dependencies shared by every endpoint.
type Handler struct {
	store  Store
	broker Broker
	clock  domain.Clock
	logger *slog.Logger
}

// New wires the handler dependencies.
func New(st Store, br Broker, clock domain.Clock, logger *slog.Logger) *Handler {
	if clock == nil {
		clock = domain.SystemClock{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{store: st, broker: br, clock: clock, logger: logger}
}

// Routes builds the router. chi supplies request ids, route/method fallbacks
// and the middleware chain; handlers stay plain net/http.
func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(h.recoverer)
	r.Use(h.requestLogger)
	r.Use(bodyLimit(maxBodyBytes))

	r.Get("/healthz", h.health)
	r.Get("/readyz", h.ready)

	r.Route("/v1", func(r chi.Router) {
		r.Post("/tenants", h.createTenant)
		r.Get("/tenants", h.listTenants)
		r.Get("/tenants/{id}", h.getTenant)
		r.Patch("/tenants/{id}", h.updateTenant)
		r.Delete("/tenants/{id}", h.deleteTenant)
		r.Get("/tasks", h.listTasks)
		r.Get("/tasks/{id}", h.getTask)
	})

	r.NotFound(func(w http.ResponseWriter, r *http.Request) {
		h.writeError(w, r, domain.ErrRouteNotFound)
	})
	r.MethodNotAllowed(func(w http.ResponseWriter, r *http.Request) {
		h.writeError(w, r, domain.ErrMethodNotAllowed)
	})
	return r
}

func requestID(r *http.Request) string {
	return middleware.GetReqID(r.Context())
}
