-- 0001_init.up.sql — tenants, tasks, transactional outbox, consumer inbox.
-- Status and type values are constrained here as well as in internal/domain:
-- the domain decides, the database guarantees.

CREATE TABLE tenants (
    id         uuid        PRIMARY KEY,
    slug       text        NOT NULL,
    name       text        NOT NULL,
    status     text        NOT NULL,
    version    integer     NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT tenants_slug_uniq        UNIQUE (slug),
    CONSTRAINT tenants_slug_format      CHECK (slug ~ '^[a-z][-a-z0-9]{1,26}[a-z0-9]$'),
    CONSTRAINT tenants_name_nonempty    CHECK (btrim(name) <> ''),
    CONSTRAINT tenants_status_valid     CHECK (status IN
        ('provisioning','active','updating','destroying','destroyed','failed')),
    CONSTRAINT tenants_version_positive CHECK (version > 0)
);

CREATE TABLE tasks (
    id         uuid        PRIMARY KEY,
    tenant_id  uuid        NOT NULL REFERENCES tenants (id),
    type       text        NOT NULL,
    status     text        NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT tasks_type_valid   CHECK (type IN ('deploy','update','destroy')),
    CONSTRAINT tasks_status_valid CHECK (status IN ('accepted','in_progress','done','failed'))
);

CREATE INDEX tasks_tenant_created_idx ON tasks (tenant_id, created_at DESC, id DESC);
CREATE INDEX tasks_created_idx        ON tasks (created_at DESC, id DESC);
CREATE UNIQUE INDEX tasks_one_open_per_tenant
    ON tasks (tenant_id) WHERE status IN ('accepted','in_progress');

CREATE TABLE outbox_events (
    id           uuid        PRIMARY KEY,
    topic        text        NOT NULL,
    routing_key  text        NOT NULL,
    payload      jsonb       NOT NULL,
    headers      jsonb       NOT NULL DEFAULT '{}'::jsonb,
    created_at   timestamptz NOT NULL,
    published_at timestamptz,
    attempts     integer     NOT NULL DEFAULT 0,
    last_error   text
);

CREATE INDEX outbox_unpublished_idx ON outbox_events (created_at)
    WHERE published_at IS NULL;

CREATE TABLE processed_updates (
    event_id   uuid        PRIMARY KEY,
    task_id    uuid        NOT NULL REFERENCES tasks (id),
    outcome    text        NOT NULL,
    applied_at timestamptz NOT NULL,
    CONSTRAINT processed_outcome_valid CHECK (outcome IN
        ('in_progress','done','failed','ignored'))
);
