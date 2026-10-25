# ctrlplane — multi-tenant control plane

A small, complete control plane for tenant lifecycle operations. An HTTP API persists the desired
state, a transactional outbox hands work to RabbitMQ, a worker simulates the long-running operation,
and the API consumes the result and converges the tenant state.

```
client ──HTTP──▶ api ──── tx ────▶ PostgreSQL ────▶ outbox relay ────▶ RabbitMQ (controlplane.tasks)
                  ▲                    │                                       │
                  │                    └── processed_updates / tasks / tenants  ▼
                  └──── task.result consumer ◀──── RabbitMQ (controlplane.task-results) ◀── worker
```

* `cmd/api` — HTTP server + outbox relay + `task.result` consumer (one process).
* `cmd/worker` — consumes `task.created`, simulates work, and publishes `in_progress` then `done` (or a single `failed` on the failure path).
* `cmd/migrate` — applies the embedded SQL migrations once each (`schema_migrations`, advisory lock).

## Guarantees

| Concern | Mechanism |
| --- | --- |
| API state change and event publication are atomic | tenant row + task row + outbox row are written in one Postgres transaction; nothing is published from the request path |
| Events are published at least once | relay claims rows with `FOR UPDATE SKIP LOCKED`, publishes with publisher confirms, then stamps `published_at` |
| Duplicate results are harmless | `processed_updates.event_id` is a primary key; a second delivery inserts nothing and is acknowledged as `duplicate` |
| Out-of-order results do not regress state | `EvaluateResult` freezes terminal tasks, and the tenant update is guarded by the expected source status |
| Concurrent writers cannot lose an update | `PATCH` requires the current `version`; the store performs a compare-and-swap (`version = version + 1 WHERE version = $expected`) |
| Poison messages do not loop forever | deliveries are retried with capped exponential backoff up to `CONSUMER_MAX_ATTEMPTS`, then dead-lettered to the `*.dlq` queues |
| Startup is safe | migrations are idempotent, `readyz` reports which dependency is down, shutdown is graceful |

## Tenant and task state machines

```
tenant: provisioning ──deploy done──▶ active ──patch──▶ updating ──update done──▶ active
              │                         │                  │
              └──deploy failed──▶ failed └──delete──▶ destroying ──destroy done──▶ destroyed
                                         failed ──delete──▶ destroying ──destroy done──▶ destroyed
```

* `PATCH /v1/tenants/{id}` is only allowed from `active` (409 `tenant_update_not_allowed` otherwise).
* `DELETE /v1/tenants/{id}` is allowed from `active` and `failed`; a destroyed tenant stays `destroyed`
  and its slug stays reserved.
* Task statuses: `accepted → in_progress → done | failed`; `accepted → done`/`failed` are also accepted
  when progress is skipped (the failure path emits `failed` directly). Terminal tasks ignore further
  results.

## Repository layout

```
cmd/api            HTTP server, outbox relay, result consumer
cmd/worker         task consumer + simulated processor
cmd/migrate        migration runner (up / --down)
internal/api       routing, decoding, DTOs, error envelope (HTTP only)
internal/domain    state machines, validation, event payloads (no I/O)
internal/store     pgx store: tenants, tasks, outbox, result application
internal/outbox    relay loop (claim → publish → mark)
internal/broker    AMQP client, topology declaration, confirms
internal/consumer  generic AMQP consumer: decode, retry, dedupe acks, DLQ
internal/worker    task processor + task consumer wiring
migrations         embedded SQL (0001_init.up/down.sql)
docs/              design documents (project, context, architecture, plan, progress-tracker, agent, changelog)
```

## Prerequisites

* Go `1.26` (the module pins `go 1.26.2`; verified with `go1.26.2`).
* PostgreSQL 16 (verified with 16.15; any recent version works).
* RabbitMQ 3.12+ (AMQP 0-9-1; topology is declared at startup, the management plugin is optional).
* `psql`, `curl` and `jq` for the walkthrough below.

No Docker Compose, Makefile or external linter is required: everything runs with the Go toolchain and
the two services above.

## Configuration

The binaries read configuration from the environment (see `.env.example` for a template):

| Variable | Default | Used by | Notes |
| --- | --- | --- | --- |
| `DATABASE_URL` | — | api, migrate | **required**; append `?search_path=app` to keep the schema isolated |
| `AMQP_URL` | — | api, worker | **required**, e.g. `amqp://user:pass@127.0.0.1:5672/` |
| `HTTP_ADDR` | `:8080` | api | listen address |
| `HTTP_READ_TIMEOUT` | `10s` | api | `http.Server` read timeout |
| `HTTP_WRITE_TIMEOUT` | `15s` | api | `http.Server` write timeout |
| `HTTP_SHUTDOWN_TIMEOUT` | `15s` | api | graceful-drain budget |
| `APP_ENV` | `dev` | both | added to every log record |
| `LOG_LEVEL` | `info` | both | `debug`, `info`, `warn`, `error` |
| `DB_MAX_CONNS` | `10` | api | pgx pool size |
| `OUTBOX_POLL_INTERVAL` | `250ms` | api | relay poll period |
| `OUTBOX_BATCH_SIZE` | `50` | api | rows claimed per batch |
| `CONSUMER_PREFETCH` | `10` | api, worker | unacked window |
| `CONSUMER_MAX_ATTEMPTS` | `5` | api, worker | deliveries before DLQ |
| `CONSUMER_RETRY_BASE` / `CONSUMER_RETRY_MAX` | `100ms` / `3s` | api, worker | backoff bounds |
| `AMQP_EXCHANGE` | `controlplane` | both | topic exchange |
| `AMQP_TASKS_QUEUE` / `AMQP_TASKS_DLQ` | `controlplane.tasks` / `controlplane.tasks.dlq` | both | task routing |
| `AMQP_RESULTS_QUEUE` / `AMQP_RESULTS_DLQ` | `controlplane.task-results` / `controlplane.task-results.dlq` | both | result routing |
| `WORKER_ID` | `<hostname>-<pid>` | worker | identity written into result events |
| `WORKER_FAIL_RATE` / `WORKER_DUPLICATE_RATE` | `0` | worker | simulation knobs, `[0,1]` |
| `WORKER_MIN_DELAY_MS` / `WORKER_MAX_DELAY_MS` | `1000ms` / `5000ms` | worker | simulated work duration; Go duration syntax (`1000ms`) |
| `WORKER_SEED` | `0` | worker | hash salt for the deterministic per-task decision; must be ≥ 0 |
| `TEST_DATABASE_URL` | — | tests only | enables store/API integration tests |

Worker flags override the corresponding environment variables:
`--worker-id`, `--fail-rate`, `--duplicate-rate`, `--min-delay`, `--max-delay`, `--seed`.
`cmd/migrate` accepts `--database-url`, `--down` and `--timeout`.

## Running it locally

The example below uses a local PostgreSQL on `127.0.0.1:5432` and RabbitMQ on `127.0.0.1:5672`.
Replace the credentials with your own; **never commit real credentials**. It assumes the PostgreSQL
role and database, and the RabbitMQ user/vhost, already exist; create them with your usual admin
tooling (`createuser` / `createdb`, `rabbitmqctl add_user` / `set_permissions`) if they do not.

```bash
# 1. One database, one private schema for this service.
psql 'postgres://ctrlplane:ctrlplane@127.0.0.1:5432/saas_task' -c 'CREATE SCHEMA IF NOT EXISTS app'

# 2. Shared configuration for every command.
cat > .env <<'EOF'
export DATABASE_URL='postgres://ctrlplane:ctrlplane@127.0.0.1:5432/saas_task?sslmode=disable&search_path=app'
export AMQP_URL='amqp://ctrlplane:ctrlplane@127.0.0.1:5672/'
export HTTP_ADDR=':8090'
EOF
source .env

# 3. Apply migrations (idempotent; safe to re-run).
go run ./cmd/migrate

# 4. Terminal A — API, outbox relay and result consumer.
go run ./cmd/api

# 5. Terminal B — worker (fast simulation for demos).
go run ./cmd/worker --min-delay 200ms --max-delay 800ms --worker-id local-1
```

Readiness and liveness:

```bash
curl -sS localhost:8090/healthz
curl -sS localhost:8090/readyz | jq
# {"status":"ready"}
```

`/readyz` returns `503` with `{"error":{"code":"service_unavailable","details":{"check":"database"}}}`
when a dependency is down; `/healthz` only proves the process is alive.

## API walkthrough

Every mutating request returns the tenant snapshot and the task created for it. `POST`, `PATCH` and
`DELETE` answer `201`/`202` with a `Location: /v1/tenants/{id}` header.

```bash
BASE=http://127.0.0.1:8090
J='Content-Type: application/json'

# Create -> provisioning, task type=deploy
curl -sS -D /tmp/h -o /tmp/t.json -X POST "$BASE/v1/tenants" -H "$J" \
  -d '{"slug":"acme","name":"Acme Corp"}'
jq '{tenant: .tenant|{id,status,version}, task: .task|{id,type,status}}' /tmp/t.json
ID=$(jq -r .tenant.id /tmp/t.json)
grep -i '^location' /tmp/h

# The worker finishes the deploy; the consumer flips the tenant to active/version 2.
for _ in $(seq 1 30); do
  [ "$(curl -sS "$BASE/v1/tenants/$ID" | jq -r .status)" = active ] && break
  sleep 1
done
curl -sS "$BASE/v1/tenants/$ID" | jq '{status,version}'

# Update requires the current version (optimistic locking).
curl -sS -X PATCH "$BASE/v1/tenants/$ID" -H "$J" \
  -d '{"version":2,"name":"Acme Corp Ltd"}' | jq '.tenant|{status,version}'
# -> updating, 3; then active, 4 once the update task completes (poll as above).
for _ in $(seq 1 30); do
  [ "$(curl -sS "$BASE/v1/tenants/$ID" | jq -r .status)" = active ] && break
  sleep 1
done

# Two more tenants give the cursor a second page.
for slug in beta gamma; do
  curl -sS -o /dev/null -X POST "$BASE/v1/tenants" -H "$J" \
    -d "{\"slug\":\"$slug\",\"name\":\"$slug\"}"
done
for _ in $(seq 1 30); do
  [ "$(curl -sS "$BASE/v1/tenants?status=active&limit=10" | jq '.items|length')" -ge 3 ] && break
  sleep 1
done

# List with filters and keyset pagination.
PAGE=$(curl -sS "$BASE/v1/tenants?status=active&limit=2")
echo "$PAGE" | jq '{slugs: [.items[].slug], next_cursor}'
CURSOR=$(echo "$PAGE" | jq -r .next_cursor)
curl -sS "$BASE/v1/tenants?limit=2&cursor=$CURSOR" | jq .

# Tasks of one tenant.
curl -sS "$BASE/v1/tasks?tenant_id=$ID&limit=10" | jq '.items[]|{type,status}'

# Delete -> destroying, then destroyed. A second DELETE is a 409.
curl -sS -X DELETE "$BASE/v1/tenants/$ID" | jq '.tenant|{status,version}'
curl -sS -o /dev/null -w '%{http_code}\n' -X DELETE "$BASE/v1/tenants/$ID"   # 409
for _ in $(seq 1 30); do
  [ "$(curl -sS "$BASE/v1/tenants/$ID" | jq -r .status)" = destroyed ] && break
  sleep 1
done
curl -sS "$BASE/v1/tenants/$ID" | jq '{status,version}'
```

Endpoints: `GET /healthz`, `GET /readyz`, `POST|GET /v1/tenants`, `GET|PATCH|DELETE /v1/tenants/{id}`,
`GET /v1/tasks`, `GET /v1/tasks/{id}`.

Errors always use one envelope:

```json
{"error":{"code":"tenant_version_conflict","message":"...","details":{"expected_version":2,"current_version":3}}}
```

`422 validation_error` (with `details.field`), `409 tenant_version_conflict`,
`409 tenant_update_not_allowed`, `409 tenant_already_exists`, `404 tenant_not_found`,
`404 task_not_found`, `404 route_not_found`, `405 method_not_allowed`,
`400 invalid_request` (malformed body / wrong content type / unknown field), `500 internal_error`, `503 service_unavailable`.


## Testing

### Hermetic suite (no infrastructure)

```bash
go test ./... -count=1
go test ./... -count=1 -race
gofmt -l .          # must print nothing
go vet ./...
```

This runs the domain state-machine tests, handler tests with a fake store, consumer/worker tests with
fake acks, store, and publisher, migration tests, and config tests. Store and API integration tests are
**skipped** unless `TEST_DATABASE_URL` is set.

### Integration suite (Postgres)

The integration tests only need a disposable database. Every test recreates its package's private
schema (`ctrlplane_store_it`, `ctrlplane_api_it`) before running, so `go test ./...` may run packages in
parallel; the schemas stay behind for the next run.

```bash
psql 'postgres://ctrlplane:ctrlplane@127.0.0.1:5432/postgres' -c 'CREATE DATABASE saas_task_test'  # once

TEST_DATABASE_URL='postgres://ctrlplane:ctrlplane@127.0.0.1:5432/saas_task_test?sslmode=disable' \
  go test ./... -count=1 -race
```

Concurrency acceptance criteria, specifically:

```bash
TEST_DATABASE_URL='postgres://ctrlplane:ctrlplane@127.0.0.1:5432/saas_task_test?sslmode=disable' \
  go test ./internal/api -count=1 -race -run 'TestConcurrent' -v
```

* `TestConcurrentCreateSameSlug` — N parallel `POST` requests: exactly one `201`, every other response is
  `409 tenant_already_exists` (the unique index arbitrates, not application code).
* `TestConcurrentPatchSameVersion` — two `PATCH` requests with the same expected version: exactly one
  `202`, one `409 tenant_version_conflict`, and the tenant version advances by exactly one.

No broker is needed for the test suites: broker-facing logic is covered hermetically with fakes, and the
real broker path is exercised end-to-end by the smoke run below.

### End-to-end smoke run

With PostgreSQL, RabbitMQ, the API and at least one worker running (see *Running it locally*), the
walkthrough in *API walkthrough* is the smoke test: create → `active`, patch → `updating` → `active`,
delete → `destroying` → `destroyed`. Add state checks at any time:

```bash
# pgx understands DATABASE_URL's ?search_path=app; psql does not, so pass it via PGOPTIONS.
PSQL_URL='postgres://ctrlplane:ctrlplane@127.0.0.1:5432/saas_task?sslmode=disable'
export PGOPTIONS='-c search_path=app'

psql "$PSQL_URL" -x -c 'SELECT slug, status, version FROM tenants ORDER BY created_at DESC LIMIT 5'
psql "$PSQL_URL" -c 'SELECT type, status FROM tasks ORDER BY created_at DESC LIMIT 5'
psql "$PSQL_URL" -c 'SELECT event_id, outcome FROM processed_updates ORDER BY applied_at DESC LIMIT 5'
psql "$PSQL_URL" -c 'SELECT count(*) FROM outbox_events WHERE published_at IS NULL'
```

A healthy run leaves no unpublished outbox rows, each event has `attempts = 1` (publish succeeded on the
first try), task results show `in_progress` followed by `done`, and the task queues drain to zero.

### Failure-injection demos

The worker knobs make the at-least-once and failure paths reproducible without external tooling. Run
these **instead of** the normal worker (stop it first), otherwise the healthy worker can consume the
same tasks and mask the injected outcome:

```bash
# Deploy fails -> tenant becomes failed (version 2); DELETE is still allowed.
go run ./cmd/worker --fail-rate 1 --min-delay 10ms --max-delay 20ms --seed 3

# The same result is published twice. The duplicate event id hits the processed_updates
# primary key: it is acknowledged, never applied twice, and adds no new inbox row.
go run ./cmd/worker --duplicate-rate 1 --min-delay 10ms --max-delay 20ms --seed 4
```

Inspect what happened:

```bash
psql "$PSQL_URL" -c "SELECT outcome, count(*) FROM processed_updates GROUP BY 1 ORDER BY 1"
# done | failed | in_progress | ignored
# `ignored` = a late result for an already terminal task; a same-id redelivery is not inserted at all.
```

Messages that cannot be decoded, or that fail permanently, are retried with capped exponential backoff
and then dead-lettered:

```bash
# Host install:
rabbitmqctl list_queues name messages_ready messages_unacknowledged arguments
# Broker running in a container (NAME from `docker ps`):
docker exec NAME rabbitmqctl list_queues name messages_ready messages_unacknowledged arguments
# Management API (requires rabbitmq_management):
curl -s -u user:pass 'http://127.0.0.1:15672/api/queues/%2F/controlplane.tasks.dlq' | jq '{messages}'
# controlplane.tasks, controlplane.tasks.dlq, controlplane.task-results, controlplane.task-results.dlq
```

## Design notes and trade-offs

* **Outbox instead of publishing from the handler.** The request transaction only writes rows; the relay
  publishes after commit and stamps `published_at`. A crash between commit and publish costs latency,
  never consistency. `FOR UPDATE SKIP LOCKED` lets several relay replicas run safely.
* **At-least-once delivery with a dedupe inbox.** RabbitMQ redelivers after crashes; `processed_updates`
  (primary key on the event id) makes duplicate application impossible. The consumer acknowledges
  duplicates instead of retrying them.
* **Optimistic locking over row locks in HTTP.** `PATCH` requires the version the client saw. The store
  compares and swaps inside one statement; the loser gets `409` with both versions.
* **Guarded state transitions.** The task state machine freezes terminal tasks; the tenant update is
  guarded by the expected source status (`provisioning`/`updating`/`destroying`), so a late result can
  never move a tenant backwards.
* **One process for API + relay + result consumer.** It keeps the demo simple; each part is a separate
  component and can be split without code changes because they communicate only through Postgres and
  RabbitMQ.
* **Worker has no database access.** It only consumes task events and publishes result events, which
  keeps the simulated work isolated and horizontally scalable.

Known limitations: there is no authentication or TLS (out of scope for the exercise), the database schema
must exist before `cmd/migrate` runs, slugs are never reused after deletion, and the worker's work is
simulated rather than real provisioning.

## Verified results

* `gofmt -l .` — clean; `go vet ./...` — clean.
* `go test ./... -count=1` and `go test ./... -count=1 -race` — all packages pass.
* `TEST_DATABASE_URL=… go test ./... -count=1 -race` — 50/50 top-level tests pass (0 skipped,
  0 failed) in 7 packages with test files, including the concurrency tests.
* Live smoke run against PostgreSQL and RabbitMQ — 91/91 assertions passed on 2026-10-25:
  create/patch/delete lifecycle, optimistic-locking conflicts, validation and routing errors, keyset
  pagination and filters, duplicate-result dedupe, outbox fully published, and queue drain. The earlier
  clean-database run passed 77/77 on 2026-10-24.
* Process restart — a tenant created and brought to `active` (version 2) was unchanged after stopping
  and restarting the API and worker, and matched the PostgreSQL row.
* Graceful shutdown — `SIGTERM` to the API and to the worker returned in 103 ms and 104 ms with active
  consumers; the idle re-run on 2026-10-25 returned in 3 ms and 2 ms. Both logged their stop message
  instead of waiting on an open AMQP delivery channel.
* Clean-database state after the 2026-10-24 smoke — one applied migration; 3 tenants, 6 tasks,
  6 outbox rows (all published on the first attempt), 11 dedupe rows; all four queues were empty.
* Re-run state on 2026-10-25 — 5 new tasks and 10 processed outcomes (`in_progress` + `done` per
  task); the injected duplicate `done` result reused its deterministic `event_id` and was deduped,
  unpublished outbox rows = 0, and all four queues were empty after shutdown.


