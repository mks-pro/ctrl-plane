SHELL := /bin/sh

COMPOSE ?= docker compose
TEST_DB ?= saas_task_test

.DEFAULT_GOAL := help

.PHONY: help up down clean logs psql queues test test-hermetic

help:
	@printf '%s\n' \
		'ctrlplane targets:' \
		'  make up            build and start PostgreSQL, RabbitMQ, migrate, API, worker' \
		'  make down          stop the stack, keep named volumes' \
		'  make clean         stop the stack and delete named volumes' \
		'  make logs          follow API and worker logs' \
		'  make psql          open psql in the Compose Postgres service' \
		'  make queues        show queue depth through rabbitmqctl' \
		'  make test          one-command quality gate in a Go container + disposable test DB' \
		'  make test-hermetic host-only gate (no Docker, no Postgres)'

up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs -f api worker

psql:
	$(COMPOSE) exec postgres psql -U ctrlplane -d saas_task

queues:
	$(COMPOSE) exec rabbitmq rabbitmqctl list_queues name messages_ready messages_unacknowledged arguments

# Starts the Compose dependencies, ensures the disposable test database exists,
# then runs the full gate inside the Go test image. The source tree is mounted
# read-only, so this target cannot rewrite go.mod/go.sum or the working tree.
test:
	@command -v docker >/dev/null 2>&1 || { \
		echo "docker is required for 'make test'; use 'make test-hermetic' on a Go-only host" >&2; \
		exit 1; \
	}
	@docker info >/dev/null 2>&1 || { \
		echo "Docker daemon is not reachable by this user; add the user to the docker group or run 'make test-hermetic'" >&2; \
		exit 1; \
	}
	@$(COMPOSE) up -d --wait --wait-timeout 90 postgres
	@i=0; until $(COMPOSE) exec -T postgres pg_isready -U ctrlplane -d saas_task >/dev/null 2>&1; do \
		i=$$((i + 1)); \
		if [ $$i -ge 30 ]; then \
			echo "postgres did not become ready" >&2; \
			exit 1; \
		fi; \
		sleep 1; \
	done
	@$(COMPOSE) exec -T postgres psql -U ctrlplane -d postgres -tAc \
		"SELECT 1 FROM pg_database WHERE datname='$(TEST_DB)'" | grep -q 1 || \
		$(COMPOSE) exec -T postgres createdb -U ctrlplane -O ctrlplane '$(TEST_DB)'
	@$(COMPOSE) --profile test run --rm test

test-hermetic:
	@out="$$(gofmt -l .)"; \
	if [ -n "$$out" ]; then \
		echo "gofmt required:"; \
		echo "$$out"; \
		exit 1; \
	fi
	go vet ./...
	go mod tidy -diff
	go test ./... -count=1 -race
