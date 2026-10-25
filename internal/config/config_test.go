package config_test

import (
	"strings"
	"testing"
	"time"

	"github.com/your-org/ctrlplane/internal/config"
)

var envKeys = []string{
	"APP_ENV", "LOG_LEVEL", "HTTP_ADDR", "HTTP_READ_TIMEOUT", "HTTP_WRITE_TIMEOUT",
	"HTTP_SHUTDOWN_TIMEOUT", "DATABASE_URL", "DB_MAX_CONNS", "AMQP_URL", "AMQP_EXCHANGE",
	"AMQP_TASKS_QUEUE", "AMQP_TASKS_DLQ", "AMQP_RESULTS_QUEUE", "AMQP_RESULTS_DLQ",
	"OUTBOX_POLL_INTERVAL", "OUTBOX_BATCH_SIZE", "CONSUMER_PREFETCH", "CONSUMER_MAX_ATTEMPTS",
	"CONSUMER_RETRY_BASE", "CONSUMER_RETRY_MAX", "WORKER_FAIL_RATE", "WORKER_MIN_DELAY_MS",
	"WORKER_MAX_DELAY_MS", "WORKER_SEED", "WORKER_ID", "WORKER_DUPLICATE_RATE",
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range envKeys {
		t.Setenv(k, "")
	}
}

func minimalAPI(t *testing.T) {
	t.Helper()
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost:5432/db")
	t.Setenv("AMQP_URL", "amqp://guest:guest@localhost:5672/")
}

func TestLoadAPIDefaults(t *testing.T) {
	minimalAPI(t)
	cfg, err := config.LoadAPI()
	if err != nil {
		t.Fatalf("LoadAPI: %v", err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.AppEnv != "dev" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.DBMaxConns != 10 || cfg.OutboxBatchSize != 50 || cfg.ConsumerPrefetch != 10 || cfg.ConsumerMaxAttempts != 5 {
		t.Fatalf("unexpected integer defaults: %+v", cfg)
	}
	if cfg.OutboxPollInterval != 250*time.Millisecond {
		t.Fatalf("poll interval = %s", cfg.OutboxPollInterval)
	}
	if cfg.ConsumerRetryBase != 100*time.Millisecond || cfg.ConsumerRetryMax != 3*time.Second {
		t.Fatalf("retry defaults = %s/%s", cfg.ConsumerRetryBase, cfg.ConsumerRetryMax)
	}
	if cfg.Exchange != "controlplane" || cfg.TasksQueue != "controlplane.tasks" ||
		cfg.TasksDLQ != "controlplane.tasks.dlq" || cfg.ResultsQueue != "controlplane.task-results" ||
		cfg.ResultsDLQ != "controlplane.task-results.dlq" {
		t.Fatalf("unexpected topology defaults: %+v", cfg)
	}
	if cfg.WorkerMinDelay != time.Second || cfg.WorkerMaxDelay != 5*time.Second {
		t.Fatalf("worker delay defaults = %s/%s", cfg.WorkerMinDelay, cfg.WorkerMaxDelay)
	}
	if cfg.WorkerID == "" || !strings.Contains(cfg.WorkerID, "-") {
		t.Fatalf("worker id = %q", cfg.WorkerID)
	}
}

func TestLoadAPIRequiresConnectionStrings(t *testing.T) {
	clearEnv(t)
	if _, err := config.LoadAPI(); err == nil || !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Fatalf("missing DATABASE_URL error = %v", err)
	}
	t.Setenv("DATABASE_URL", "postgres://u:p@localhost/db")
	if _, err := config.LoadAPI(); err == nil || !strings.Contains(err.Error(), "AMQP_URL") {
		t.Fatalf("missing AMQP_URL error = %v", err)
	}
}

func TestLoadAPIOverrides(t *testing.T) {
	minimalAPI(t)
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("HTTP_ADDR", "127.0.0.1:9999")
	t.Setenv("DB_MAX_CONNS", "3")
	t.Setenv("OUTBOX_POLL_INTERVAL", "1s")
	t.Setenv("OUTBOX_BATCH_SIZE", "7")
	t.Setenv("CONSUMER_RETRY_BASE", "20ms")
	t.Setenv("CONSUMER_RETRY_MAX", "40ms")
	t.Setenv("AMQP_EXCHANGE", "x")
	cfg, err := config.LoadAPI()
	if err != nil {
		t.Fatalf("LoadAPI: %v", err)
	}
	if cfg.HTTPAddr != "127.0.0.1:9999" || cfg.DBMaxConns != 3 || cfg.OutboxBatchSize != 7 {
		t.Fatalf("overrides not applied: %+v", cfg)
	}
	if cfg.OutboxPollInterval != time.Second || cfg.ConsumerRetryBase != 20*time.Millisecond {
		t.Fatalf("duration overrides not applied: %+v", cfg)
	}
	if cfg.Exchange != "x" {
		t.Fatalf("exchange = %q", cfg.Exchange)
	}
	if cfg.LogLevel.String() != "DEBUG" {
		t.Fatalf("log level = %s", cfg.LogLevel)
	}
}

func TestLoadRejectsBadValues(t *testing.T) {
	cases := []struct{ key, value string }{
		{"LOG_LEVEL", "loud"},
		{"DB_MAX_CONNS", "many"},
		{"DB_MAX_CONNS", "0"},
		{"OUTBOX_POLL_INTERVAL", "soon"},
		{"OUTBOX_POLL_INTERVAL", "0s"},
		{"CONSUMER_RETRY_BASE", "5s"},
	}
	for _, c := range cases {
		minimalAPI(t)
		if c.value != "" {
			t.Setenv(c.key, c.value)
		}
		if _, err := config.LoadAPI(); err == nil {
			t.Fatalf("%s=%q should be rejected", c.key, c.value)
		}
	}
}

func TestValidateWorker(t *testing.T) {
	base := config.Config{WorkerMinDelay: time.Second, WorkerMaxDelay: 2 * time.Second}
	if err := base.ValidateWorker(); err != nil {
		t.Fatalf("valid worker config rejected: %v", err)
	}
	cases := []config.Config{
		{WorkerFailRate: 1.5, WorkerMaxDelay: time.Second},
		{WorkerDuplicateRate: -0.1, WorkerMaxDelay: time.Second},
		{WorkerFailRate: 0.5, WorkerMinDelay: 3 * time.Second, WorkerMaxDelay: time.Second},
		{WorkerSeed: -1, WorkerMaxDelay: time.Second},
	}
	for i, c := range cases {
		if err := c.ValidateWorker(); err == nil {
			t.Fatalf("case %d should be rejected: %+v", i, c)
		}
	}
}
