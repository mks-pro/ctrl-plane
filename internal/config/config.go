// Package config parses environment configuration with defaults. There is no
// config file: precedence is CLI flag (worker only) -> environment -> default.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the full runtime configuration. API-only fields are ignored by the
// worker and vice versa.
type Config struct {
	AppEnv   string
	LogLevel slog.Level

	HTTPAddr            string
	HTTPReadTimeout     time.Duration
	HTTPWriteTimeout    time.Duration
	HTTPShutdownTimeout time.Duration

	DatabaseURL string
	DBMaxConns  int32

	AMQPURL      string
	Exchange     string
	TasksQueue   string
	TasksDLQ     string
	ResultsQueue string
	ResultsDLQ   string

	OutboxPollInterval time.Duration
	OutboxBatchSize    int

	ConsumerPrefetch    int
	ConsumerMaxAttempts int
	ConsumerRetryBase   time.Duration
	ConsumerRetryMax    time.Duration

	WorkerFailRate      float64
	WorkerMinDelay      time.Duration
	WorkerMaxDelay      time.Duration
	WorkerSeed          int64
	WorkerID            string
	WorkerDuplicateRate float64
}

// LoadAPI loads and validates the configuration required by the API process.
func LoadAPI() (Config, error) {
	cfg, err := load()
	if err != nil {
		return Config{}, err
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	if cfg.AMQPURL == "" {
		return Config{}, fmt.Errorf("AMQP_URL is required")
	}
	if cfg.DBMaxConns < 1 {
		return Config{}, fmt.Errorf("DB_MAX_CONNS must be at least 1")
	}
	if cfg.OutboxBatchSize < 1 {
		return Config{}, fmt.Errorf("OUTBOX_BATCH_SIZE must be at least 1")
	}
	if cfg.ConsumerPrefetch < 1 {
		return Config{}, fmt.Errorf("CONSUMER_PREFETCH must be at least 1")
	}
	if cfg.ConsumerMaxAttempts < 1 {
		return Config{}, fmt.Errorf("CONSUMER_MAX_ATTEMPTS must be at least 1")
	}
	if cfg.ConsumerRetryBase > cfg.ConsumerRetryMax {
		return Config{}, fmt.Errorf("CONSUMER_RETRY_BASE must not exceed CONSUMER_RETRY_MAX")
	}
	return cfg, nil
}

// LoadWorker loads and validates the configuration required by the worker.
func LoadWorker() (Config, error) {
	cfg, err := load()
	if err != nil {
		return Config{}, err
	}
	if cfg.AMQPURL == "" {
		return Config{}, fmt.Errorf("AMQP_URL is required")
	}
	return cfg, nil
}

// ValidateWorker checks worker-only settings after flag parsing.
func (c Config) ValidateWorker() error {
	if c.WorkerFailRate < 0 || c.WorkerFailRate > 1 {
		return fmt.Errorf("fail rate must be within [0,1]")
	}
	if c.WorkerDuplicateRate < 0 || c.WorkerDuplicateRate > 1 {
		return fmt.Errorf("duplicate rate must be within [0,1]")
	}
	if c.WorkerMinDelay < 0 || c.WorkerMaxDelay < 0 {
		return fmt.Errorf("delays must not be negative")
	}
	if c.WorkerMinDelay > c.WorkerMaxDelay {
		return fmt.Errorf("min delay must not exceed max delay")
	}
	if c.WorkerSeed < 0 {
		return fmt.Errorf("seed must not be negative")
	}
	return nil
}

func load() (Config, error) {
	level, err := parseLevel(getenv("LOG_LEVEL", "info"))
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		AppEnv:   getenv("APP_ENV", "dev"),
		LogLevel: level,
		HTTPAddr: getenv("HTTP_ADDR", ":8080"),
	}
	durations := []struct {
		key    string
		def    time.Duration
		target *time.Duration
	}{
		{"HTTP_READ_TIMEOUT", 10 * time.Second, &cfg.HTTPReadTimeout},
		{"HTTP_WRITE_TIMEOUT", 15 * time.Second, &cfg.HTTPWriteTimeout},
		{"HTTP_SHUTDOWN_TIMEOUT", 15 * time.Second, &cfg.HTTPShutdownTimeout},
		{"OUTBOX_POLL_INTERVAL", 250 * time.Millisecond, &cfg.OutboxPollInterval},
		{"CONSUMER_RETRY_BASE", 100 * time.Millisecond, &cfg.ConsumerRetryBase},
		{"CONSUMER_RETRY_MAX", 3 * time.Second, &cfg.ConsumerRetryMax},
		{"WORKER_MIN_DELAY_MS", 1000 * time.Millisecond, &cfg.WorkerMinDelay},
		{"WORKER_MAX_DELAY_MS", 5000 * time.Millisecond, &cfg.WorkerMaxDelay},
	}
	for _, d := range durations {
		v, err := envDuration(d.key, d.def)
		if err != nil {
			return Config{}, err
		}
		*d.target = v
	}
	ints := []struct {
		key    string
		def    int
		target *int
	}{
		{"OUTBOX_BATCH_SIZE", 50, &cfg.OutboxBatchSize},
		{"CONSUMER_PREFETCH", 10, &cfg.ConsumerPrefetch},
		{"CONSUMER_MAX_ATTEMPTS", 5, &cfg.ConsumerMaxAttempts},
	}
	for _, f := range ints {
		v, err := envInt(f.key, f.def)
		if err != nil {
			return Config{}, err
		}
		*f.target = v
	}
	conns, err := envInt("DB_MAX_CONNS", 10)
	if err != nil {
		return Config{}, err
	}
	cfg.DBMaxConns = int32(conns)
	failRate, err := envFloat("WORKER_FAIL_RATE", 0)
	if err != nil {
		return Config{}, err
	}
	dupRate, err := envFloat("WORKER_DUPLICATE_RATE", 0)
	if err != nil {
		return Config{}, err
	}
	seed, err := envInt64("WORKER_SEED", 0)
	if err != nil {
		return Config{}, err
	}
	cfg.WorkerFailRate = failRate
	cfg.WorkerDuplicateRate = dupRate
	cfg.WorkerSeed = seed
	cfg.DatabaseURL = os.Getenv("DATABASE_URL")
	cfg.AMQPURL = os.Getenv("AMQP_URL")
	cfg.Exchange = getenv("AMQP_EXCHANGE", "controlplane")
	cfg.TasksQueue = getenv("AMQP_TASKS_QUEUE", "controlplane.tasks")
	cfg.TasksDLQ = getenv("AMQP_TASKS_DLQ", "controlplane.tasks.dlq")
	cfg.ResultsQueue = getenv("AMQP_RESULTS_QUEUE", "controlplane.task-results")
	cfg.ResultsDLQ = getenv("AMQP_RESULTS_DLQ", "controlplane.task-results.dlq")
	cfg.WorkerID = getenv("WORKER_ID", defaultWorkerID())
	if cfg.HTTPReadTimeout <= 0 || cfg.HTTPWriteTimeout <= 0 || cfg.HTTPShutdownTimeout <= 0 {
		return Config{}, fmt.Errorf("HTTP timeouts must be positive")
	}
	if cfg.OutboxPollInterval <= 0 || cfg.ConsumerRetryBase <= 0 || cfg.ConsumerRetryMax <= 0 {
		return Config{}, fmt.Errorf("intervals must be positive")
	}
	return cfg, nil
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "worker"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}

func getenv(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return def
}

func parseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("LOG_LEVEL %q is not one of debug, info, warn, error", s)
	}
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func envInt(key string, def int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return n, nil
}

func envInt64(key string, def int64) (int64, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def, nil
	}
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer: %w", key, err)
	}
	return n, nil
}

func envFloat(key string, def float64) (float64, error) {
	raw, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(raw) == "" {
		return def, nil
	}
	f, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be a number: %w", key, err)
	}
	return f, nil
}
