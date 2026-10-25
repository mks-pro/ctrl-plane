// Command worker consumes task.created messages, simulates work and publishes
// task.result messages. It has no database access.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/your-org/ctrlplane/internal/broker"
	"github.com/your-org/ctrlplane/internal/config"
	"github.com/your-org/ctrlplane/internal/worker"
)

func main() {
	cfg, err := config.LoadWorker()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}

	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	failRate := fs.Float64("fail-rate", cfg.WorkerFailRate, "probability a task fails, [0,1]")
	duplicateRate := fs.Float64("duplicate-rate", cfg.WorkerDuplicateRate, "probability a result is published twice, [0,1]")
	minDelay := fs.Duration("min-delay", cfg.WorkerMinDelay, "minimum simulated work duration")
	maxDelay := fs.Duration("max-delay", cfg.WorkerMaxDelay, "maximum simulated work duration")
	seed := fs.Int64("seed", cfg.WorkerSeed, "deterministic simulation seed")
	workerID := fs.String("worker-id", cfg.WorkerID, "worker identity reported in results")
	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(2)
	}
	cfg.WorkerFailRate = *failRate
	cfg.WorkerDuplicateRate = *duplicateRate
	cfg.WorkerMinDelay = *minDelay
	cfg.WorkerMaxDelay = *maxDelay
	cfg.WorkerSeed = *seed
	cfg.WorkerID = *workerID

	if err := cfg.ValidateWorker(); err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(2)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("service", "worker", "env", cfg.AppEnv, "worker_id", cfg.WorkerID)
	logger.Info("worker configuration",
		"fail_rate", cfg.WorkerFailRate,
		"duplicate_rate", cfg.WorkerDuplicateRate,
		"min_delay", cfg.WorkerMinDelay,
		"max_delay", cfg.WorkerMaxDelay,
		"seed", cfg.WorkerSeed)

	if err := run(cfg, logger); err != nil {
		logger.Error("worker stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("worker stopped")
}

func run(cfg config.Config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pub := broker.NewClient(cfg.AMQPURL, cfg.Topology(), logger)
	if err := pub.Connect(ctx); err != nil {
		return err
	}
	defer pub.Close()

	proc := worker.NewProcessor(worker.Options{
		WorkerID:      cfg.WorkerID,
		FailRate:      cfg.WorkerFailRate,
		DuplicateRate: cfg.WorkerDuplicateRate,
		MinDelay:      cfg.WorkerMinDelay,
		MaxDelay:      cfg.WorkerMaxDelay,
		Seed:          cfg.WorkerSeed,
	})
	cons := worker.NewConsumer(
		cfg.AMQPURL, cfg.Topology(), cfg.TasksQueue,
		"ctrlplane-worker-"+cfg.WorkerID,
		cfg.ConsumerPrefetch, cfg.ConsumerMaxAttempts,
		cfg.ConsumerRetryBase, cfg.ConsumerRetryMax,
		proc, pub, logger,
	)
	return cons.Run(ctx)
}
