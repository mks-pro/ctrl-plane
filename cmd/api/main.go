// Command api serves the HTTP control plane and runs the outbox relay plus the
// task.result consumer inside the same process.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/your-org/ctrlplane/internal/api"
	"github.com/your-org/ctrlplane/internal/broker"
	"github.com/your-org/ctrlplane/internal/config"
	"github.com/your-org/ctrlplane/internal/consumer"
	"github.com/your-org/ctrlplane/internal/domain"
	"github.com/your-org/ctrlplane/internal/outbox"
	"github.com/your-org/ctrlplane/internal/store"
)

func main() {
	cfg, err := config.LoadAPI()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config: %v\n", err)
		os.Exit(1)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel})).
		With("service", "api", "env", cfg.AppEnv)
	if err := run(cfg, logger); err != nil {
		logger.Error("api stopped with error", "error", err)
		os.Exit(1)
	}
	logger.Info("api stopped")
}

func run(cfg config.Config, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns, cfg.Exchange, domain.SystemClock{}, logger)
	if err != nil {
		return err
	}
	defer st.Close()

	pub := broker.NewClient(cfg.AMQPURL, cfg.Topology(), logger)
	if err := pub.Connect(ctx); err != nil {
		return err
	}
	defer pub.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	relay := outbox.New(st, pub, cfg.OutboxPollInterval, cfg.OutboxBatchSize, logger)
	go func() {
		defer wg.Done()
		relay.Run(ctx)
	}()

	resultConsumer := consumer.New(
		cfg.AMQPURL, cfg.Topology(), cfg.ResultsQueue,
		fmt.Sprintf("ctrlplane-api-%d", os.Getpid()),
		cfg.ConsumerPrefetch, cfg.ConsumerMaxAttempts,
		cfg.ConsumerRetryBase, cfg.ConsumerRetryMax,
		st, logger,
	)
	go func() {
		defer wg.Done()
		if err := resultConsumer.Run(ctx); err != nil {
			logger.Error("result consumer stopped", "error", err)
		}
	}()

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(st, pub, domain.SystemClock{}, logger).Routes(),
		ReadHeaderTimeout: cfg.HTTPReadTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       60 * time.Second,
	}
	serveErr := make(chan error, 1)
	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		err := srv.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveErr <- err
	}()

	var runErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serveErr:
		runErr = err
		if err != nil {
			logger.Error("http server failed", "error", err)
		}
	}

	stop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.HTTPShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "error", err)
	}
	wg.Wait()
	return runErr
}
