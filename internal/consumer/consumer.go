// Package consumer applies task.result events. It owns the ack policy: applied
// -> ack, duplicate -> ack, permanent -> DLQ, transient -> bounded retries.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/your-org/ctrlplane/internal/broker"
	"github.com/your-org/ctrlplane/internal/domain"
	"github.com/your-org/ctrlplane/internal/store"
)

// MaxBodyBytes is the inbound payload cap; larger messages are poison.
const MaxBodyBytes = 64 * 1024

// Store is the subset of the repository the consumer needs.
type Store interface {
	ApplyResult(ctx context.Context, ev domain.ResultEvent) (store.ResultOutcome, error)
}

// Consumer runs the result queue loop with manual acknowledgements.
type Consumer struct {
	url         string
	topo        broker.Topology
	queue       string
	tag         string
	prefetch    int
	maxAttempts int
	retryBase   time.Duration
	retryMax    time.Duration
	store       Store
	logger      *slog.Logger
}

// New builds a consumer. Defaults are applied for non-positive tuning values.
func New(url string, topo broker.Topology, queue, tag string, prefetch, maxAttempts int,
	retryBase, retryMax time.Duration, st Store, logger *slog.Logger,
) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	if prefetch <= 0 {
		prefetch = 10
	}
	if maxAttempts <= 0 {
		maxAttempts = 5
	}
	if retryBase <= 0 {
		retryBase = 100 * time.Millisecond
	}
	if retryMax < retryBase {
		retryMax = 3 * time.Second
	}
	return &Consumer{
		url: url, topo: topo, queue: queue, tag: tag, prefetch: prefetch,
		maxAttempts: maxAttempts, retryBase: retryBase, retryMax: retryMax,
		store: st, logger: logger,
	}
}

// Run consumes until the context is cancelled, reconnecting on channel loss.
func (c *Consumer) Run(ctx context.Context) error {
	wait := time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}
		cons, deliveries, err := broker.OpenConsumer(c.url, c.topo, c.queue, c.tag, c.prefetch)
		if err != nil {
			c.logger.Error("consumer connect failed", "error", err, "retry_in", wait)
			if !sleepCtx(ctx, wait) {
				return nil
			}
			wait *= 2
			if wait > 5*time.Second {
				wait = 5 * time.Second
			}
			continue
		}
		wait = time.Second
		c.logger.Info("consumer started", "queue", c.queue, "prefetch", c.prefetch)
		// Closing the channel unblocks the deliveries range. Without this, a
		// cancelled context leaves Run waiting forever for the next delivery.
		stopCloser := closeOnCancel(ctx, cons)
		for d := range deliveries {
			c.handle(ctx, d)
		}
		stopCloser()
		if ctx.Err() != nil {
			return nil
		}
		c.logger.Warn("consumer channel closed; reconnecting")
	}
}

// closeOnCancel closes cons when ctx is cancelled, which ends the deliveries
// channel and lets Run return promptly during shutdown. The returned function
// stops the watcher; Close is guarded so the normal and cancelled paths can
// both call it safely.
func closeOnCancel(ctx context.Context, cons *broker.Consumer) func() {
	done := make(chan struct{})
	var once sync.Once
	closeConsumer := func() { once.Do(cons.Close) }
	go func() {
		select {
		case <-ctx.Done():
			closeConsumer()
		case <-done:
		}
	}()
	return func() {
		close(done)
		closeConsumer()
	}
}

func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) {
	defer func() {
		if r := recover(); r != nil {
			c.logger.Error("panic while handling result", "panic", r)
			_ = d.Nack(false, true)
		}
	}()

	ev, err := DecodeResult(d.Body)
	if err != nil {
		c.logger.Warn("poison message: invalid payload", "error", err)
		_ = d.Nack(false, false)
		return
	}

	applyCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := c.store.ApplyResult(applyCtx, ev)
	if err == nil {
		_ = d.Ack(false)
		c.logger.Debug("result handled",
			"event_id", ev.EventID, "task_id", ev.TaskID, "outcome", out.Outcome,
			"duplicate", out.Duplicate, "tenant_updated", out.TenantUpdated)
		return
	}
	if permanent(err) {
		c.logger.Warn("poison message: cannot apply result",
			"event_id", ev.EventID, "task_id", ev.TaskID, "error", err)
		_ = d.Nack(false, false)
		return
	}

	attempt := deliveryAttempt(d)
	if attempt >= c.maxAttempts {
		c.logger.Error("result exhausted retries; dead-lettering",
			"event_id", ev.EventID, "task_id", ev.TaskID, "attempt", attempt, "error", err)
		_ = d.Nack(false, false)
		return
	}
	delay := backoff(c.retryBase, c.retryMax, attempt)
	c.logger.Warn("transient result failure; retrying",
		"event_id", ev.EventID, "task_id", ev.TaskID, "attempt", attempt, "error", err, "retry_in", delay)
	if !sleepCtx(ctx, delay) {
		_ = d.Nack(false, true)
		return
	}
	_ = d.Nack(false, true)
}

// DecodeResult enforces the size cap, JSON shape and contract validation.
func DecodeResult(body []byte) (domain.ResultEvent, error) {
	if len(body) == 0 {
		return domain.ResultEvent{}, fmt.Errorf("%w: empty body", domain.ErrInvalidEvent)
	}
	if len(body) > MaxBodyBytes {
		return domain.ResultEvent{}, fmt.Errorf("%w: body exceeds %d bytes", domain.ErrInvalidEvent, MaxBodyBytes)
	}
	var ev domain.ResultEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return domain.ResultEvent{}, fmt.Errorf("%w: %v", domain.ErrInvalidEvent, err)
	}
	if err := ev.Validate(); err != nil {
		return domain.ResultEvent{}, err
	}
	return ev, nil
}

func permanent(err error) bool {
	return errors.Is(err, domain.ErrTaskNotFound) ||
		errors.Is(err, domain.ErrTenantMismatch) ||
		errors.Is(err, domain.ErrInvalidEvent) ||
		errors.Is(err, domain.ErrInvalid)
}

func deliveryAttempt(d amqp.Delivery) int {
	if v, ok := d.Headers["x-delivery-count"]; ok {
		switch n := v.(type) {
		case int64:
			return int(n) + 1
		case int32:
			return int(n) + 1
		case int:
			return n + 1
		}
	}
	if d.Redelivered {
		return 2
	}
	return 1
}

func backoff(base, max time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		d = max
	}
	return time.Duration(float64(d) * (0.8 + rand.Float64()*0.4))
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
