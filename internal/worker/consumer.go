package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/your-org/ctrlplane/internal/broker"
	"github.com/your-org/ctrlplane/internal/domain"
)

// MaxBodyBytes caps inbound task payloads.
const MaxBodyBytes = 64 * 1024

// Publisher is the confirms-based publisher used for results.
type Publisher interface {
	Publish(ctx context.Context, m broker.Message) error
}

// Consumer consumes task.created and publishes task.result.
type Consumer struct {
	url         string
	topo        broker.Topology
	queue       string
	tag         string
	prefetch    int
	maxAttempts int
	retryBase   time.Duration
	retryMax    time.Duration
	proc        *Processor
	pub         Publisher
	logger      *slog.Logger
}

// NewConsumer builds the task consumer.
func NewConsumer(url string, topo broker.Topology, queue, tag string, prefetch, maxAttempts int,
	retryBase, retryMax time.Duration, proc *Processor, pub Publisher, logger *slog.Logger,
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
		proc: proc, pub: pub, logger: logger,
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
			c.logger.Error("task consumer connect failed", "error", err, "retry_in", wait)
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
		c.logger.Info("task consumer started", "queue", c.queue, "prefetch", c.prefetch)
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
		c.logger.Warn("task consumer channel closed; reconnecting")
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
			c.logger.Error("panic while handling task", "panic", r)
			_ = d.Nack(false, true)
		}
	}()

	ev, err := DecodeTaskCreated(d.Body)
	if err != nil {
		c.logger.Warn("poison message: invalid task.created", "error", err)
		_ = d.Nack(false, false)
		return
	}

	events, err := c.proc.Process(ctx, ev)
	if err != nil {
		if ctx.Err() != nil {
			_ = d.Nack(false, true)
			return
		}
		c.retry(ctx, d, ev, fmt.Errorf("simulate work: %w", err))
		return
	}
	for _, result := range events {
		if err := c.publish(ctx, result); err != nil {
			c.retry(ctx, d, ev, fmt.Errorf("publish result %s: %w", result.EventID, err))
			return
		}
	}
	_ = d.Ack(false)
	c.logger.Info("task processed",
		"task_id", ev.Task.ID, "tenant_id", ev.Task.TenantID,
		"type", ev.Task.Type, "results", len(events))
}

func (c *Consumer) publish(ctx context.Context, ev domain.ResultEvent) error {
	body, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	return c.pub.Publish(ctx, broker.Message{
		Exchange:   c.topo.Exchange,
		RoutingKey: broker.RoutingTaskResult,
		MessageID:  ev.EventID.String(),
		Type:       domain.EventTaskResult,
		AppID:      "ctrlplane-worker",
		Body:       body,
		Headers:    amqp.Table{"schema_version": int32(domain.SchemaVersion)},
	})
}

// retry requeues after a bounded backoff, or dead-letters once exhausted.
func (c *Consumer) retry(ctx context.Context, d amqp.Delivery, ev domain.TaskCreatedEvent, cause error) {
	attempt := deliveryAttempt(d)
	if attempt >= c.maxAttempts {
		c.logger.Error("task exhausted retries; dead-lettering",
			"task_id", ev.Task.ID, "attempt", attempt, "error", cause)
		_ = d.Nack(false, false)
		return
	}
	delay := backoff(c.retryBase, c.retryMax, attempt)
	c.logger.Warn("task handling failed; retrying",
		"task_id", ev.Task.ID, "attempt", attempt, "error", cause, "retry_in", delay)
	if !sleepCtx(ctx, delay) {
		_ = d.Nack(false, true)
		return
	}
	_ = d.Nack(false, true)
}

// DecodeTaskCreated enforces size, JSON shape and contract validation.
func DecodeTaskCreated(body []byte) (domain.TaskCreatedEvent, error) {
	if len(body) == 0 {
		return domain.TaskCreatedEvent{}, fmt.Errorf("%w: empty body", domain.ErrInvalidEvent)
	}
	if len(body) > MaxBodyBytes {
		return domain.TaskCreatedEvent{}, fmt.Errorf("%w: body exceeds %d bytes", domain.ErrInvalidEvent, MaxBodyBytes)
	}
	var ev domain.TaskCreatedEvent
	if err := json.Unmarshal(body, &ev); err != nil {
		return domain.TaskCreatedEvent{}, fmt.Errorf("%w: %v", domain.ErrInvalidEvent, err)
	}
	if err := ev.Validate(); err != nil {
		return domain.TaskCreatedEvent{}, err
	}
	return ev, nil
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
			d = max
			break
		}
	}
	if d > max {
		d = max
	}
	return time.Duration(float64(d) * (0.8 + rand.Float64()*0.4))
}
