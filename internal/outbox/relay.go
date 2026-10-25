// Package outbox runs the transactional-outbox relay: it polls unpublished
// rows, publishes them with confirms, and stops a batch at the first failure so
// events cannot overtake each other.
package outbox

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/your-org/ctrlplane/internal/broker"
	"github.com/your-org/ctrlplane/internal/store"
)

// Store is the subset of the repository the relay needs.
type Store interface {
	ProcessOutboxBatch(ctx context.Context, limit int, publish store.PublishFunc) (int, bool, error)
}

// Publisher is the subset of the broker client the relay needs.
type Publisher interface {
	Publish(ctx context.Context, m broker.Message) error
}

// Relay polls the outbox table forever until its context is cancelled.
type Relay struct {
	store    Store
	pub      Publisher
	interval time.Duration
	batch    int
	logger   *slog.Logger
}

// New builds a relay. interval is the idle poll cadence and the initial retry
// backoff; batch is the maximum rows per transaction.
func New(st Store, pub Publisher, interval time.Duration, batch int, logger *slog.Logger) *Relay {
	if logger == nil {
		logger = slog.Default()
	}
	if interval <= 0 {
		interval = 250 * time.Millisecond
	}
	if batch <= 0 {
		batch = 50
	}
	return &Relay{store: st, pub: pub, interval: interval, batch: batch, logger: logger}
}

// Run executes the polling loop. It returns nil when ctx is cancelled.
func (r *Relay) Run(ctx context.Context) {
	backoff := r.interval
	for {
		published, full, err := r.store.ProcessOutboxBatch(ctx, r.batch, r.publish)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			r.logger.Warn("outbox batch failed",
				"error", err, "published", published, "retry_in", backoff)
			if !sleep(ctx, jitter(backoff)) {
				return
			}
			backoff *= 2
			if backoff > 5*time.Second {
				backoff = 5 * time.Second
			}
			continue
		}
		backoff = r.interval
		if published > 0 {
			r.logger.Debug("outbox batch published", "count", published, "full", full)
		}
		if full {
			continue // drain the backlog without waiting
		}
		if !sleep(ctx, r.interval) {
			return
		}
	}
}

func (r *Relay) publish(ctx context.Context, e store.OutboxEvent) error {
	headers := amqp.Table{}
	var meta struct {
		SchemaVersion int `json:"schema_version"`
	}
	if len(e.Headers) > 0 {
		if err := json.Unmarshal(e.Headers, &meta); err != nil {
			return err
		}
	}
	headers["schema_version"] = int32(meta.SchemaVersion)
	return r.pub.Publish(ctx, broker.Message{
		Exchange:   e.Topic,
		RoutingKey: e.RoutingKey,
		MessageID:  e.ID.String(),
		Type:       e.RoutingKey,
		AppID:      "ctrlplane-api",
		Body:       e.Payload,
		Headers:    headers,
	})
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// jitter adds +/-20% noise so several relays do not poll in lockstep.
func jitter(d time.Duration) time.Duration {
	factor := 0.8 + rand.Float64()*0.4
	return time.Duration(float64(d) * factor)
}
