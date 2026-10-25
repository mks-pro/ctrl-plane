// Package broker owns the AMQP connection, topology declaration and a
// confirm-mode publisher. Both binaries use it so the wire properties are
// identical everywhere.
package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Message is one publish request.
type Message struct {
	Exchange   string
	RoutingKey string
	MessageID  string
	Type       string
	AppID      string
	Body       []byte
	Headers    amqp.Table
}

// Client lazily (re)connects one shared publish connection in confirm mode.
type Client struct {
	url    string
	topo   Topology
	logger *slog.Logger

	mu   sync.Mutex
	conn *amqp.Connection
	ch   *amqp.Channel
}

// NewClient builds a client; call Connect to fail fast at startup.
func NewClient(url string, topo Topology, logger *slog.Logger) *Client {
	if logger == nil {
		logger = slog.Default()
	}
	return &Client{url: url, topo: topo, logger: logger}
}

// Connect dials, declares the topology and enables publisher confirms.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connectLocked(ctx)
}

func (c *Client) connectLocked(ctx context.Context) error {
	if c.conn != nil && !c.conn.IsClosed() {
		return nil
	}
	conn, err := amqp.DialConfig(c.url, amqp.Config{
		Properties: amqp.Table{"connection_name": "ctrlplane-publisher"},
	})
	if err != nil {
		return fmt.Errorf("dial amqp: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}
	if err := c.topo.Declare(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return err
	}
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("enable confirms: %w", err)
	}
	c.conn, c.ch = conn, ch
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.logger.Info("amqp publisher connected")
	return nil
}

// IsOpen reports whether the publish connection is currently usable. Used by
// /readyz only; publishing reconnects on demand.
func (c *Client) IsOpen() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil && !c.conn.IsClosed() && c.ch != nil && !c.ch.IsClosed()
}

// Publish sends one persistent message and waits for the broker confirm. A
// nack or a closed channel is an error; the caller decides how to retry.
func (c *Client) Publish(ctx context.Context, m Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connectLocked(ctx); err != nil {
		return err
	}
	publishing := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    m.MessageID,
		Type:         m.Type,
		AppId:        m.AppID,
		Timestamp:    time.Now().UTC(),
		Headers:      m.Headers,
		Body:         m.Body,
	}
	conf, err := c.ch.PublishWithDeferredConfirmWithContext(
		ctx, m.Exchange, m.RoutingKey, false, false, publishing)
	if err != nil {
		c.invalidateLocked()
		return fmt.Errorf("publish: %w", err)
	}
	acked, err := conf.WaitContext(ctx)
	if err != nil {
		return err
	}
	if !acked {
		c.invalidateLocked()
		return errors.New("publish not confirmed (nack or closed channel)")
	}
	return nil
}

func (c *Client) invalidateLocked() {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.ch, c.conn = nil, nil
}

// Close releases the publish connection.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.invalidateLocked()
}

// Consumer is a dedicated connection + channel used for consuming. It is
// separate from the publish connection so a consumer cancel cannot break
// publishing.
type Consumer struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

// OpenConsumer dials, declares the topology and starts consuming queue with
// manual acknowledgements and the requested prefetch window.
func OpenConsumer(url string, topo Topology, queue, tag string, prefetch int) (*Consumer, <-chan amqp.Delivery, error) {
	conn, err := amqp.DialConfig(url, amqp.Config{
		Properties: amqp.Table{"connection_name": "ctrlplane-consumer"},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("dial amqp: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, fmt.Errorf("open consumer channel: %w", err)
	}
	if err := topo.Declare(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, err
	}
	if err := ch.Qos(prefetch, 0, false); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("set qos: %w", err)
	}
	deliveries, err := ch.Consume(queue, tag, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return nil, nil, fmt.Errorf("consume %s: %w", queue, err)
	}
	return &Consumer{conn: conn, ch: ch}, deliveries, nil
}

// Close shuts the consumer channel and connection.
func (c *Consumer) Close() {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}
