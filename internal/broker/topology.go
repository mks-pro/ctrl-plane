package broker

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Topology names every broker object this system declares. The API and the
// worker declare the identical topology at startup; a mismatch is a
// 406 PRECONDITION_FAILED and fails fast, which is the desired behavior.
type Topology struct {
	Exchange     string
	TasksQueue   string
	TasksDLQ     string
	ResultsQueue string
	ResultsDLQ   string
}

// DefaultTopology mirrors the documented defaults.
func DefaultTopology() Topology {
	return Topology{
		Exchange:     "controlplane",
		TasksQueue:   "controlplane.tasks",
		TasksDLQ:     "controlplane.tasks.dlq",
		ResultsQueue: "controlplane.task-results",
		ResultsDLQ:   "controlplane.task-results.dlq",
	}
}

// DLX is the dead-letter exchange paired with Exchange.
func (t Topology) DLX() string { return t.Exchange + ".dlx" }

// Routing keys.
const (
	RoutingTaskCreated = "task.created"
	RoutingTaskResult  = "task.result"
	RoutingTasksDead   = "task.created.dead"
	RoutingResultsDead = "task.result.dead"
)

// Declare creates exchanges, queues and bindings. It is idempotent.
func (t Topology) Declare(ch *amqp.Channel) error {
	for _, name := range []string{t.Exchange, t.DLX()} {
		if err := ch.ExchangeDeclare(name, "topic", true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare exchange %s: %w", name, err)
		}
	}
	workQueues := []struct {
		name string
		dead string
	}{
		{t.TasksQueue, RoutingTasksDead},
		{t.ResultsQueue, RoutingResultsDead},
	}
	for _, q := range workQueues {
		args := amqp.Table{
			"x-queue-type":              "quorum",
			"x-delivery-limit":          int32(5),
			"x-dead-letter-exchange":    t.DLX(),
			"x-dead-letter-routing-key": q.dead,
		}
		if _, err := ch.QueueDeclare(q.name, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare queue %s: %w", q.name, err)
		}
	}
	for _, name := range []string{t.TasksDLQ, t.ResultsDLQ} {
		args := amqp.Table{"x-queue-type": "quorum"}
		if _, err := ch.QueueDeclare(name, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare queue %s: %w", name, err)
		}
	}
	bindings := []struct {
		queue, exchange, key string
	}{
		{t.TasksQueue, t.Exchange, RoutingTaskCreated},
		{t.ResultsQueue, t.Exchange, RoutingTaskResult},
		{t.TasksDLQ, t.DLX(), RoutingTasksDead},
		{t.ResultsDLQ, t.DLX(), RoutingResultsDead},
	}
	for _, b := range bindings {
		if err := ch.QueueBind(b.queue, b.key, b.exchange, false, nil); err != nil {
			return fmt.Errorf("bind %s to %s (%s): %w", b.queue, b.exchange, b.key, err)
		}
	}
	return nil
}
