package config

import "github.com/your-org/ctrlplane/internal/broker"

// Topology returns the broker object names derived from this configuration.
func (c Config) Topology() broker.Topology {
	return broker.Topology{
		Exchange:     c.Exchange,
		TasksQueue:   c.TasksQueue,
		TasksDLQ:     c.TasksDLQ,
		ResultsQueue: c.ResultsQueue,
		ResultsDLQ:   c.ResultsDLQ,
	}
}
