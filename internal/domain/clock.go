package domain

import "time"

// Clock exists so timestamps are injectable in tests. Production uses
// SystemClock; tests use a fixed clock and get byte-identical JSON.
type Clock interface {
	Now() time.Time
}

// SystemClock returns the current UTC time.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now().UTC() }

// FixedClock is a test helper that returns a constant time (and can advance).
type FixedClock struct {
	T time.Time
}

func (c *FixedClock) Now() time.Time { return c.T }

// TimeLayout is the only timestamp format this system emits: UTC, milliseconds,
// trailing Z. Parsing is more permissive (RFC 3339), emission is not.
const TimeLayout = "2006-01-02T15:04:05.000Z"

// FormatTime renders t in the wire format.
func FormatTime(t time.Time) string { return t.UTC().Format(TimeLayout) }
