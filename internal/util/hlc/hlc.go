package hlc

import (
	"sync"
	"time"
)

// WallClock abstracts the physical clock so tests can inject a deterministic
// source without real time dependencies.
type WallClock interface {
	Now() int64 // nanoseconds since the Unix epoch
}

// systemClock wraps the real wall clock.
type systemClock struct{}

func (systemClock) Now() int64 { return time.Now().UnixNano() }

// SystemWallClock returns a WallClock that delegates to time.Now.
func SystemWallClock() WallClock { return systemClock{} }

// Clock implements the Hybrid Logical Clock algorithm (Kulkarni et al., 2014).
//
// It guarantees that:
//   - Now() is strictly monotonic across calls on the same Clock
//   - Timestamps generated on a Clock C1 causally precede timestamps generated
//     on a Clock C2 after C2.Update(t) has been called with t from C1
//
// These properties hold even if the underlying wall clock moves backward
// (during NTP correction) or if two nodes' wall clocks differ by up to
// maxOffset.
type Clock struct {
	wall      WallClock
	maxOffset time.Duration

	mu     sync.Mutex
	state  Timestamp
}

// NewClock constructs a Clock backed by wall. maxOffset is the maximum clock
// skew tolerated between nodes; it is stored for callers that need to compute
// uncertainty windows but is not enforced by Clock itself.
func NewClock(wall WallClock, maxOffset time.Duration) *Clock {
	return &Clock{wall: wall, maxOffset: maxOffset}
}

// MaxOffset returns the configured maximum clock skew.
func (c *Clock) MaxOffset() time.Duration { return c.maxOffset }

// Now returns the next timestamp in the clock's sequence.
// Rule: WallTime = max(lastState.WallTime, physical wall clock).
// If the wall advanced, Logical resets to 0; otherwise Logical increments.
func (c *Clock) Now() Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	physical := c.wall.Now()
	if physical > c.state.WallTime {
		c.state = Timestamp{WallTime: physical, Logical: 0}
	} else {
		// Wall clock did not advance (same nanosecond or went backward).
		// Hold the wall time and increment logical to preserve monotonicity.
		c.state.Logical++
	}
	return c.state
}

// Update advances the clock based on a remote timestamp received from another
// node. Rule: state = max(local, remote, physical). This is the mechanism by
// which HLC tracks causality across nodes.
func (c *Clock) Update(remote Timestamp) Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	physical := c.wall.Now()

	// Pick the latest of three candidates.
	var next Timestamp
	switch {
	case physical >= c.state.WallTime && physical >= remote.WallTime:
		next = Timestamp{WallTime: physical, Logical: 0}
	case c.state.WallTime >= remote.WallTime:
		next = Timestamp{WallTime: c.state.WallTime, Logical: c.state.Logical + 1}
	default:
		next = Timestamp{WallTime: remote.WallTime, Logical: remote.Logical + 1}
	}

	// Guard against degenerate cases where logical ends up ≤ current.
	if !c.state.Less(next) {
		next = c.state.Next()
	}

	c.state = next
	return c.state
}

// ManualClock is a controllable wall-clock source for tests.
// It is safe for concurrent use.
type ManualClock struct {
	mu    sync.Mutex
	nanos int64
}

// NewManualClock creates a ManualClock initialized to nanos.
func NewManualClock(nanos int64) *ManualClock {
	return &ManualClock{nanos: nanos}
}

// Now returns the current manual time.
func (m *ManualClock) Now() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.nanos
}

// Set replaces the current manual time.
func (m *ManualClock) Set(nanos int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nanos = nanos
}

// Increment advances the manual time by delta nanoseconds.
func (m *ManualClock) Increment(delta int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.nanos += delta
}
