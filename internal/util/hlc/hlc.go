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

	// Pick the WallTime from the maximum of three candidates, then set
	// Logical to advance past both local state and remote.
	var next Timestamp
	switch {
	case physical > c.state.WallTime && physical > remote.WallTime:
		// Physical wall strictly ahead of everything — reset logical.
		next = Timestamp{WallTime: physical, Logical: 0}
	case c.state.WallTime > remote.WallTime && c.state.WallTime > physical:
		// Local state is strictly ahead.
		next = Timestamp{WallTime: c.state.WallTime, Logical: c.state.Logical + 1}
	case remote.WallTime > c.state.WallTime && remote.WallTime > physical:
		// Remote is strictly ahead.
		next = Timestamp{WallTime: remote.WallTime, Logical: remote.Logical + 1}
	default:
		// Two or more candidates share the maximum WallTime.
		// Pick that WallTime and set Logical past the highest among them.
		maxWall := physical
		if c.state.WallTime > maxWall {
			maxWall = c.state.WallTime
		}
		if remote.WallTime > maxWall {
			maxWall = remote.WallTime
		}
		maxLogical := int32(0)
		if c.state.WallTime == maxWall && c.state.Logical > maxLogical {
			maxLogical = c.state.Logical
		}
		if remote.WallTime == maxWall && remote.Logical > maxLogical {
			maxLogical = remote.Logical
		}
		next = Timestamp{WallTime: maxWall, Logical: maxLogical + 1}
	}

	// Safety net: next must always exceed local state.
	if !c.state.Less(next) {
		next = c.state.Next()
	}

	c.state = next
	return c.state
}
