package hlc

import "sync"

// ManualClock is a controllable wall-clock source for deterministic tests.
// It implements WallClock and is safe for concurrent use.
//
// Use Set and Increment to drive time forward, backward, or to simulate
// clock skew between nodes.
type ManualClock struct {
	mu    sync.Mutex
	nanos int64
}

// NewManualClock creates a ManualClock initialized to nanos.
func NewManualClock(nanos int64) *ManualClock {
	return &ManualClock{nanos: nanos}
}

// Now returns the current manual time in nanoseconds.
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
