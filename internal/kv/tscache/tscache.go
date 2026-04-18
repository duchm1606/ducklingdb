// Package tscache implements a bounded in-memory cache of the most recent
// read timestamps per key. Writers consult the cache to decide whether their
// write timestamp must be pushed forward to preserve the snapshots of
// concurrent readers.
//
// The cache is a safety net against a subtle anomaly: a writer that commits
// at a timestamp older than a previously-completed read would retroactively
// alter that read's snapshot. Recording reads in the cache lets writers push
// forward past them, ensuring every committed snapshot stays valid.
package tscache

import (
	"sync"

	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// Cache stores the latest timestamp at which each key was read.
//
// When the cache is full, the oldest entry (lowest timestamp) is evicted and
// the low-water mark is advanced to its timestamp. Any key absent from the
// cache is assumed to have been read at the low-water mark — a conservative
// floor that may over-push but never under-pushes.
//
// Cache is safe for concurrent use.
type Cache struct {
	mu       sync.Mutex
	reads    map[string]hlc.Timestamp
	lowWater hlc.Timestamp
	maxSize  int
}

// New returns a Cache that holds up to maxSize entries before eviction. A
// maxSize of 0 disables eviction-by-capacity (entries only age out via manual
// SetLowWater calls). Callers should pick a bound appropriate to their
// memory budget; CockroachDB's default is in the thousands of entries per
// store.
func New(maxSize int) *Cache {
	return &Cache{
		reads:   make(map[string]hlc.Timestamp),
		maxSize: maxSize,
	}
}

// Add records that key was read at timestamp. If a newer read is already
// recorded for key, the existing entry is kept — the cache always holds the
// highest timestamp.
//
// If recording this entry would exceed maxSize, the entry with the lowest
// timestamp is evicted and the low-water mark is advanced to that timestamp.
func (c *Cache) Add(key []byte, timestamp hlc.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// A read below the low-water mark is already covered by the floor; no
	// need to insert it.
	if timestamp.LessEq(c.lowWater) {
		return
	}

	k := string(key)
	if existing, ok := c.reads[k]; ok {
		if existing.Less(timestamp) {
			c.reads[k] = timestamp
		}
		return
	}

	if c.maxSize > 0 && len(c.reads) >= c.maxSize {
		c.evictOldestLocked()
	}
	c.reads[k] = timestamp
}

// GetMax returns the highest read timestamp that covers key. If key has no
// explicit entry, the low-water mark is returned — the conservative floor.
//
// A writer at timestamp writeTS must push to at least GetMax(key).Next() to
// avoid invalidating any past read.
func (c *Cache) GetMax(key []byte) hlc.Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ts, ok := c.reads[string(key)]; ok {
		return ts
	}
	return c.lowWater
}

// LowWater returns the current low-water mark. Keys without explicit entries
// are assumed to have been read at this timestamp.
func (c *Cache) LowWater() hlc.Timestamp {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lowWater
}

// SetLowWater advances the low-water mark. Entries at or below the new mark
// are pruned — they are already implicitly covered by the floor.
//
// This is primarily useful when rebuilding the cache after a restart (the
// caller may know a safe floor from durable state) or when manually trimming
// memory without waiting for Add to hit the size bound.
func (c *Cache) SetLowWater(timestamp hlc.Timestamp) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if timestamp.LessEq(c.lowWater) {
		return
	}
	c.lowWater = timestamp
	for k, ts := range c.reads {
		if ts.LessEq(timestamp) {
			delete(c.reads, k)
		}
	}
}

// Len returns the number of explicit entries in the cache (excludes keys
// covered only by the low-water mark).
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.reads)
}

// evictOldestLocked removes the entry with the lowest timestamp and advances
// the low-water mark to that timestamp. Caller must hold c.mu.
//
// We scan the map on each eviction; at small capacities this is fine. A
// production cache would use a priority queue or LRU list to make this O(1),
// but the extra machinery is not pedagogically illuminating.
func (c *Cache) evictOldestLocked() {
	var (
		oldestKey string
		oldestTS  hlc.Timestamp
		first     = true
	)
	for k, ts := range c.reads {
		if first || ts.Less(oldestTS) {
			oldestKey = k
			oldestTS = ts
			first = false
		}
	}
	if !first {
		delete(c.reads, oldestKey)
		if c.lowWater.Less(oldestTS) {
			c.lowWater = oldestTS
		}
	}
}
