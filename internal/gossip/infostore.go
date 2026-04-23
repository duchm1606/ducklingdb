package gossip

import (
	"sync"
	"sync/atomic"
	"time"
)

// Info is a single piece of gossip data.
type Info struct {
	Key       string
	Value     []byte
	OrigStamp int64         // monotonic sequence number from the originating node
	NodeID    int32         // which node created this info
	TTL       time.Duration // 0 means no expiry
	CreatedAt time.Time
}

// expired reports whether this info has passed its TTL.
func (i *Info) expired(now time.Time) bool {
	return i.TTL > 0 && now.After(i.CreatedAt.Add(i.TTL))
}

// InfoStore holds all gossip data known by this node plus high-water stamps
// that track the highest sequence number seen per originating node.
type InfoStore struct {
	mu        sync.RWMutex
	infos     map[string]*Info
	highWater map[int32]int64 // nodeID → highest OrigStamp seen from that node
	seqGen    atomic.Int64
}

func NewInfoStore() *InfoStore {
	return &InfoStore{
		infos:     make(map[string]*Info),
		highWater: make(map[int32]int64),
	}
}

// AddInfo stores a new info entry originated by nodeID with the next local
// sequence number. It always overwrites the existing entry for the key.
func (is *InfoStore) AddInfo(key string, val []byte, ttl time.Duration, nodeID int32) *Info {
	stamp := is.seqGen.Add(1)
	info := &Info{
		Key:       key,
		Value:     val,
		OrigStamp: stamp,
		NodeID:    nodeID,
		TTL:       ttl,
		CreatedAt: time.Now(),
	}

	is.mu.Lock()
	defer is.mu.Unlock()

	is.infos[key] = info
	if stamp > is.highWater[nodeID] {
		is.highWater[nodeID] = stamp
	}
	return info
}

// GetInfo returns the info for key and whether it was found (and not expired).
func (is *InfoStore) GetInfo(key string) (*Info, bool) {
	is.mu.RLock()
	defer is.mu.RUnlock()

	info, ok := is.infos[key]
	if !ok || info.expired(time.Now()) {
		return nil, false
	}
	return info, true
}

// Combine merges an info received from a peer. It is accepted if:
//   - no entry exists for the key, or
//   - the incoming entry has a higher OrigStamp than the stored one.
//
// Returns true if the info was accepted (i.e. it was new or fresher).
func (is *InfoStore) Combine(info *Info) bool {
	is.mu.Lock()
	defer is.mu.Unlock()

	existing, ok := is.infos[info.Key]
	if ok && existing.OrigStamp >= info.OrigStamp {
		return false
	}

	is.infos[info.Key] = info
	if info.OrigStamp > is.highWater[info.NodeID] {
		is.highWater[info.NodeID] = info.OrigStamp
	}
	return true
}

// Delta returns all infos that are fresher than what peerHighWater describes.
// An info is fresh for the peer if its OrigStamp exceeds the peer's recorded
// high-water for its originating node.
func (is *InfoStore) Delta(peerHighWater map[int32]int64) map[string]*Info {
	is.mu.RLock()
	defer is.mu.RUnlock()

	now := time.Now()
	delta := make(map[string]*Info)
	for key, info := range is.infos {
		if info.expired(now) {
			continue
		}
		if info.OrigStamp > peerHighWater[info.NodeID] {
			delta[key] = info
		}
	}
	return delta
}

// HighWater returns a snapshot of the current high-water map.
func (is *InfoStore) HighWater() map[int32]int64 {
	is.mu.RLock()
	defer is.mu.RUnlock()

	hw := make(map[int32]int64, len(is.highWater))
	for k, v := range is.highWater {
		hw[k] = v
	}
	return hw
}

// AllInfos returns a snapshot of all non-expired infos.
func (is *InfoStore) AllInfos() map[string]*Info {
	is.mu.RLock()
	defer is.mu.RUnlock()

	now := time.Now()
	out := make(map[string]*Info, len(is.infos))
	for k, v := range is.infos {
		if !v.expired(now) {
			out[k] = v
		}
	}
	return out
}
