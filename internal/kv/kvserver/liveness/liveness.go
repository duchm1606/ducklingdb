package liveness

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/duchm1606/ducklingdb/internal/gossip"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
	"github.com/duchm1606/ducklingdb/internal/util/stop"
)

const (
	defaultHeartbeatInterval  = 1 * time.Second
	defaultLivenessThreshold  = 5 * time.Second
)

// Liveness is the record written by each node to prove it is alive.
// It is persisted to the engine and gossiped to all peers.
type Liveness struct {
	NodeID     int32         `json:"node_id"`
	Epoch      int64         `json:"epoch"`
	Expiration hlc.Timestamp `json:"expiration"`
}

// IsLive reports whether this record is still within its expiration window.
func (l Liveness) IsLive(now hlc.Timestamp) bool {
	return now.Less(l.Expiration)
}

// NodeLiveness manages the liveness heartbeat for the local node and
// tracks the last known liveness of remote nodes received via gossip.
type NodeLiveness struct {
	nodeID  int32
	engine  storage.Engine
	clock   *hlc.Clock
	gossip  *gossip.Gossip
	stopper *stop.Stopper

	mu                sync.Mutex
	self              Liveness
	others            map[int32]Liveness
	interval          time.Duration
	livenessThreshold time.Duration
}

func New(
	nodeID int32,
	engine storage.Engine,
	clock *hlc.Clock,
	g *gossip.Gossip,
	stopper *stop.Stopper,
) *NodeLiveness {
	nl := &NodeLiveness{
		nodeID:            nodeID,
		engine:            engine,
		clock:             clock,
		gossip:            g,
		stopper:           stopper,
		others:            make(map[int32]Liveness),
		interval:          defaultHeartbeatInterval,
		livenessThreshold: defaultLivenessThreshold,
	}

	// Register gossip callback so remote liveness records are cached locally.
	g.RegisterCallback(gossip.KeyNodeLivenessPrefix, nl.onLivenessGossip)

	return nl
}

// SetThreshold overrides the liveness expiration window. Must be called before Start.
func (nl *NodeLiveness) SetThreshold(d time.Duration) {
	nl.livenessThreshold = d
}

// Start begins the heartbeat loop. The first heartbeat fires immediately.
func (nl *NodeLiveness) Start() {
	nl.stopper.RunWorker(func() {
		// First heartbeat without waiting for the ticker.
		_ = nl.Heartbeat()

		ticker := time.NewTicker(nl.interval)
		defer ticker.Stop()
		for {
			select {
			case <-nl.stopper.ShouldStop():
				return
			case <-ticker.C:
				_ = nl.Heartbeat()
			}
		}
	})
}

// Heartbeat writes a fresh liveness record to the engine and gossips it.
// The epoch is loaded from the persisted record on the first call (so a
// restarted node continues from its last epoch + 1).
func (nl *NodeLiveness) Heartbeat() error {
	nl.mu.Lock()
	defer nl.mu.Unlock()

	// On the very first heartbeat, read the persisted epoch so that a restart
	// bumps the epoch rather than resetting it to 0.
	if nl.self.Epoch == 0 {
		if existing, err := nl.readFromEngine(nl.nodeID); err == nil {
			nl.self.Epoch = existing.Epoch + 1
		} else {
			nl.self.Epoch = 1
		}
		nl.self.NodeID = nl.nodeID
	}

	now := nl.clock.Now()
	nl.self.Expiration = hlc.Timestamp{
		WallTime: now.WallTime + int64(nl.livenessThreshold),
		Logical:  now.Logical,
	}

	if err := nl.writeToEngine(nl.self); err != nil {
		return fmt.Errorf("liveness heartbeat: %w", err)
	}

	data, err := json.Marshal(nl.self)
	if err != nil {
		return err
	}
	nl.gossip.AddInfo(gossip.MakeNodeLivenessKey(nl.nodeID), data, nl.livenessThreshold+nl.interval)

	return nil
}

// IsLive returns true if nodeID has a valid, unexpired liveness record.
func (nl *NodeLiveness) IsLive(nodeID int32) bool {
	l, err := nl.GetLiveness(nodeID)
	if err != nil {
		return false
	}
	return l.IsLive(nl.clock.Now())
}

// GetLiveness returns the most recent liveness record known for nodeID.
// For the local node it reads from the engine; for remote nodes it uses
// the gossip cache populated by onLivenessGossip.
func (nl *NodeLiveness) GetLiveness(nodeID int32) (Liveness, error) {
	if nodeID == nl.nodeID {
		return nl.readFromEngine(nodeID)
	}

	nl.mu.Lock()
	l, ok := nl.others[nodeID]
	nl.mu.Unlock()

	if !ok {
		return Liveness{}, fmt.Errorf("liveness: no record for node %d", nodeID)
	}
	return l, nil
}

// onLivenessGossip is called by the gossip subsystem whenever a
// node-liveness entry arrives or is updated.
func (nl *NodeLiveness) onLivenessGossip(_ string, val []byte) {
	var l Liveness
	if err := json.Unmarshal(val, &l); err != nil {
		return
	}
	nl.mu.Lock()
	existing, ok := nl.others[l.NodeID]
	if !ok || l.Epoch > existing.Epoch ||
		(l.Epoch == existing.Epoch && existing.Expiration.Less(l.Expiration)) {
		nl.others[l.NodeID] = l
	}
	nl.mu.Unlock()
}

// livenessKey returns the raw engine key for nodeID's liveness record.
func livenessKey(nodeID int32) []byte {
	return []byte(fmt.Sprintf("\x00liveness-%d", nodeID))
}

func (nl *NodeLiveness) writeToEngine(l Liveness) error {
	data, err := json.Marshal(l)
	if err != nil {
		return err
	}
	return nl.engine.Put(livenessKey(l.NodeID), data)
}

func (nl *NodeLiveness) readFromEngine(nodeID int32) (Liveness, error) {
	data, err := nl.engine.Get(livenessKey(nodeID))
	if err != nil {
		return Liveness{}, err
	}
	var l Liveness
	if err := json.Unmarshal(data, &l); err != nil {
		return Liveness{}, err
	}
	return l, nil
}
