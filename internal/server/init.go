package server

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/rpc"
	"github.com/duchm1606/ducklingdb/internal/storage"
)

// ClusterID is a 16-byte UUID that uniquely identifies a DucklingDB cluster.
// It is generated once during bootstrap and never changes.
type ClusterID [16]byte

// InitState is returned by InitNode and describes how the node was initialized.
type InitState struct {
	ClusterID    ClusterID
	NodeID       NodeID
	Bootstrapped bool // true only for the very first Bootstrap call
}

// Well-known engine keys for persisted cluster/node identity.
var (
	keyClusterID  = []byte("\x00cluster-id")
	keyNodeID     = []byte("\x00node-id")
	keyNextNodeID = []byte("\x00next-node-id")
)

// Bootstrap initializes a brand-new single-node cluster.
// It generates a random ClusterID, assigns NodeID=1, writes both to the
// engine, and seeds the NodeIDAllocator at 2 for future joiners.
func Bootstrap(engine storage.Engine) (*InitState, error) {
	var clusterID ClusterID
	if _, err := rand.Read(clusterID[:]); err != nil {
		return nil, fmt.Errorf("generate cluster id: %w", err)
	}

	if err := engine.Put(keyClusterID, clusterID[:]); err != nil {
		return nil, fmt.Errorf("write cluster id: %w", err)
	}

	nodeID := NodeID(1)
	if err := writeNodeID(engine, nodeID); err != nil {
		return nil, err
	}

	// Initialize allocator: next ID to hand out is 2.
	if err := writeNextNodeID(engine, 2); err != nil {
		return nil, err
	}

	return &InitState{ClusterID: clusterID, NodeID: nodeID, Bootstrapped: true}, nil
}

// joinTimeout is the per-seed RPC deadline during a join attempt.
const joinTimeout = 5 * time.Second

// Join contacts a seed node to receive a ClusterID and an allocated NodeID,
// then persists both to the engine.
func Join(ctx context.Context, engine storage.Engine, rpcCtx *rpc.Context, seedAddrs []string) (*InitState, error) {
	if len(seedAddrs) == 0 {
		return nil, fmt.Errorf("join: no seed addresses provided")
	}

	var lastErr error
	for _, seed := range seedAddrs {
		conn, err := rpcCtx.GRPCDialNode(seed)
		if err != nil {
			lastErr = err
			continue
		}

		dialCtx, cancel := context.WithTimeout(ctx, joinTimeout)
		client := pb.NewInternalClient(conn)
		resp, err := client.AllocateNodeID(dialCtx, &pb.AllocateNodeIDRequest{})
		cancel()
		if err != nil {
			lastErr = err
			continue
		}

		var clusterID ClusterID
		copy(clusterID[:], resp.ClusterId)
		nodeID := NodeID(resp.NodeId)

		if err := engine.Put(keyClusterID, clusterID[:]); err != nil {
			return nil, fmt.Errorf("write cluster id: %w", err)
		}
		if err := writeNodeID(engine, nodeID); err != nil {
			return nil, err
		}

		return &InitState{ClusterID: clusterID, NodeID: nodeID, Bootstrapped: false}, nil
	}

	return nil, fmt.Errorf("join: could not reach any seed: %w", lastErr)
}

// InitNode decides whether to bootstrap, join, or restart:
//   - Persisted node ID found in engine → restart (return stored state)
//   - No persisted state, no seed addresses → Bootstrap
//   - No persisted state, seed addresses provided → Join
func InitNode(engine storage.Engine, rpcCtx *rpc.Context, seedAddrs []string) (*InitState, error) {
	nodeIDBytes, err := engine.Get(keyNodeID)
	if err == nil {
		// Persisted state found — this is a restart.
		clusterIDBytes, err := engine.Get(keyClusterID)
		if err != nil {
			return nil, fmt.Errorf("found node id but missing cluster id: %w", err)
		}
		var clusterID ClusterID
		copy(clusterID[:], clusterIDBytes)
		nodeID := NodeID(binary.BigEndian.Uint32(nodeIDBytes))
		return &InitState{ClusterID: clusterID, NodeID: nodeID, Bootstrapped: false}, nil
	}

	if len(seedAddrs) == 0 {
		return Bootstrap(engine)
	}
	return Join(context.Background(), engine, rpcCtx, seedAddrs)
}

// NodeIDAllocator dispenses unique NodeIDs from a counter persisted in the engine.
// It is used by the bootstrap node to hand out IDs to joiners via AllocateNodeID RPC.
// Not safe for concurrent use — in practice nodes join one at a time.
type NodeIDAllocator struct {
	engine    storage.Engine
	clusterID ClusterID
}

func newNodeIDAllocator(engine storage.Engine, clusterID ClusterID) *NodeIDAllocator {
	return &NodeIDAllocator{engine: engine, clusterID: clusterID}
}

// Allocate reads the next available NodeID, increments the counter, and returns the ID.
func (a *NodeIDAllocator) Allocate() (NodeID, error) {
	data, err := a.engine.Get(keyNextNodeID)
	if err != nil {
		return 0, fmt.Errorf("allocate node id: %w", err)
	}

	next := NodeID(binary.BigEndian.Uint32(data))
	if err := writeNextNodeID(a.engine, uint32(next)+1); err != nil {
		return 0, err
	}
	return next, nil
}

func writeNodeID(engine storage.Engine, id NodeID) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(id))
	return engine.Put(keyNodeID, buf[:])
}

func writeNextNodeID(engine storage.Engine, next uint32) error {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], next)
	return engine.Put(keyNextNodeID, buf[:])
}
