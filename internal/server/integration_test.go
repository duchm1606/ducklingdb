package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
)

// startNode is a helper that creates and starts a node, registering cleanup.
func startNode(t *testing.T, cfg NodeConfig) *Node {
	t.Helper()
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	t.Cleanup(n.Stop)
	return n
}

// TestThreeNodeCluster boots a three-node cluster and verifies:
//   - all nodes share the same ClusterID
//   - each node has a distinct NodeID (1, 2, 3)
//   - all nodes report themselves as live
func TestThreeNodeCluster(t *testing.T) {
	n1 := startNode(t, NodeConfig{Addr: ":0", DataDir: t.TempDir()})
	n2 := startNode(t, NodeConfig{Addr: ":0", DataDir: t.TempDir(), JoinAddrs: []string{n1.RPCAddr()}})
	n3 := startNode(t, NodeConfig{Addr: ":0", DataDir: t.TempDir(), JoinAddrs: []string{n1.RPCAddr()}})

	// All in same cluster.
	if n2.ClusterID() != n1.ClusterID() || n3.ClusterID() != n1.ClusterID() {
		t.Fatal("nodes must share ClusterID")
	}

	// IDs are 1, 2, 3.
	ids := map[NodeID]bool{n1.NodeID(): true, n2.NodeID(): true, n3.NodeID(): true}
	for _, want := range []NodeID{1, 2, 3} {
		if !ids[want] {
			t.Fatalf("expected NodeID %d in cluster, got ids: %v", want, ids)
		}
	}

	// Each node sees itself as live after liveness starts.
	time.Sleep(50 * time.Millisecond) // allow first heartbeat
	for _, n := range []*Node{n1, n2, n3} {
		if !n.Liveness().IsLive(int32(n.NodeID())) {
			t.Errorf("node %d should be live", n.NodeID())
		}
	}
}

// TestBatchRequestAcrossNodes verifies that a BatchRequest sent to one node
// stores a value that can then be retrieved from the same node.
//
// It also asserts the write was committed *through Raft* rather than written
// directly to local MVCC. That assertion is the point: this test previously
// passed because nodeServer.Batch bypassed Raft entirely, so it was evidence
// against replication rather than for it.
//
// Note these nodes share a cluster via JoinAddrs but do not share a Raft group
// (no Peers), so each is a one-peer group. Asserting convergence across a real
// multi-node group needs the Peers harness and belongs with the M4 verification
// suite.
func TestBatchRequestAcrossNodes(t *testing.T) {
	n1 := startNode(t, NodeConfig{Addr: ":0", DataDir: t.TempDir()})
	n2 := startNode(t, NodeConfig{Addr: ":0", DataDir: t.TempDir(), JoinAddrs: []string{n1.RPCAddr()}})
	_ = n2

	if !n1.WaitForLeader(5 * time.Second) {
		t.Fatal("n1 raft group has no leader; writes cannot be committed")
	}

	conn, err := n1.RPCContext().GRPCDialNode(n1.RPCAddr())
	if err != nil {
		t.Fatalf("dial n1: %v", err)
	}
	client := pb.NewInternalClient(conn)

	appliedBefore := waitForStableApplied(t, n1, 5*time.Second)

	// Write via n1.
	putReq := &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: 1}},
		Requests: []*pb.RequestUnion{{
			Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("integration-key"),
				Value: &pb.Value{RawBytes: []byte("integration-value")},
			}},
		}},
	}
	ctx := context.Background()
	resp, err := client.Batch(ctx, putReq)
	if err != nil {
		t.Fatalf("put RPC: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("put error: %s", resp.Error.Message)
	}

	// Exactly one entry: the write under test. `>` would also be satisfied by
	// the leader's bootstrap no-op landing late.
	if applied := n1.getReplica().AppliedIndex(); applied != appliedBefore+1 {
		t.Fatalf("write did not go through the Raft log as exactly one entry: applied %d, want %d",
			applied, appliedBefore+1)
	}

	// Read it back.
	getReq := &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: 1, Logical: 1}},
		Requests: []*pb.RequestUnion{{
			Value: &pb.RequestUnion_Get{Get: &pb.GetRequest{Key: []byte("integration-key")}},
		}},
	}
	resp, err = client.Batch(ctx, getReq)
	if err != nil {
		t.Fatalf("get RPC: %v", err)
	}
	if resp.Error != nil {
		t.Fatalf("get error: %s", resp.Error.Message)
	}
	got := resp.Responses[0].GetGet().GetValue()
	if got == nil || string(got.RawBytes) != "integration-value" {
		t.Fatalf("expected %q, got %q", "integration-value", got.GetRawBytes())
	}
}

// TestNodeLivenessDetection verifies that after a node stops heartbeating,
// IsLive eventually returns false once the expiration window passes.
func TestNodeLivenessDetection(t *testing.T) {
	const threshold = 200 * time.Millisecond

	n1 := startNode(t, NodeConfig{
		Addr:              ":0",
		DataDir:           t.TempDir(),
		LivenessThreshold: threshold,
		HeartbeatInterval: 50 * time.Millisecond,
	})

	// Wait for at least one heartbeat so liveness is written.
	time.Sleep(100 * time.Millisecond)

	if !n1.Liveness().IsLive(int32(n1.NodeID())) {
		t.Fatal("node should be live after heartbeat")
	}

	// Stop the node's stopper (stops heartbeating) without closing the engine.
	n1.stopper.Stop()

	// After the threshold passes the record should be expired.
	time.Sleep(threshold + 50*time.Millisecond)

	if n1.Liveness().IsLive(int32(n1.NodeID())) {
		t.Fatal("node should no longer be live after expiration")
	}
}

// TestNodeRejoin verifies that a node which restarts against the same data
// directory gets its original NodeID back and its epoch is bumped.
func TestNodeRejoin(t *testing.T) {
	dir := t.TempDir()

	// First boot.
	n1 := startNode(t, NodeConfig{Addr: ":0", DataDir: dir})
	clusterID := n1.ClusterID()
	time.Sleep(50 * time.Millisecond) // one heartbeat
	n1.Stop()

	// Rejoin with same dir.
	n2, err := NewNode(NodeConfig{Addr: ":0", DataDir: dir})
	if err != nil {
		t.Fatalf("rejoin NewNode: %v", err)
	}
	n2.Start()
	defer n2.Stop()

	if n2.NodeID() != 1 {
		t.Fatalf("rejoin NodeID: got %d, want 1", n2.NodeID())
	}
	if n2.ClusterID() != clusterID {
		t.Fatal("rejoin ClusterID mismatch")
	}

	// Wait for first heartbeat; epoch should be > 1 after the previous run.
	time.Sleep(100 * time.Millisecond)
	l, err := n2.Liveness().GetLiveness(int32(n2.NodeID()))
	if err != nil {
		t.Fatalf("GetLiveness: %v", err)
	}
	if l.Epoch < 2 {
		t.Fatalf("epoch after rejoin: got %d, want >= 2", l.Epoch)
	}
}

// TestClockOffsetTracking verifies that after a heartbeat round the
// RemoteClockMonitor has a measurement for the peer.
func TestClockOffsetTracking(t *testing.T) {
	n1 := startNode(t, NodeConfig{Addr: ":0", DataDir: t.TempDir()})
	n2 := startNode(t, NodeConfig{
		Addr:              ":0",
		DataDir:           t.TempDir(),
		JoinAddrs:         []string{n1.RPCAddr()},
		HeartbeatInterval: 50 * time.Millisecond,
	})

	// Add n1 as peer of n2 so measurePeerOffsets can reach it.
	n2.Gossip().AddPeer(n1.RPCAddr())

	// Wait for at least one heartbeat worker tick.
	time.Sleep(200 * time.Millisecond)

	_, ok := n2.ClockMonitor().GetOffset(int32(n1.NodeID()))
	if !ok {
		t.Fatal("n2 should have a clock offset measurement for n1 after heartbeat")
	}
}
