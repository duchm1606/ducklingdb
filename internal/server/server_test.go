package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
)

func TestStopperRunAndStop(t *testing.T) {
	s := NewStopper()

	var count atomic.Int32
	for range 3 {
		s.RunWorker(func() {
			<-s.ShouldStop()
			count.Add(1)
		})
	}

	time.Sleep(10 * time.Millisecond)
	s.Stop()

	if got := count.Load(); got != 3 {
		t.Fatalf("expected 3 workers to finish, got %d", got)
	}
}

func TestStopperRunAfterStop(t *testing.T) {
	s := NewStopper()
	s.Stop()

	var ran atomic.Bool
	s.RunWorker(func() {
		ran.Store(true)
	})

	time.Sleep(10 * time.Millisecond)
	if ran.Load() {
		t.Fatal("worker should not run after stopper is stopped")
	}
}

func TestNodeStartStop(t *testing.T) {
	dir := t.TempDir()
	n, err := NewNode(NodeConfig{
		Addr:    ":0",
		DataDir: dir,
		NodeID:  1,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	n.Start()
	defer n.Stop()

	if n.NodeID() != 1 {
		t.Fatalf("NodeID: got %d, want 1", n.NodeID())
	}

	desc := n.Descriptor()
	if desc.NodeId != 1 {
		t.Fatalf("descriptor NodeId: got %d, want 1", desc.NodeId)
	}
	if desc.Address == "" {
		t.Fatal("descriptor address is empty")
	}
	if n.RPCAddr() == "" {
		t.Fatal("RPCAddr is empty")
	}
}

func TestNodeHeartbeatViaRPC(t *testing.T) {
	dir := t.TempDir()
	n, err := NewNode(NodeConfig{
		Addr:    ":0",
		DataDir: dir,
		NodeID:  1,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	n.Start()
	defer n.Stop()

	conn, err := n.RPCContext().GRPCDialNode(n.RPCAddr())
	if err != nil {
		t.Fatalf("GRPCDialNode: %v", err)
	}

	client := pb.NewInternalClient(conn)
	resp, err := client.Heartbeat(context.Background(), &pb.PingRequest{
		NodeId: 2,
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	if resp.NodeId != 1 {
		t.Fatalf("node_id: got %d, want 1", resp.NodeId)
	}
	if resp.ServerTime == nil {
		t.Fatal("server_time is nil")
	}
}

func TestNodeEngineReadWrite(t *testing.T) {
	dir := t.TempDir()
	n, err := NewNode(NodeConfig{
		Addr:    ":0",
		DataDir: dir,
		NodeID:  1,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	n.Start()
	defer n.Stop()

	if err := n.Engine().Put([]byte("key"), []byte("value")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	val, err := n.Engine().Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "value" {
		t.Fatalf("Get: got %q, want %q", val, "value")
	}
}

// waitForStableApplied waits until the replica's applied index has stopped
// moving, then returns it.
//
// Sampling right after WaitForLeader is racy in the *passing* direction:
// WaitForLeader returns as soon as Lead() != 0, but the no-op entry that
// becomeLeader appends is applied later in that same Ready cycle. A sample
// taken inside that window reads 0, and the no-op alone then advances the
// index — so an `applied > appliedBefore` assertion would pass even if the
// write under test never reached Raft.
func waitForStableApplied(t *testing.T, n *Node, timeout time.Duration) uint64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last uint64
	stable := 0
	for time.Now().Before(deadline) {
		r := n.getReplica()
		if r == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		cur := r.AppliedIndex()
		if cur != 0 && cur == last {
			if stable++; stable >= 2 {
				return cur
			}
		} else {
			stable = 0
		}
		last = cur
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("applied index did not stabilise within %s (last=%d)", timeout, last)
	return 0
}

func TestBatchRPCPutAndGet(t *testing.T) {
	n, err := NewNode(NodeConfig{
		Addr:    ":0",
		DataDir: t.TempDir(),
		NodeID:  1,
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	defer n.Stop()

	// Writes now require a committed Raft entry, so the group must have a
	// leader before the client writes.
	if !n.WaitForLeader(5 * time.Second) {
		t.Fatal("raft group has no leader; writes cannot be committed")
	}

	conn, err := n.RPCContext().GRPCDialNode(n.RPCAddr())
	if err != nil {
		t.Fatalf("GRPCDialNode: %v", err)
	}
	client := pb.NewInternalClient(conn)

	appliedBefore := waitForStableApplied(t, n, 5*time.Second)

	putResp, err := client.Batch(context.Background(), &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: 100}},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("rpc-key"),
				Value: &pb.Value{RawBytes: []byte("rpc-val")},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Put via RPC: %v", err)
	}
	if putResp.Error != nil {
		t.Fatalf("Put error: %s", putResp.Error.Message)
	}

	// The write must have been committed through Raft, not applied directly to
	// local MVCC. Before the write path was collapsed, this test passed with
	// nodeServer.Batch never touching Raft at all.
	// Exactly one entry: the write under test. `>` would also be satisfied by
	// the leader's bootstrap no-op landing late.
	if applied := n.getReplica().AppliedIndex(); applied != appliedBefore+1 {
		t.Fatalf("write did not go through the Raft log as exactly one entry: applied %d, want %d",
			applied, appliedBefore+1)
	}

	getResp, err := client.Batch(context.Background(), &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: 100, Logical: 1}},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Get{Get: &pb.GetRequest{
				Key: []byte("rpc-key"),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Get via RPC: %v", err)
	}
	if getResp.Error != nil {
		t.Fatalf("Get error: %s", getResp.Error.Message)
	}

	val := getResp.Responses[0].GetGet().GetValue()
	if val == nil || string(val.RawBytes) != "rpc-val" {
		t.Fatalf("got %q, want %q", val.GetRawBytes(), "rpc-val")
	}
}

func TestCrossNodeBatchRPC(t *testing.T) {
	nodeA, err := NewNode(NodeConfig{
		Addr:    ":0",
		DataDir: t.TempDir(),
		NodeID:  1,
	})
	if err != nil {
		t.Fatalf("NewNode A: %v", err)
	}
	nodeA.Start()
	defer nodeA.Stop()

	nodeB, err := NewNode(NodeConfig{
		Addr:    ":0",
		DataDir: t.TempDir(),
		NodeID:  2,
	})
	if err != nil {
		t.Fatalf("NewNode B: %v", err)
	}
	nodeB.Start()
	defer nodeB.Stop()

	if !nodeA.WaitForLeader(5 * time.Second) {
		t.Fatal("nodeA raft group has no leader; writes cannot be committed")
	}

	conn, err := nodeB.RPCContext().GRPCDialNode(nodeA.RPCAddr())
	if err != nil {
		t.Fatalf("B dial A: %v", err)
	}
	client := pb.NewInternalClient(conn)

	appliedBefore := waitForStableApplied(t, nodeA, 5*time.Second)

	_, err = client.Batch(context.Background(), &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: 100}},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("cross-key"),
				Value: &pb.Value{RawBytes: []byte("cross-val")},
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Put A from B: %v", err)
	}

	// The write was committed through nodeA's Raft log, not written directly to
	// its engine by the RPC handler.
	// Exactly one entry: the write under test. `>` would also be satisfied by
	// the leader's bootstrap no-op landing late.
	if applied := nodeA.getReplica().AppliedIndex(); applied != appliedBefore+1 {
		t.Fatalf("write did not go through the Raft log as exactly one entry: applied %d, want %d",
			applied, appliedBefore+1)
	}

	getResp, err := client.Batch(context.Background(), &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: 100, Logical: 1}},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Get{Get: &pb.GetRequest{
				Key: []byte("cross-key"),
			}}},
		},
	})
	if err != nil {
		t.Fatalf("Get A from B: %v", err)
	}

	val := getResp.Responses[0].GetGet().GetValue()
	if val == nil || string(val.RawBytes) != "cross-val" {
		t.Fatalf("got %q, want %q", val.GetRawBytes(), "cross-val")
	}
}
