package server

import (
	"context"
	"strings"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
)

// TestNodeStopStopsReplica pins that shutting a node down also stops its Raft
// Ready loop.
//
// Node.Stop() closed the engine but never stopped the Replica, so run() kept
// ticking every 10ms and calling SaveHardState / AppendEntries against a closed
// WAL — and in tests, against a t.TempDir() about to be deleted. That leak used
// to be confined to nodes started with peers; once every node got a Replica it
// became universal.
func TestNodeStopStopsReplica(t *testing.T) {
	n, err := NewNode(NodeConfig{Addr: ":0", DataDir: t.TempDir(), NodeID: 1})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	if !n.WaitForLeader(5 * time.Second) {
		n.Stop()
		t.Fatal("no leader")
	}
	r := n.getReplica()
	if r == nil {
		n.Stop()
		t.Fatal("no replica")
	}

	n.Stop()

	// A stopped Ready loop refuses work rather than servicing it against a
	// closed engine. Before the fix the loop was still running here.
	if _, err := r.CreateSnapshot(); err == nil || !strings.Contains(err.Error(), "replica stopped") {
		t.Fatalf("after Node.Stop the replica loop should be stopped; CreateSnapshot returned %v", err)
	}
}

// TestReplicaRetriesPeerResolution pins that a node whose peers are not yet
// discoverable keeps trying.
//
// The resolver used to log once and return, leaving getReplica() nil for the
// process's lifetime — every subsequent write failed with "replica not ready"
// forever. "Never ready" is a worse failure than "not ready yet".
func TestReplicaRetriesPeerResolution(t *testing.T) {
	n, err := NewNode(NodeConfig{
		Addr:    ":0",
		DataDir: t.TempDir(),
		NodeID:  1,
		// A peer that gossip cannot resolve yet.
		Peers: []string{"127.0.0.1:59999"},
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	// Keep each resolution attempt short so the retry loop turns over quickly.
	n.peerResolveTimeout = 200 * time.Millisecond
	n.Start()
	defer n.Stop()

	// First attempts must fail: the peer is unknown.
	time.Sleep(400 * time.Millisecond)
	if n.getReplica() != nil {
		t.Fatal("replica should not exist while the peer is unresolvable")
	}

	// The peer becomes resolvable, as it would when its gossip descriptor
	// arrives.
	n.addrMu.Lock()
	n.addrToNodeID["127.0.0.1:59999"] = 2
	n.addrMu.Unlock()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n.getReplica() != nil {
			return // retry succeeded
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("replica was never created after the peer became resolvable: " +
		"peer resolution did not retry")
}

// TestMixedBatchRejected pins that a batch containing both reads and writes is
// refused rather than answered with a fabricated response.
//
// Such a batch takes the Raft path, where the response is rebuilt from the
// request shape — so the Get came back as an empty PutResponse and the read
// result was silently dropped.
func TestMixedBatchRejected(t *testing.T) {
	n, err := NewNode(NodeConfig{Addr: ":0", DataDir: t.TempDir(), NodeID: 1})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	defer n.Stop()
	if !n.WaitForLeader(5 * time.Second) {
		t.Fatal("no leader")
	}

	conn, err := n.RPCContext().GRPCDialNode(n.RPCAddr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := pb.NewInternalClient(conn)

	resp, err := client.Batch(context.Background(), &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: 100}},
		Requests: []*pb.RequestUnion{
			{Value: &pb.RequestUnion_Get{Get: &pb.GetRequest{Key: []byte("k")}}},
			{Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key: []byte("k"), Value: &pb.Value{RawBytes: []byte("v")},
			}}},
		},
	})
	if err == nil {
		t.Fatalf("mixed read/write batch should be rejected, got response %+v", resp)
	}
	if !strings.Contains(err.Error(), "mixes reads and writes") {
		t.Fatalf("unexpected error for mixed batch: %v", err)
	}
}

// TestDeterministicRequestErrorStaysInBand pins the client contract for a
// request that fails deterministically while applying.
//
// Routing writes through Raft turned these into gRPC errors with a nil
// response, where the direct-MVCC path had returned a well-formed
// BatchResponse carrying resp.Error. Callers checking resp.Error broke.
func TestDeterministicRequestErrorStaysInBand(t *testing.T) {
	n, err := NewNode(NodeConfig{Addr: ":0", DataDir: t.TempDir(), NodeID: 1})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	defer n.Stop()
	if !n.WaitForLeader(5 * time.Second) {
		t.Fatal("no leader")
	}

	conn, err := n.RPCContext().GRPCDialNode(n.RPCAddr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := pb.NewInternalClient(conn)

	// A RequestUnion with no Value set fails in executeSingle with
	// "unknown request type" — deterministic on every replica.
	resp, err := client.Batch(context.Background(), &pb.BatchRequest{
		Header:   &pb.Header{Timestamp: &pb.Timestamp{WallTime: 100}},
		Requests: []*pb.RequestUnion{{}},
	})
	if err != nil {
		t.Fatalf("deterministic request failure should be in-band, got RPC error: %v", err)
	}
	if resp == nil || resp.Error == nil {
		t.Fatalf("expected resp.Error to carry the failure, got %+v", resp)
	}

	// And it must not be recorded as divergence: every replica fails identically.
	if got := n.getReplica().ApplyErrorCount(); got != 0 {
		t.Errorf("deterministic request failure counted as divergence: ApplyErrorCount=%d", got)
	}
}
