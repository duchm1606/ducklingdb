package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// S7 — leader-only reads + typed NotLeaderError redirect, exercised over the
// real gRPC transport via the D-08 Peers cluster harness.

func dialNode(t *testing.T, addr string) pb.InternalClient {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewInternalClient(conn)
}

func writeBatch(key, value []byte) *pb.BatchRequest {
	return &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: time.Now().UnixNano()}},
		Requests: []*pb.RequestUnion{{
			Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   key,
				Value: &pb.Value{RawBytes: value},
			}},
		}},
	}
}

func readBatch(key []byte) *pb.BatchRequest {
	return &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: time.Now().UnixNano()}},
		Requests: []*pb.RequestUnion{{
			Value: &pb.RequestUnion_Get{Get: &pb.GetRequest{Key: key}},
		}},
	}
}

// TestNotLeaderErrorShapeIsIdenticalOnBothPaths drives the same not-the-leader
// condition through Batch and through ExecSQL on a follower and asserts both
// return the SAME typed NotLeaderError with the same fields. Before S7 one path
// returned a bare "not the leader" string (no identity) and the other a
// formatted string (an ID but no address); neither was machine-readable and
// they disagreed.
func TestNotLeaderErrorShapeIsIdenticalOnBothPaths(t *testing.T) {
	nodes := startPeersCluster(t, 3)
	leader := waitForClusterLeader(t, nodes, 10*time.Second)
	follower := followerOf(t, nodes, leader)

	fc := dialNode(t, follower.RPCAddr())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	batchResp, err := fc.Batch(ctx, readBatch([]byte("shape-key")))
	if err != nil {
		t.Fatalf("Batch to follower: %v", err)
	}
	sqlResp, err := fc.ExecSQL(ctx, &pb.SQLRequest{Sql: "SELECT v FROM t"})
	if err != nil {
		t.Fatalf("ExecSQL to follower: %v", err)
	}

	bnl, snl := batchResp.GetNotLeader(), sqlResp.GetNotLeader()
	if bnl == nil {
		t.Fatal("Batch on a follower must return a NotLeader redirect, got none")
	}
	if snl == nil {
		t.Fatal("ExecSQL on a follower must return a NotLeader redirect, got none")
	}
	if bnl.GetLeaderNodeId() != snl.GetLeaderNodeId() || bnl.GetLeaderAddress() != snl.GetLeaderAddress() {
		t.Fatalf("Batch and ExecSQL disagree on the NotLeader shape:\n  Batch  =%+v\n  ExecSQL=%+v", bnl, snl)
	}
	if bnl.GetLeaderNodeId() != uint64(leader.NodeID()) {
		t.Fatalf("NotLeader names node %d, want the actual leader %d", bnl.GetLeaderNodeId(), leader.NodeID())
	}
	if bnl.GetLeaderAddress() != leader.RPCAddr() {
		t.Fatalf("NotLeader address %q is not the leader's dialable address %q",
			bnl.GetLeaderAddress(), leader.RPCAddr())
	}
}

// TestReplicaNotReadyIsUnavailableOnBothPaths pins the other half of the S7
// error unification: while the Raft group is still forming (getReplica() nil),
// Batch and ExecSQL both report it as a codes.Unavailable transport status.
// Before S7, Batch returned a transport error while ExecSQL folded it in-band
// into SQLResponse.Error — the same condition in two shapes.
func TestReplicaNotReadyIsUnavailableOnBothPaths(t *testing.T) {
	// A peer that never appears in gossip keeps the group perpetually forming,
	// so this node's replica stays nil.
	self := freeAddr(t)
	n, err := NewNode(NodeConfig{
		Addr:    self,
		DataDir: t.TempDir(),
		NodeID:  1,
		Peers:   []string{self, "127.0.0.1:1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	n.Start()
	t.Cleanup(n.Stop)

	if n.getReplica() != nil {
		t.Fatal("precondition: replica must not be ready with an unresolvable peer")
	}

	c := dialNode(t, n.RPCAddr())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	_, berr := c.Batch(ctx, readBatch([]byte("k")))
	_, serr := c.ExecSQL(ctx, &pb.SQLRequest{Sql: "SELECT 1"})
	if status.Code(berr) != codes.Unavailable {
		t.Fatalf("Batch not-ready: got %v (code %v), want codes.Unavailable", berr, status.Code(berr))
	}
	if status.Code(serr) != codes.Unavailable {
		t.Fatalf("ExecSQL not-ready: got %v (code %v), want codes.Unavailable", serr, status.Code(serr))
	}
}

// TestNoStaleReadFromFollower proves the gate, not absence of data, is what
// prevents a stale read: the value is replicated into the follower's own engine,
// yet the follower's API still refuses to serve it and redirects instead.
func TestNoStaleReadFromFollower(t *testing.T) {
	nodes := startPeersCluster(t, 3)
	leader := waitForClusterLeader(t, nodes, 10*time.Second)
	follower := followerOf(t, nodes, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	key, value := []byte("stale-key"), []byte("stale-value")
	lc := dialNode(t, leader.RPCAddr())
	if _, err := lc.Batch(ctx, writeBatch(key, value)); err != nil {
		t.Fatalf("leader write: %v", err)
	}

	// Wait until the write has actually replicated into the follower's local
	// engine — only then is a stale read genuinely possible to serve.
	end := append(append([]byte{}, key...), 0)
	deadline := time.Now().Add(5 * time.Second)
	present := false
	for time.Now().Before(deadline) {
		kvs, err := mvcc.MVCCScan(follower.engine, key, end,
			hlc.Timestamp{WallTime: time.Now().UnixNano()}, mvcc.ReadOptions{})
		if err == nil && len(kvs) > 0 && bytes.Equal(kvs[0].Value, value) {
			present = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !present {
		t.Fatal("precondition: value never replicated into the follower's engine")
	}

	// The follower HOLDS the value but must still refuse to serve it.
	fc := dialNode(t, follower.RPCAddr())
	resp, err := fc.Batch(ctx, readBatch(key))
	if err != nil {
		t.Fatalf("follower read: %v", err)
	}
	if resp.GetNotLeader() == nil {
		t.Fatal("follower served a read from local state — stale read not prevented")
	}
	if len(resp.GetResponses()) > 0 {
		t.Fatal("follower returned response data on a read it should have redirected")
	}
}
