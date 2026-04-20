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
