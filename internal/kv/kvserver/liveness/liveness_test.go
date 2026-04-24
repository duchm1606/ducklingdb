package liveness

import (
	"testing"
	"time"

	"github.com/duchm1606/ducklingdb/internal/gossip"
	"github.com/duchm1606/ducklingdb/internal/rpc"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
	"github.com/duchm1606/ducklingdb/internal/util/stop"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
)

func setupLiveness(t *testing.T, nodeID int32, wallTime int64) (*NodeLiveness, *hlc.ManualClock) {
	t.Helper()

	engine, err := lsm.OpenLSM(lsm.LSMOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenLSM: %v", err)
	}
	t.Cleanup(func() { engine.Close() })

	wall := hlc.NewManualClock(wallTime)
	clock := hlc.NewClock(wall, 500*time.Millisecond)

	stopper := stop.NewStopper()
	t.Cleanup(stopper.Stop)

	srv, err := rpc.NewServer(":0")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	rpcCtx := rpc.NewContext()
	t.Cleanup(rpcCtx.Close)

	g := gossip.New(nodeID, srv.Addr(), rpcCtx, stopper)
	pb.RegisterGossipServiceServer(srv.GRPCServer(), g)
	srv.Start()
	t.Cleanup(srv.Stop)

	nl := New(nodeID, engine, clock, g, stopper)
	nl.interval = 50 * time.Millisecond
	nl.livenessThreshold = 200 * time.Millisecond

	return nl, wall
}

func TestHeartbeatWritesRecord(t *testing.T) {
	nl, _ := setupLiveness(t, 1, 1000)

	if err := nl.Heartbeat(); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	l, err := nl.readFromEngine(1)
	if err != nil {
		t.Fatalf("readFromEngine: %v", err)
	}
	if l.NodeID != 1 {
		t.Fatalf("NodeID: got %d, want 1", l.NodeID)
	}
	if l.Epoch != 1 {
		t.Fatalf("Epoch: got %d, want 1", l.Epoch)
	}
	if l.Expiration.WallTime == 0 {
		t.Fatal("Expiration should be non-zero")
	}
}

func TestIsLiveAfterHeartbeat(t *testing.T) {
	nl, _ := setupLiveness(t, 1, 1_000_000_000) // 1s in nanos

	if err := nl.Heartbeat(); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	if !nl.IsLive(1) {
		t.Fatal("node should be live after heartbeat")
	}
}

func TestIsNotLiveAfterExpiry(t *testing.T) {
	nl, wall := setupLiveness(t, 1, 1_000_000_000)
	nl.livenessThreshold = 10 * time.Millisecond

	if err := nl.Heartbeat(); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// Advance the manual clock past the expiration.
	wall.Increment(int64(100 * time.Millisecond))

	if nl.IsLive(1) {
		t.Fatal("node should be dead after expiry")
	}
}

func TestEpochIncrementsOnRestart(t *testing.T) {
	engine, err := lsm.OpenLSM(lsm.LSMOptions{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("OpenLSM: %v", err)
	}
	defer engine.Close()

	wall := hlc.NewManualClock(1_000_000_000)
	clock := hlc.NewClock(wall, 500*time.Millisecond)
	stopper := stop.NewStopper()
	defer stopper.Stop()

	srv, _ := rpc.NewServer(":0")
	rpcCtx := rpc.NewContext()
	defer rpcCtx.Close()
	g := gossip.New(1, srv.Addr(), rpcCtx, stopper)
	pb.RegisterGossipServiceServer(srv.GRPCServer(), g)
	srv.Start()
	defer srv.Stop()

	// First "run" — epoch becomes 1.
	nl1 := New(1, engine, clock, g, stopper)
	nl1.livenessThreshold = 200 * time.Millisecond
	if err := nl1.Heartbeat(); err != nil {
		t.Fatalf("first heartbeat: %v", err)
	}
	l1, _ := nl1.readFromEngine(1)
	if l1.Epoch != 1 {
		t.Fatalf("first run epoch: got %d, want 1", l1.Epoch)
	}

	// "Restart" — new NodeLiveness instance with same engine → epoch becomes 2.
	nl2 := New(1, engine, clock, g, stopper)
	nl2.livenessThreshold = 200 * time.Millisecond
	if err := nl2.Heartbeat(); err != nil {
		t.Fatalf("restart heartbeat: %v", err)
	}
	l2, _ := nl2.readFromEngine(1)
	if l2.Epoch != 2 {
		t.Fatalf("restart epoch: got %d, want 2", l2.Epoch)
	}
}

func TestLivenessGossipPropagation(t *testing.T) {
	// Node 1 heartbeats → node 2 learns its liveness via gossip exchange.
	stopper := stop.NewStopper()
	defer stopper.Stop()

	// --- Node 1 setup ---
	engine1, _ := lsm.OpenLSM(lsm.LSMOptions{Dir: t.TempDir()})
	defer engine1.Close()
	clock1 := hlc.NewClock(hlc.NewManualClock(1_000_000_000), 500*time.Millisecond)
	srv1, _ := rpc.NewServer(":0")
	rpcCtx1 := rpc.NewContext()
	defer rpcCtx1.Close()
	g1 := gossip.New(1, srv1.Addr(), rpcCtx1, stopper)
	pb.RegisterGossipServiceServer(srv1.GRPCServer(), g1)
	srv1.Start()
	defer srv1.Stop()
	nl1 := New(1, engine1, clock1, g1, stopper)
	nl1.livenessThreshold = 5 * time.Second

	// --- Node 2 setup ---
	engine2, _ := lsm.OpenLSM(lsm.LSMOptions{Dir: t.TempDir()})
	defer engine2.Close()
	clock2 := hlc.NewClock(hlc.NewManualClock(1_000_000_000), 500*time.Millisecond)
	srv2, _ := rpc.NewServer(":0")
	rpcCtx2 := rpc.NewContext()
	defer rpcCtx2.Close()
	g2 := gossip.New(2, srv2.Addr(), rpcCtx2, stopper)
	pb.RegisterGossipServiceServer(srv2.GRPCServer(), g2)
	srv2.Start()
	defer srv2.Stop()
	nl2 := New(2, engine2, clock2, g2, stopper)

	// Node 1 heartbeats → gossips its liveness.
	if err := nl1.Heartbeat(); err != nil {
		t.Fatalf("nl1.Heartbeat: %v", err)
	}

	// Trigger a gossip exchange: node 2 pulls from node 1.
	g2.AddPeer(srv1.Addr())
	conn, err := rpcCtx2.GRPCDialNode(srv1.Addr())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	_ = conn

	// Use the internal gossipWithPeer to do one synchronous round.
	if err := triggerGossip(g2, srv1.Addr()); err != nil {
		t.Fatalf("gossip exchange: %v", err)
	}

	// Node 2 should now have node 1's liveness in its cache.
	l, err := nl2.GetLiveness(1)
	if err != nil {
		t.Fatalf("GetLiveness: %v", err)
	}
	if l.NodeID != 1 {
		t.Fatalf("NodeID: got %d, want 1", l.NodeID)
	}
	if l.Epoch != 1 {
		t.Fatalf("Epoch: got %d, want 1", l.Epoch)
	}
}

// triggerGossip calls gossipWithPeer via reflection-free exported wrapper.
// We expose it only for tests by calling the unexported method through a
// test-only helper function in the same package.
func triggerGossip(g *gossip.Gossip, addr string) error {
	return g.GossipWithPeerForTest(addr)
}
