package gossip

import (
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/rpc"
	"github.com/duchm1606/ducklingdb/internal/util/stop"
)

// startGossipNode creates a gossip instance with its own gRPC server and starts both.
func startGossipNode(t *testing.T, nodeID int32) (*Gossip, string) {
	t.Helper()

	stopper := stop.NewStopper()
	t.Cleanup(stopper.Stop)

	srv, err := rpc.NewServer(":0")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	rpcCtx := rpc.NewContext()
	t.Cleanup(rpcCtx.Close)

	addr := srv.Addr()
	g := New(nodeID, addr, rpcCtx, stopper)

	pb.RegisterGossipServiceServer(srv.GRPCServer(), g)
	srv.Start()
	t.Cleanup(srv.Stop)

	return g, addr
}

func TestGossipTwoNodesExchange(t *testing.T) {
	g1, addr1 := startGossipNode(t, 1)
	g2, _ := startGossipNode(t, 2)

	// Node 1 has info that node 2 doesn't.
	g1.AddInfo("test-key", []byte("test-value"), 0)

	// Trigger a single gossip round from node 2 → node 1.
	g2.AddPeer(addr1)
	if err := g2.gossipWithPeer(addr1); err != nil {
		t.Fatalf("gossipWithPeer: %v", err)
	}

	info, ok := g2.GetInfo("test-key")
	if !ok {
		t.Fatal("node 2 should have received test-key")
	}
	if string(info.Value) != "test-value" {
		t.Fatalf("got %q, want %q", info.Value, "test-value")
	}
}

func TestGossipDeltaOnlyFresh(t *testing.T) {
	g1, addr1 := startGossipNode(t, 1)
	g2, _ := startGossipNode(t, 2)

	// Both nodes add their own info.
	g1.AddInfo("from-1", []byte("a"), 0)
	g2.AddInfo("from-2", []byte("b"), 0)

	// First exchange: both sync.
	g2.AddPeer(addr1)
	if err := g2.gossipWithPeer(addr1); err != nil {
		t.Fatalf("first gossip: %v", err)
	}

	// Verify cross-sync.
	if _, ok := g2.GetInfo("from-1"); !ok {
		t.Fatal("node 2 should have from-1 after first exchange")
	}
	if _, ok := g1.GetInfo("from-2"); !ok {
		t.Fatal("node 1 should have from-2 after first exchange")
	}

	// Second exchange: nothing new — delta should be empty on both sides.
	// We verify indirectly by checking counts didn't change.
	before := len(g1.store.AllInfos())
	if err := g2.gossipWithPeer(addr1); err != nil {
		t.Fatalf("second gossip: %v", err)
	}
	after := len(g1.store.AllInfos())
	if before != after {
		t.Fatalf("second exchange should not add items: before=%d after=%d", before, after)
	}
}

func TestGossipThreeNodeChain(t *testing.T) {
	// A ↔ B ↔ C, but A and C are not directly connected.
	// Info added on A should reach C via B.
	g1, addr1 := startGossipNode(t, 1)
	g2, addr2 := startGossipNode(t, 2)
	g3, _ := startGossipNode(t, 3)

	g1.AddInfo("chain-key", []byte("chain-value"), 0)

	// B gossips with A → B learns chain-key.
	g2.AddPeer(addr1)
	if err := g2.gossipWithPeer(addr1); err != nil {
		t.Fatalf("B→A gossip: %v", err)
	}

	// C gossips with B → C learns chain-key via B.
	g3.AddPeer(addr2)
	if err := g3.gossipWithPeer(addr2); err != nil {
		t.Fatalf("C→B gossip: %v", err)
	}

	info, ok := g3.GetInfo("chain-key")
	if !ok {
		t.Fatal("node C should have chain-key after two hops")
	}
	if string(info.Value) != "chain-value" {
		t.Fatalf("got %q, want %q", info.Value, "chain-value")
	}
}

func TestGossipTTLExpiry(t *testing.T) {
	g1, addr1 := startGossipNode(t, 1)
	g2, _ := startGossipNode(t, 2)

	// Add info with a very short TTL.
	g1.AddInfo("short-lived", []byte("v"), 1*time.Millisecond)

	// Wait for it to expire.
	time.Sleep(10 * time.Millisecond)

	// Even after gossip exchange, the expired item should not appear.
	g2.AddPeer(addr1)
	if err := g2.gossipWithPeer(addr1); err != nil {
		t.Fatalf("gossipWithPeer: %v", err)
	}

	if _, ok := g2.GetInfo("short-lived"); ok {
		t.Fatal("expired info should not propagate")
	}
}

func TestGossipPeerDiscovery(t *testing.T) {
	g1, addr1 := startGossipNode(t, 1)
	g2, addr2 := startGossipNode(t, 2)

	// Node 1 doesn't know about node 2 initially.
	g2.AddPeer(addr1)
	if err := g2.gossipWithPeer(addr1); err != nil {
		t.Fatalf("gossipWithPeer: %v", err)
	}

	// After exchange, node 1 should have learned node 2's address.
	g1.mu.RLock()
	_, learned := g1.peers[addr2]
	g1.mu.RUnlock()

	if !learned {
		t.Fatalf("node 1 should have discovered node 2's address %s", addr2)
	}
}
