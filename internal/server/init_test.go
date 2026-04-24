package server

import (
	"testing"
	"time"
)

func TestBootstrap(t *testing.T) {
	n, err := NewNode(NodeConfig{
		Addr:    ":0",
		DataDir: t.TempDir(),
		// NodeID deliberately left 0 → triggers InitNode → Bootstrap
	})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	defer n.Stop()

	if n.NodeID() != 1 {
		t.Fatalf("bootstrap NodeID: got %d, want 1", n.NodeID())
	}
	var zero ClusterID
	if n.ClusterID() == zero {
		t.Fatal("bootstrap ClusterID should be non-zero")
	}
}

func TestRestart(t *testing.T) {
	dir := t.TempDir()

	// First run — bootstraps.
	n1, err := NewNode(NodeConfig{Addr: ":0", DataDir: dir})
	if err != nil {
		t.Fatalf("first NewNode: %v", err)
	}
	n1.Start()
	clusterID := n1.ClusterID()
	n1.Stop()

	// Second run with same data dir — should restore persisted state.
	n2, err := NewNode(NodeConfig{Addr: ":0", DataDir: dir})
	if err != nil {
		t.Fatalf("restart NewNode: %v", err)
	}
	n2.Start()
	defer n2.Stop()

	if n2.NodeID() != 1 {
		t.Fatalf("restart NodeID: got %d, want 1", n2.NodeID())
	}
	if n2.ClusterID() != clusterID {
		t.Fatal("restart ClusterID should match original")
	}
}

func TestJoinGetsNodeID2(t *testing.T) {
	// Bootstrap node 1.
	n1, err := NewNode(NodeConfig{Addr: ":0", DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("bootstrap NewNode: %v", err)
	}
	n1.Start()
	defer n1.Stop()

	// Node 2 joins via node 1.
	n2, err := NewNode(NodeConfig{
		Addr:      ":0",
		DataDir:   t.TempDir(),
		JoinAddrs: []string{n1.RPCAddr()},
	})
	if err != nil {
		t.Fatalf("join NewNode: %v", err)
	}
	n2.Start()
	defer n2.Stop()

	if n2.NodeID() != 2 {
		t.Fatalf("joined NodeID: got %d, want 2", n2.NodeID())
	}
	if n2.ClusterID() != n1.ClusterID() {
		t.Fatal("joined node should have same ClusterID as bootstrap node")
	}
}

func TestThreeNodeFormation(t *testing.T) {
	// Bootstrap node 1.
	n1, err := NewNode(NodeConfig{Addr: ":0", DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("node 1: %v", err)
	}
	n1.Start()
	defer n1.Stop()

	// Node 2 joins via node 1.
	n2, err := NewNode(NodeConfig{
		Addr:      ":0",
		DataDir:   t.TempDir(),
		JoinAddrs: []string{n1.RPCAddr()},
	})
	if err != nil {
		t.Fatalf("node 2: %v", err)
	}
	n2.Start()
	defer n2.Stop()

	// Node 3 joins via node 1.
	n3, err := NewNode(NodeConfig{
		Addr:      ":0",
		DataDir:   t.TempDir(),
		JoinAddrs: []string{n1.RPCAddr()},
	})
	if err != nil {
		t.Fatalf("node 3: %v", err)
	}
	n3.Start()
	defer n3.Stop()

	if n1.NodeID() != 1 {
		t.Fatalf("n1 NodeID: got %d, want 1", n1.NodeID())
	}
	if n2.NodeID() != 2 {
		t.Fatalf("n2 NodeID: got %d, want 2", n2.NodeID())
	}
	if n3.NodeID() != 3 {
		t.Fatalf("n3 NodeID: got %d, want 3", n3.NodeID())
	}

	clusterID := n1.ClusterID()
	if n2.ClusterID() != clusterID || n3.ClusterID() != clusterID {
		t.Fatal("all nodes should share the same ClusterID")
	}
}

func TestThreeNodeGossipConvergence(t *testing.T) {
	// Bootstrap node 1 and add a gossip entry.
	n1, err := NewNode(NodeConfig{Addr: ":0", DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("node 1: %v", err)
	}
	n1.Start()
	defer n1.Stop()
	n1.Gossip().AddInfo("cluster-key", []byte("cluster-value"), 0)

	// Nodes 2 and 3 join via node 1.
	n2, err := NewNode(NodeConfig{
		Addr:      ":0",
		DataDir:   t.TempDir(),
		JoinAddrs: []string{n1.RPCAddr()},
	})
	if err != nil {
		t.Fatalf("node 2: %v", err)
	}
	n2.Start()
	defer n2.Stop()

	n3, err := NewNode(NodeConfig{
		Addr:      ":0",
		DataDir:   t.TempDir(),
		JoinAddrs: []string{n1.RPCAddr()},
	})
	if err != nil {
		t.Fatalf("node 3: %v", err)
	}
	n3.Start()
	defer n3.Stop()

	// Manually trigger gossip rounds: n2→n1, n3→n1, n3→n2.
	if err := n2.Gossip().GossipWithPeerForTest(n1.RPCAddr()); err != nil {
		t.Fatalf("n2→n1 gossip: %v", err)
	}
	if err := n3.Gossip().GossipWithPeerForTest(n1.RPCAddr()); err != nil {
		t.Fatalf("n3→n1 gossip: %v", err)
	}

	// n2 now has cluster-key from node 1. n3 also has it directly from n1.
	info2, ok2 := n2.Gossip().GetInfo("cluster-key")
	info3, ok3 := n3.Gossip().GetInfo("cluster-key")

	if !ok2 || string(info2.Value) != "cluster-value" {
		t.Fatal("node 2 should have cluster-key via gossip")
	}
	if !ok3 || string(info3.Value) != "cluster-value" {
		t.Fatal("node 3 should have cluster-key via gossip")
	}
}

func TestJoinUnreachableSeed(t *testing.T) {
	_, err := NewNode(NodeConfig{
		Addr:      ":0",
		DataDir:   t.TempDir(),
		JoinAddrs: []string{"127.0.0.1:1"}, // port 1 is always closed
	})
	if err == nil {
		t.Fatal("expected error joining unreachable seed")
	}
}

func TestNodeIDAllocatorSequential(t *testing.T) {
	dir := t.TempDir()

	// Bootstrap initializes next-node-id = 2.
	n1, err := NewNode(NodeConfig{Addr: ":0", DataDir: dir})
	if err != nil {
		t.Fatalf("bootstrap: %v", err)
	}
	n1.Start()
	defer n1.Stop()

	// Three successive joins should get IDs 2, 3, 4.
	for wantID := NodeID(2); wantID <= 4; wantID++ {
		n, err := NewNode(NodeConfig{
			Addr:      ":0",
			DataDir:   t.TempDir(),
			JoinAddrs: []string{n1.RPCAddr()},
		})
		if err != nil {
			t.Fatalf("join (want %d): %v", wantID, err)
		}
		n.Start()
		time.Sleep(5 * time.Millisecond)
		if n.NodeID() != wantID {
			n.Stop()
			t.Fatalf("sequential join: got %d, want %d", n.NodeID(), wantID)
		}
		n.Stop()
	}
}
