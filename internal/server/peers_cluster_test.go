package server

import (
	"net"
	"testing"
	"time"
)

// D-08 — real-transport Peers cluster harness.
//
// The existing multi-node tests wire nodes with JoinAddrs, which makes each node
// its own one-member Raft group — there is no leader/follower relationship and
// no replication. These helpers instead form a single replicated group via
// Peers, over the real gRPC transport, so leader election, replication, and the
// NotLeaderError redirect can be exercised end to end.

// freeAddr returns a concrete, currently-free loopback address. Peers clusters
// need real addresses before the nodes start: a node advertises
// normalizeAddr(cfg.Addr) in gossip, and ":0" would advertise port 0, which no
// peer could dial or match. So the test pre-binds to learn a port, releases it,
// and hands the address to the node. (This is a test-side technique; it does not
// change any production API.)
func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// startPeersCluster starts n nodes (NodeIDs 1..n) wired into one replicated Raft
// group via Peers, over real gRPC. Node 1 is the gossip bootstrap; the rest join
// through it. All nodes are stopped on cleanup.
func startPeersCluster(t *testing.T, n int) []*Node {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		addrs[i] = freeAddr(t)
	}
	nodes := make([]*Node, n)
	for i := 0; i < n; i++ {
		cfg := NodeConfig{
			Addr:    addrs[i],
			DataDir: t.TempDir(),
			NodeID:  NodeID(i + 1),
			Peers:   addrs,
		}
		if i > 0 {
			cfg.JoinAddrs = []string{addrs[0]}
		}
		node, err := NewNode(cfg)
		if err != nil {
			t.Fatalf("NewNode %d: %v", i+1, err)
		}
		nodes[i] = node
	}
	for _, node := range nodes {
		node.Start()
	}
	t.Cleanup(func() {
		for _, node := range nodes {
			node.Stop()
		}
	})
	return nodes
}

// waitForClusterLeader blocks until every node has an initialised replica AND
// all of them agree on the same leader, then returns that leader.
//
// It deliberately waits for cluster-wide agreement rather than for one node to
// call itself leader: a test that probes a follower the instant a leader wins
// would otherwise catch that follower before its replica exists (getReplica()
// nil → "replica not ready") or before it has heard the leader's first append
// (Lead() == 0 → a redirect naming node 0). Both are correct transient node
// states, not product bugs — the harness simply must not probe mid-convergence.
func waitForClusterLeader(t *testing.T, nodes []*Node, timeout time.Duration) *Node {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaderID := uint64(0)
		agreed := true
		for _, n := range nodes {
			r := n.getReplica()
			if r == nil {
				agreed = false
				break
			}
			l := r.Lead()
			switch {
			case l == 0:
				agreed = false
			case leaderID == 0:
				leaderID = l
			case l != leaderID:
				agreed = false
			}
			if !agreed {
				break
			}
		}
		if agreed && leaderID != 0 {
			for _, n := range nodes {
				if uint64(n.NodeID()) == leaderID {
					return n
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("cluster did not converge on a single agreed leader within timeout")
	return nil
}

// followerOf returns a node in the group that is not the leader.
func followerOf(t *testing.T, nodes []*Node, leader *Node) *Node {
	t.Helper()
	for _, n := range nodes {
		if n.NodeID() != leader.NodeID() {
			return n
		}
	}
	t.Fatal("no follower in cluster")
	return nil
}

// TestPeersClusterElectsLeader is the harness smoke test and the first time
// MsgPreVote/MsgPreVoteResp cross the real gRPC transport. With PreVote (S4)
// every election begins with a pre-vote round; if raft_convert.go cannot carry
// those message types, the pre-vote responses never arrive and no leader is ever
// elected. A green run here proves the S3/S4 message types survive conversion.
func TestPeersClusterElectsLeader(t *testing.T) {
	nodes := startPeersCluster(t, 3)
	leader := waitForClusterLeader(t, nodes, 10*time.Second)
	t.Logf("leader elected over real transport: node %d", leader.NodeID())
}
