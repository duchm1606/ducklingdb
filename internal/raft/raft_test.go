package raft

import "testing"

// network wires multiple RawNodes together for deterministic testing.
// Each node has its own memStorage. Messages are delivered synchronously.
type testNetwork struct {
	nodes map[uint64]*RawNode
}

func newTestNetwork(ids ...uint64) *testNetwork {
	nt := &testNetwork{nodes: make(map[uint64]*RawNode)}
	peers := make([]uint64, len(ids))
	copy(peers, ids)
	for _, id := range ids {
		nt.nodes[id] = NewRawNode(id, peers, newMemStorage())
	}
	return nt
}

// deliver sends all messages produced by a node after a Tick or Step.
func (nt *testNetwork) deliverReady(id uint64) {
	n := nt.nodes[id]
	if !n.HasReady() {
		return
	}
	rd := n.Ready()
	n.Advance(rd)
	for _, msg := range rd.Messages {
		if target, ok := nt.nodes[msg.To]; ok {
			target.Step(msg)
		}
	}
}

// tickAll ticks every node and delivers resulting messages.
func (nt *testNetwork) tickAll() {
	for _, n := range nt.nodes {
		n.Tick()
	}
	for id := range nt.nodes {
		nt.deliverReady(id)
	}
}

func TestElectionSingleNode(t *testing.T) {
	nt := newTestNetwork(1)
	// Tick past election timeout (max 20 ticks)
	for i := 0; i < 25; i++ {
		nt.tickAll()
	}
	if nt.nodes[1].state != StateLeader {
		t.Fatalf("single node should become leader, got state=%d", nt.nodes[1].state)
	}
}

func TestElectionThreeNodes(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	// Tick until a leader emerges (at most 50 ticks)
	var leader uint64
	for i := 0; i < 50 && leader == 0; i++ {
		nt.tickAll()
		for id, n := range nt.nodes {
			if n.state == StateLeader {
				leader = id
			}
		}
	}
	if leader == 0 {
		t.Fatal("no leader elected within 50 ticks")
	}
	// All nodes should agree on the same leader
	for id, n := range nt.nodes {
		if n.leadID != leader && n.state != StateCandidate {
			t.Errorf("node %d thinks leader is %d, want %d", id, n.leadID, leader)
		}
	}
}

func TestHigherTermReverts(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	// Elect a leader
	for i := 0; i < 50; i++ {
		nt.tickAll()
	}
	var leader uint64
	for id, n := range nt.nodes {
		if n.state == StateLeader {
			leader = id
		}
	}
	if leader == 0 {
		t.Fatal("no leader elected")
	}
	// Send a message with a higher term to the leader
	higherTerm := nt.nodes[leader].term + 1
	var otherID uint64
	for id := range nt.nodes {
		if id != leader {
			otherID = id
			break
		}
	}
	nt.nodes[leader].Step(Message{
		Type: MsgHeartbeat,
		From: otherID,
		To:   leader,
		Term: higherTerm,
	})
	if nt.nodes[leader].state != StateFollower {
		t.Fatalf("leader should revert to follower on higher term, got %d", nt.nodes[leader].state)
	}
}
