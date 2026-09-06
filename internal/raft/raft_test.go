package raft

import "testing"

// network wires multiple RawNodes together for deterministic testing.
// Each node has its own memStorage. Messages are delivered synchronously.
type testNetwork struct {
	nodes   map[uint64]*RawNode
	storage map[uint64]*memStorage
	// partitioned nodes keep ticking (so they still time out and campaign) but
	// their inbound and outbound messages are dropped, modelling a node whose
	// network link is cut while its process keeps running.
	partitioned map[uint64]bool
}

func newTestNetwork(ids ...uint64) *testNetwork {
	nt := &testNetwork{
		nodes:       make(map[uint64]*RawNode),
		storage:     make(map[uint64]*memStorage),
		partitioned: make(map[uint64]bool),
	}
	peers := make([]uint64, len(ids))
	copy(peers, ids)
	for _, id := range ids {
		ms := newMemStorage()
		nt.storage[id] = ms
		nt.nodes[id] = NewRawNode(id, peers, ms)
	}
	return nt
}

// partition cuts node id off from the rest of the network.
func (nt *testNetwork) partition(id uint64) { nt.partitioned[id] = true }

// heal restores node id's network link.
func (nt *testNetwork) heal(id uint64) { delete(nt.partitioned, id) }

// deliver sends all messages produced by a node after a Tick or Step.
func (nt *testNetwork) deliverReady(id uint64) {
	n := nt.nodes[id]
	if !n.HasReady() {
		return
	}
	rd := n.Ready()
	// Persist unstable entries to storage so lastIndex() is accurate after Advance.
	if len(rd.Entries) > 0 {
		nt.storage[id].AppendEntries(rd.Entries)
	}
	n.Advance(rd)
	for _, msg := range rd.Messages {
		// Drop any message that would have to cross a partition boundary.
		if nt.partitioned[id] || nt.partitioned[msg.To] {
			continue
		}
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
	// Followers learn who the leader is from its first MsgApp (becomeLeader
	// appends a no-op and broadcasts it), not from having voted for it — a
	// candidate we voted for may still lose. Deliver that round before
	// asserting agreement.
	nt.tickAll()

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

func TestLogReplicationOneEntry(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	// Elect leader
	for i := 0; i < 50; i++ {
		nt.tickAll()
	}
	var leaderID uint64
	for id, n := range nt.nodes {
		if n.state == StateLeader {
			leaderID = id
		}
	}
	if leaderID == 0 {
		t.Fatal("no leader elected")
	}

	// Propose entry on leader
	nt.nodes[leaderID].Propose([]byte("hello"))
	// Deliver messages until all nodes apply
	for i := 0; i < 20; i++ {
		nt.tickAll()
	}

	// All nodes should have committed the entry
	for id, n := range nt.nodes {
		if n.log.committed < 2 { // index 1 = no-op, index 2 = "hello"
			t.Errorf("node %d: want committed>=2, got %d", id, n.log.committed)
		}
	}
}

func TestLeaderAppendsNoOpOnElection(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	for i := 0; i < 50; i++ {
		nt.tickAll()
	}
	for id, n := range nt.nodes {
		if n.state == StateLeader {
			// Leader should have appended a no-op (index 1)
			if n.log.lastIndex() < 1 {
				t.Errorf("node %d (leader) should have no-op at index 1", id)
			}
			break
		}
	}
}

func TestFollowerRejectsConflictingEntries(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	// Elect a leader with enough ticks
	for i := 0; i < 50; i++ {
		nt.tickAll()
	}
	var followerID uint64
	for id, n := range nt.nodes {
		if n.state == StateFollower {
			followerID = id
			break
		}
	}
	if followerID == 0 {
		t.Fatal("no follower found in 3-node network after 50 ticks")
	}
	// Step a bad AppendEntries into the follower
	nt.nodes[followerID].Step(Message{
		Type:    MsgApp,
		From:    1,
		To:      followerID,
		Term:    nt.nodes[followerID].term,
		Index:   99, // prevLogIndex doesn't exist
		LogTerm: 1,
	})
	// The follower should have produced a rejection message
	if nt.nodes[followerID].HasReady() {
		rd := nt.nodes[followerID].Ready()
		nt.nodes[followerID].Advance(rd)
		rejected := false
		for _, m := range rd.Messages {
			if m.Type == MsgAppResp && m.Reject {
				rejected = true
			}
		}
		if !rejected {
			t.Error("follower should reject AppendEntries with non-existent prevLogIndex")
		}
	}
}
