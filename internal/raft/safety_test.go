package raft

import "testing"

// TestHeartbeatCommitClampedToMatchIndex pins a Raft safety rule that the
// original implementation violated.
//
// bcastHeartbeat used to send `Commit: rn.log.committed` to every peer
// unconditionally, and the receiver advanced its commit index with
// `commitTo(min64(m.Commit, lastIndex()))` — no log-match check anywhere. A
// follower holding uncommitted entries from an older term at higher indices
// would therefore commit *its own divergent entries* up to the leader's commit
// index, and apply them to its state machine.
//
// etcd/raft sends min(pr.Match, r.raftLog.committed) precisely to prevent this:
// never tell a follower to commit past what we know it has matching.
func TestHeartbeatCommitClampedToMatchIndex(t *testing.T) {
	n := NewRawNode(1, []uint64{1, 2, 3}, newMemStorage())
	n.becomeCandidate()
	n.becomeLeader()

	// The leader has committed everything it holds.
	last := n.log.lastIndex()
	n.log.commitTo(last)
	if n.log.committed == 0 {
		t.Fatal("precondition: leader should have a non-zero commit index")
	}

	// Node 3 is fully caught up; node 2 has matched nothing yet — it may be
	// holding entries the leader is about to overwrite.
	n.matchIndex[2] = 0
	n.matchIndex[3] = last

	n.msgs = nil
	n.bcastHeartbeat()

	var checked bool
	for _, m := range n.msgs {
		if m.Type != MsgHeartbeat || m.To != 2 {
			continue
		}
		checked = true
		if m.Commit > n.matchIndex[2] {
			t.Fatalf("heartbeat told node 2 to commit up to %d, but node 2 has only matched %d: "+
				"a follower with divergent entries would commit them", m.Commit, n.matchIndex[2])
		}
	}
	if !checked {
		t.Fatal("no heartbeat was sent to node 2")
	}
}

// TestHeartbeatCommitStillAdvancesMatchedFollower guards the fix from being
// implemented as "never send a commit index", which would be safe but would
// stop followers ever learning what is committed.
func TestHeartbeatCommitStillAdvancesMatchedFollower(t *testing.T) {
	n := NewRawNode(1, []uint64{1, 2, 3}, newMemStorage())
	n.becomeCandidate()
	n.becomeLeader()

	last := n.log.lastIndex()
	n.log.commitTo(last)
	n.matchIndex[2] = last
	n.matchIndex[3] = last

	n.msgs = nil
	n.bcastHeartbeat()

	for _, m := range n.msgs {
		if m.Type == MsgHeartbeat && m.To == 2 {
			if m.Commit != last {
				t.Fatalf("a fully-matched follower should be told commit=%d, got %d", last, m.Commit)
			}
			return
		}
	}
	t.Fatal("no heartbeat was sent to node 2")
}

// TestVoteGrantDoesNotSetLeader pins that granting a vote does not record the
// candidate as leader.
//
// The original code did `rn.leadID = m.From` when granting a vote, described as
// "optimistic". leadID is surfaced by Replica.Lead(), which gates Propose and
// produces the client-facing "leader is node N" message — so a follower that
// voted for a candidate which then *lost* would direct clients to a node that
// is not the leader, until the next MsgApp corrected it.
func TestVoteGrantDoesNotSetLeader(t *testing.T) {
	n := NewRawNode(1, []uint64{1, 2, 3}, newMemStorage())

	// Node 2 campaigns at a higher term with an up-to-date (empty) log.
	n.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 2, Index: 0, LogTerm: 0})

	if n.votedFor != 2 {
		t.Fatalf("precondition: expected the vote to be granted to node 2, votedFor=%d", n.votedFor)
	}
	if n.leadID != none {
		t.Fatalf("granting a vote must not set leadID (candidate may lose); got leadID=%d", n.leadID)
	}
}
