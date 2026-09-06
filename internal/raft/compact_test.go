package raft

import "testing"

// TestCompactableIndexCapsAtLaggingFollower pins the truncation constraint: a
// leader must never report a compaction point past what a lagging follower
// still needs from the log, even when a much newer snapshot exists. Truncating
// to the snapshot would leave that follower with neither the entries nor (if its
// snapshot transfer fails) a snapshot to recover from.
func TestCompactableIndexCapsAtLaggingFollower(t *testing.T) {
	n := NewRawNode(1, []uint64{1, 2, 3}, newMemStorage())
	n.becomeCandidate()
	n.becomeLeader()

	// A snapshot exists at 100. Node 2 is caught up; node 3 lags at 5.
	n.matchIndex[1] = 100
	n.matchIndex[2] = 100
	n.matchIndex[3] = 5

	if got := n.CompactableIndex(100); got != 5 {
		t.Fatalf("leader must cap truncation at the lagging follower's match (5), got %d: "+
			"truncating to the snapshot index 100 would discard entries node 3 still needs", got)
	}

	// Once node 3 catches up (e.g. after receiving a snapshot), truncation may
	// proceed all the way to the snapshot index.
	n.matchIndex[3] = 100
	if got := n.CompactableIndex(100); got != 100 {
		t.Fatalf("with every follower caught up, truncation should reach the snapshot index 100, got %d", got)
	}
}

// TestCompactableIndexFollowerUsesSnapshotIndex pins that a non-leader compacts
// up to its snapshot: it has applied everything through it and serves entries to
// no one, so there is nothing to hold the log back.
func TestCompactableIndexFollowerUsesSnapshotIndex(t *testing.T) {
	n := NewRawNode(1, []uint64{1, 2, 3}, newMemStorage())
	if n.state == StateLeader {
		t.Fatal("precondition: a fresh node must not be a leader")
	}
	if got := n.CompactableIndex(42); got != 42 {
		t.Fatalf("a follower should compact up to its snapshot index 42, got %d", got)
	}
}
