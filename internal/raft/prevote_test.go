package raft

import "testing"

// S4 — PreVote + CheckQuorum.
//
// These tests use the preVote / checkQuorum flags (default true in NewRawNode)
// as an in-binary mutation harness: the positive tests run with the protection
// on and assert it holds; the sibling counterfactual tests turn the SAME
// protection off and assert the disruption returns. If a counterfactual ever
// starts passing with the protection off, its positive twin proves nothing —
// the scenario would no longer exercise the failure the protection prevents.

// stabilizeLeader ticks the network until a leader emerges, then lets a few
// heartbeat rounds propagate so followers record leadID and reset their timers.
func stabilizeLeader(t *testing.T, nt *testNetwork) uint64 {
	t.Helper()
	var leader uint64
	for i := 0; i < 200 && leader == 0; i++ {
		nt.tickAll()
		for id, n := range nt.nodes {
			if n.state == StateLeader {
				leader = id
			}
		}
	}
	if leader == 0 {
		t.Fatal("no leader elected within 200 ticks")
	}
	for i := 0; i < 5; i++ {
		nt.tickAll()
	}
	return leader
}

// pickFollower returns a node that is neither the leader nor currently a leader.
func pickFollower(t *testing.T, nt *testNetwork, leader uint64) uint64 {
	t.Helper()
	for id, n := range nt.nodes {
		if id != leader && n.state != StateLeader {
			return id
		}
	}
	t.Fatal("no follower found")
	return 0
}

// TestPreVoteDoesNotDisruptLeader is the S4 acceptance test (roadmap S4
// "Verify"). A follower is isolated for well over 10x the election timeout and
// then healed. With PreVote the follower cannot inflate its term while away,
// and with the CheckQuorum lease the connected majority refuses its probes, so
// the sitting leader keeps both its term and its leadership.
func TestPreVoteDoesNotDisruptLeader(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	leader := stabilizeLeader(t, nt)
	origTerm := nt.nodes[leader].term
	follower := pickFollower(t, nt, leader)

	nt.partition(follower)
	for i := 0; i < 10*electionTimeoutMax; i++ {
		nt.tickAll()
	}

	// PreVote: the isolated follower probes at term+1 without adopting it, so
	// its term must not have moved.
	if got := nt.nodes[follower].term; got > origTerm {
		t.Fatalf("isolated follower inflated its term to %d (was %d): PreVote must keep it flat",
			got, origTerm)
	}

	nt.heal(follower)
	for i := 0; i < 3*electionTimeoutMax; i++ {
		nt.tickAll()
	}

	if st := nt.nodes[leader].state; st != StateLeader {
		t.Fatalf("returning follower disrupted leader %d: state=%d, want StateLeader", leader, st)
	}
	if got := nt.nodes[leader].term; got != origTerm {
		t.Fatalf("returning follower moved leader %d's term %d -> %d (leadership churned)",
			leader, origTerm, got)
	}
	if got := nt.nodes[follower].leadID; got != leader {
		t.Fatalf("returned follower %d did not rejoin under leader %d (leadID=%d)",
			follower, leader, got)
	}
}

// TestWithoutPreVoteReturningFollowerDisruptsLeader is the counterfactual for
// TestPreVoteDoesNotDisruptLeader: with PreVote and CheckQuorum off, the same
// isolation-then-heal sequence DOES disrupt the leader (its term is forced up).
// This proves the scenario genuinely exercises the disruption the positive test
// claims to prevent.
func TestWithoutPreVoteReturningFollowerDisruptsLeader(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	for _, n := range nt.nodes {
		n.preVote = false
		n.checkQuorum = false
	}
	leader := stabilizeLeader(t, nt)
	origTerm := nt.nodes[leader].term
	follower := pickFollower(t, nt, leader)

	nt.partition(follower)
	for i := 0; i < 10*electionTimeoutMax; i++ {
		nt.tickAll()
	}
	// Without PreVote the isolated follower campaigns on every timeout and
	// inflates its term.
	if got := nt.nodes[follower].term; got <= origTerm {
		t.Fatalf("precondition: without PreVote the isolated follower should inflate its term above %d, got %d",
			origTerm, got)
	}

	nt.heal(follower)
	for i := 0; i < 3*electionTimeoutMax; i++ {
		nt.tickAll()
	}

	// The inflated-term follower must have knocked the sitting leader off its
	// original term (either it stepped down, or it re-won at a higher term).
	if nt.nodes[leader].state == StateLeader && nt.nodes[leader].term == origTerm {
		t.Fatalf("expected leader %d to be disrupted (term should move past %d), but it stayed leader "+
			"at the same term: the scenario no longer exercises the disruption",
			leader, origTerm)
	}
}

// TestCheckQuorumLeaderStepsDownWhenIsolated pins CheckQuorum's leader side: a
// leader that can no longer reach a quorum steps down on its own, rather than
// clinging to leadership and lease-blocking the majority forever.
func TestCheckQuorumLeaderStepsDownWhenIsolated(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	leader := stabilizeLeader(t, nt)

	nt.partition(leader)
	for i := 0; i < 3*electionTimeoutMax; i++ {
		nt.tickAll()
	}
	if st := nt.nodes[leader].state; st == StateLeader {
		t.Fatalf("isolated leader %d should have stepped down via CheckQuorum, still state=%d", leader, st)
	}
}

// TestWithoutCheckQuorumIsolatedLeaderStaysLeader is the counterfactual: with
// CheckQuorum off, an isolated leader never steps down. This confirms the
// step-down in the positive test is attributable to CheckQuorum and not to some
// other path.
func TestWithoutCheckQuorumIsolatedLeaderStaysLeader(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	for _, n := range nt.nodes {
		n.checkQuorum = false
	}
	leader := stabilizeLeader(t, nt)

	nt.partition(leader)
	for i := 0; i < 3*electionTimeoutMax; i++ {
		nt.tickAll()
	}
	if st := nt.nodes[leader].state; st != StateLeader {
		t.Fatalf("without CheckQuorum an isolated leader must not step down, but leader %d is state=%d",
			leader, st)
	}
}

// TestElectionSucceedsAfterLeaderIsolated is the liveness guard for the
// CheckQuorum lease: when the leader genuinely fails, the surviving majority
// must still elect a new leader. The lease each survivor holds on the old
// leader must EXPIRE, not deadlock elections.
func TestElectionSucceedsAfterLeaderIsolated(t *testing.T) {
	nt := newTestNetwork(1, 2, 3)
	leader := stabilizeLeader(t, nt)

	nt.partition(leader)
	var newLeader uint64
	for i := 0; i < 10*electionTimeoutMax && newLeader == 0; i++ {
		nt.tickAll()
		for id, n := range nt.nodes {
			if id != leader && n.state == StateLeader {
				newLeader = id
			}
		}
	}
	if newLeader == 0 {
		t.Fatalf("majority failed to elect a new leader after isolating leader %d: "+
			"the CheckQuorum lease may be deadlocking elections", leader)
	}
}

// TestLeaseHolderIgnoresHigherTermVote is a focused unit test for the lease:
// a node that just heard from its leader ignores a higher-term vote entirely —
// no term change, no step down, no response.
func TestLeaseHolderIgnoresHigherTermVote(t *testing.T) {
	n := NewRawNode(1, []uint64{1, 2, 3}, newMemStorage())
	n.Step(Message{Type: MsgHeartbeat, From: 2, To: 1, Term: 5, Commit: 0})
	if n.leadID != 2 || n.term != 5 {
		t.Fatalf("precondition: expected to follow leader 2 at term 5, got leadID=%d term=%d", n.leadID, n.term)
	}

	n.msgs = nil
	n.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 6, Index: 10, LogTerm: 5})

	if n.term != 5 {
		t.Fatalf("CheckQuorum lease should suppress the higher-term vote: term moved 5 -> %d", n.term)
	}
	if n.leadID != 2 {
		t.Fatalf("lease should keep us following leader 2, got leadID=%d", n.leadID)
	}
	if len(n.msgs) != 0 {
		t.Fatalf("a lease-suppressed vote must draw no response, got %d messages", len(n.msgs))
	}
}

// TestWithoutCheckQuorumHigherTermVoteStepsUsDown is the counterfactual: with
// CheckQuorum off, the same higher-term vote forces us to adopt the new term.
func TestWithoutCheckQuorumHigherTermVoteStepsUsDown(t *testing.T) {
	n := NewRawNode(1, []uint64{1, 2, 3}, newMemStorage())
	n.checkQuorum = false
	n.Step(Message{Type: MsgHeartbeat, From: 2, To: 1, Term: 5, Commit: 0})

	n.msgs = nil
	n.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 6, Index: 10, LogTerm: 5})

	if n.term != 6 {
		t.Fatalf("without CheckQuorum a higher-term vote must move our term to 6, got %d", n.term)
	}
}

// TestPreVoteProbeDoesNotBumpTerm pins that a leader receiving a lone PreVote
// probe at a higher term does not adopt that term or step down: a probe carries
// a term the prober has not itself committed to.
func TestPreVoteProbeDoesNotBumpTerm(t *testing.T) {
	n := NewRawNode(1, []uint64{1, 2, 3}, newMemStorage())
	n.becomeCandidate() // term -> 1
	n.becomeLeader()
	leadTerm := n.term

	n.msgs = nil
	// A probe from node 2 at term+1. Node 2's log is empty, so this probe would
	// be rejected on log grounds even if evaluated — but the point is the term.
	n.Step(Message{Type: MsgPreVote, From: 2, To: 1, Term: leadTerm + 1, Index: 0, LogTerm: 0})

	if n.term != leadTerm {
		t.Fatalf("a PreVote probe moved the leader's term %d -> %d", leadTerm, n.term)
	}
	if n.state != StateLeader {
		t.Fatalf("a PreVote probe knocked the leader out of StateLeader (state=%d)", n.state)
	}
}
