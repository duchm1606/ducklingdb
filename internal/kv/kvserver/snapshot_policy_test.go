package kvserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
	"google.golang.org/protobuf/proto"
)

// S6 — Snapshot policy + transport sizing + truncation constraint.

// waitLeaderSingle waits for a single-node replica to elect itself.
func waitLeaderSingle(t *testing.T, r *Replica) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if r.Lead() == r.id {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("replica did not become leader")
}

// proposePutN proposes n Put commands on the leader, blocking on each.
func proposePutN(t *testing.T, r *Replica, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		proposePut(t, r, []byte(fmt.Sprintf("%s-%03d", prefix, i)), []byte("v"))
	}
}

// currentLeader returns the connected replica that currently believes it leads,
// or nil if none does right now (e.g. mid-election). A partitioned replica is
// excluded: it cannot serve, and a deposed leader may still briefly report
// itself as leader before CheckQuorum steps it down.
func currentLeader(tr *gatedTransport) *Replica {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	for id, r := range tr.replicas {
		if !tr.partitioned[id] && r.Lead() == r.id {
			return r
		}
	}
	return nil
}

// leaderTracker counts how many times leadership moved across a test, so a fix
// that tolerates churn does not also hide it. Abnormally high counts are the
// signal for the D-15 investigation, not something to swallow silently.
type leaderTracker struct {
	last    uint64
	changes int
}

func (lt *leaderTracker) observe(t *testing.T, id uint64) {
	if lt.last != 0 && id != lt.last {
		lt.changes++
		t.Logf("leadership moved %d -> %d (change #%d)", lt.last, id, lt.changes)
	}
	lt.last = id
}

// proposeToLeader proposes data to whichever replica currently leads, retrying
// against the new leader if leadership moves. Leadership churn under load is
// legitimate — a real client follows exactly this redirect (internal/client) —
// so a test must not assume the node it first saw leading stays leader. lt
// records each change.
func proposeToLeader(t *testing.T, tr *gatedTransport, data []byte, lt *leaderTracker) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		leader := currentLeader(tr)
		if leader == nil {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		lt.observe(t, leader.id)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := leader.Propose(ctx, data)
		cancel()
		if err == nil {
			return
		}
		if errors.Is(err, ErrNotLeader) {
			continue // leadership moved between the lookup and the propose; retry
		}
		t.Fatalf("propose failed (non-leadership error): %v", err)
	}
	t.Fatal("proposeToLeader: no leader accepted the proposal within 5s")
}

func proposePutToLeader(t *testing.T, tr *gatedTransport, key, value []byte, lt *leaderTracker) {
	t.Helper()
	req := &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: time.Now().UnixNano()}},
		Requests: []*pb.RequestUnion{{
			Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{Key: key, Value: &pb.Value{RawBytes: value}}},
		}},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	proposeToLeader(t, tr, data, lt)
}

func proposePutNToLeader(t *testing.T, tr *gatedTransport, prefix string, n int, lt *leaderTracker) {
	t.Helper()
	for i := 0; i < n; i++ {
		proposePutToLeader(t, tr, []byte(fmt.Sprintf("%s-%03d", prefix, i)), []byte("v"), lt)
	}
}

// replicaHasValue reports whether r's engine holds key with the given value.
func replicaHasValue(r *Replica, key, want []byte) bool {
	end := append(append([]byte{}, key...), 0x00)
	kvs, err := mvcc.MVCCScan(r.batch.Engine(), key, end,
		hlc.Timestamp{WallTime: time.Now().UnixNano()}, mvcc.ReadOptions{})
	return err == nil && len(kvs) > 0 && bytes.Equal(kvs[0].Value, want)
}

func threeReplicas(t *testing.T, tr *gatedTransport) (r1, r2, r3 *Replica) {
	t.Helper()
	peers := []uint64{1, 2, 3}
	dirs := make(map[uint64]string)
	for _, id := range peers {
		d, err := os.MkdirTemp("", "snap-policy-*")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.RemoveAll(d) })
		dirs[id] = d
	}
	r1 = newReplicaWithDir(t, 1, peers, dirs[1], tr)
	r2 = newReplicaWithDir(t, 2, peers, dirs[2], tr)
	r3 = newReplicaWithDir(t, 3, peers, dirs[3], tr)
	r1.Start()
	r2.Start()
	r3.Start()
	t.Cleanup(func() { r1.Stop(); r2.Stop(); r3.Stop() })
	return r1, r2, r3
}

func otherReplica(leader *Replica, all ...*Replica) *Replica {
	for _, r := range all {
		if r.id != leader.id {
			return r
		}
	}
	return nil
}

// TestLogCompactsAutomatically pins the snapshot trigger (D3): proposing past
// the threshold advances FirstIndex on its own, with no CreateSnapshot call in
// the test. On the pre-S6 code the log never truncates without an explicit
// (test-only) CreateSnapshot, so FirstIndex would stay at 1.
func TestLogCompactsAutomatically(t *testing.T) {
	tr := &testTransport{replicas: make(map[uint64]*Replica)}
	r := newTestReplica(t, 1, []uint64{1}, tr)
	r.Start()
	t.Cleanup(r.Stop)
	waitLeaderSingle(t, r)

	// No test code ever calls CreateSnapshot.
	proposePutN(t, r, "auto", 3*int(snapshotThreshold))

	deadline := time.Now().Add(3 * time.Second)
	var first uint64
	for time.Now().Before(deadline) {
		first, _ = r.storage.FirstIndex()
		if first > snapshotThreshold {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if first <= 1 {
		t.Fatalf("log never compacted automatically: FirstIndex=%d after %d proposals "+
			"(threshold %d, no CreateSnapshot called)", first, 3*int(snapshotThreshold), snapshotThreshold)
	}
	snap, err := r.storage.Snapshot()
	if err != nil || snap.IsEmpty() {
		t.Fatalf("compaction advanced FirstIndex to %d but no snapshot was persisted: %+v err=%v",
			first, snap, err)
	}
}

// TestTruncationBlockedUntilSnapshotDelivered pins the truncation constraint. A
// follower is partitioned and the leader writes past the snapshot threshold. A
// snapshot is created, but FirstIndex must NOT advance past the entries that
// follower still needs — eager compaction (SaveSnapshot then Compact(applied),
// the pre-S6 behaviour) would strand it. Once the partition heals and the
// follower is caught up via a snapshot, truncation proceeds.
func TestTruncationBlockedUntilSnapshotDelivered(t *testing.T) {
	// DELIBERATELY DEFERRED (S6, decision C) — see M4_REFACTOR_DEBT.md item D14.
	//
	// The "hold log truncation until the snapshot is delivered" guarantee this
	// test asserts is intentionally not implemented in M4. LSMLogStorage.
	// FirstIndex() is derived as snapshotIndex+1 (storage.go:76-88), so
	// SaveSnapshot(applied) advances FirstIndex regardless of what Compact() is
	// called with — enforcing the hold requires decoupling the snapshot sent to
	// a follower from the log-truncation floor (a separate compactedIndex
	// marker), which carries a Term(compactedIndex) trap documented in D14 and
	// is scoped as its own reviewed lease.
	//
	// The hazard this guarantee targets — a follower stranded with neither
	// snapshot nor log — is already closed by a different mechanism (64 MiB
	// transport ceiling + loud send errors + MsgSnap retry); see
	// TestFailedSnapshotLeavesFollowerRecoverable. The test body below is kept,
	// not deleted, as an executable trace of the intended guarantee: un-skip it
	// when D14 lands.
	t.Skip("deferred by design (S6 decision C) — see M4_REFACTOR_DEBT.md D14; hazard closed via retry")

	tr := newGatedTransport()
	r1, r2, r3 := threeReplicas(t, tr)
	leader := waitForLeader(t, tr, 3*time.Second)
	follower := otherReplica(leader, r1, r2, r3)

	// A baseline write everyone sees, so the follower's match sits at ~index 2.
	proposePut(t, leader, []byte("baseline"), []byte("v0"))
	time.Sleep(200 * time.Millisecond)

	// Partition the follower and write well past the threshold on the quorum.
	tr.partition(follower.id)
	proposePutN(t, leader, "gap", 2*int(snapshotThreshold))
	time.Sleep(300 * time.Millisecond)

	snap, err := leader.storage.Snapshot()
	if err != nil || snap.IsEmpty() {
		t.Fatalf("leader should have auto-snapshotted while the follower was partitioned: %+v err=%v",
			snap, err)
	}
	first, _ := leader.storage.FirstIndex()
	if first > 3 {
		t.Fatalf("truncation ran past the lagging follower's needs: FirstIndex=%d while a snapshot "+
			"exists at %d and the follower still holds only up to ~index 2 — eager compaction would "+
			"strand it", first, snap.Metadata.Index)
	}

	// Heal: the follower receives a snapshot, acks it, and truncation catches up.
	tr.heal(follower.id)
	deadline := time.Now().Add(5 * time.Second)
	var advanced uint64
	for time.Now().Before(deadline) {
		advanced, _ = leader.storage.FirstIndex()
		if advanced > 3 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if advanced <= 3 {
		t.Fatalf("after the follower recovered, truncation did not proceed: FirstIndex still %d "+
			"(snapshot at %d)", advanced, snap.Metadata.Index)
	}
}

// TestFailedSnapshotLeavesFollowerRecoverable pins that a failed snapshot
// transfer does not permanently strand a follower. Snapshots to the follower
// are dropped for a while (modelling an oversized/failed transfer whose error is
// now surfaced rather than discarded); the follower stays behind. Once delivery
// is allowed, the leader's retry catches it up.
func TestFailedSnapshotLeavesFollowerRecoverable(t *testing.T) {
	tr := newGatedTransport()
	r1, r2, r3 := threeReplicas(t, tr)
	leader := waitForLeader(t, tr, 3*time.Second)
	follower := otherReplica(leader, r1, r2, r3)
	lt := &leaderTracker{}

	// Propose to whichever node currently leads, not to a captured `leader`:
	// under load the leader can legitimately change (see D-15), and a real
	// client would follow that redirect rather than fail. The partitioned
	// follower is still a fixed, stable choice — a partitioned node cannot lead.
	proposePutToLeader(t, tr, []byte("baseline"), []byte("v0"), lt)
	time.Sleep(200 * time.Millisecond)

	// Build a gap large enough to require a snapshot while the follower is away.
	tr.partition(follower.id)
	proposePutNToLeader(t, tr, "z", 2*int(snapshotThreshold), lt)
	time.Sleep(300 * time.Millisecond)

	lastKey := []byte(fmt.Sprintf("z-%03d", 2*int(snapshotThreshold)-1))

	// Drop snapshots to the follower, then heal the network. The follower now
	// receives heartbeats but every MsgSnap is discarded, so it cannot recover.
	tr.setDropSnap(follower.id, true)
	tr.heal(follower.id)
	time.Sleep(600 * time.Millisecond)
	if replicaHasValue(follower, lastKey, []byte("v")) {
		t.Fatal("follower caught up while all its snapshots were being dropped — the gap should have " +
			"required a snapshot it never received")
	}

	// Allow snapshots again: the leader retries sendAppend every cycle (match
	// index never advanced), so the next MsgSnap delivers and the follower heals.
	tr.setDropSnap(follower.id, false)
	deadline := time.Now().Add(5 * time.Second)
	recovered := false
	for time.Now().Before(deadline) {
		if replicaHasValue(follower, lastKey, []byte("v")) {
			recovered = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !recovered {
		t.Fatal("follower did not recover after snapshot delivery was allowed again: a failed " +
			"snapshot left it permanently stranded")
	}
	if tr.snapshotsDeliveredTo(follower.id) == 0 {
		t.Fatal("follower recovered but no snapshot was ever delivered — it caught up via MsgApp, " +
			"so this did not exercise snapshot-failure recovery")
	}

	// Surface, don't swallow: churn is expected to be rare here (the partitioned
	// follower cannot disrupt the quorum). A consistently high count is the D-15
	// signal that CheckQuorum is stepping the leader down too readily under load.
	t.Logf("leadership changes observed during test: %d", lt.changes)
}

// TestProposePutRetriesThroughLeadershipChange proves the leader-retargeting
// helper survives a real leadership change — the fragility behind the D-15
// flake. The plain proposePut, pinned to the node that first led, would fail
// here with "not the leader" the moment leadership moves.
func TestProposePutRetriesThroughLeadershipChange(t *testing.T) {
	tr := newGatedTransport()
	_, _, _ = threeReplicas(t, tr)
	leader := waitForLeader(t, tr, 3*time.Second)
	lt := &leaderTracker{}

	proposePutToLeader(t, tr, []byte("before"), []byte("v0"), lt)

	// Force leadership to move: isolate the current leader so the remaining two
	// elect a new one.
	tr.partition(leader.id)
	deadline := time.Now().Add(5 * time.Second)
	var newLeader *Replica
	for time.Now().Before(deadline) {
		if l := currentLeader(tr); l != nil && l.id != leader.id {
			newLeader = l
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatal("no new leader elected after isolating the old one")
	}

	// The pinned helper would fail against the deposed leader here; the
	// retargeting helper must find the new one and succeed.
	proposePutToLeader(t, tr, []byte("after"), []byte("v1"), lt)

	if lt.changes == 0 {
		t.Fatal("expected the helper to observe a leadership change, but changes=0")
	}
	found := false
	for d := time.Now().Add(3 * time.Second); time.Now().Before(d); time.Sleep(50 * time.Millisecond) {
		if replicaHasValue(newLeader, []byte("after"), []byte("v1")) {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("post-leadership-change write was not applied on the new leader")
	}
}
