package raft

import (
	"errors"
	"testing"
)

// --- LSMLogStorage snapshot/compact tests ---

func TestLSMLogStorageSnapshotRoundtrip(t *testing.T) {
	s := NewLSMLogStorage(newTestEngine(t))

	// Initially empty.
	got, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if !got.IsEmpty() {
		t.Fatalf("expected empty snapshot, got index=%d", got.Metadata.Index)
	}

	snap := Snapshot{
		Metadata: SnapshotMetadata{Index: 42, Term: 7},
		Data:     []byte("hello world"),
	}
	if err := s.SaveSnapshot(snap); err != nil {
		t.Fatal(err)
	}

	got, err = s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if got.Metadata.Index != 42 || got.Metadata.Term != 7 {
		t.Fatalf("metadata mismatch: %+v", got.Metadata)
	}
	if string(got.Data) != "hello world" {
		t.Fatalf("data mismatch: %q", got.Data)
	}
}

func TestLSMLogStorageFirstIndexAfterSnapshot(t *testing.T) {
	s := NewLSMLogStorage(newTestEngine(t))
	first, _ := s.FirstIndex()
	if first != 1 {
		t.Fatalf("empty store FirstIndex want 1, got %d", first)
	}
	if err := s.SaveSnapshot(Snapshot{Metadata: SnapshotMetadata{Index: 100, Term: 5}}); err != nil {
		t.Fatal(err)
	}
	first, _ = s.FirstIndex()
	if first != 101 {
		t.Fatalf("post-snapshot FirstIndex want 101, got %d", first)
	}
}

func TestLSMLogStorageTermAtSnapshotIndex(t *testing.T) {
	s := NewLSMLogStorage(newTestEngine(t))
	if err := s.SaveSnapshot(Snapshot{Metadata: SnapshotMetadata{Index: 100, Term: 5}}); err != nil {
		t.Fatal(err)
	}
	term, err := s.Term(100)
	if err != nil {
		t.Fatalf("Term at snapshot index: %v", err)
	}
	if term != 5 {
		t.Fatalf("Term(100) want 5, got %d", term)
	}
	// Below snapshot: ErrCompacted.
	_, err = s.Term(50)
	if !errors.Is(err, ErrCompacted) {
		t.Fatalf("Term(50) want ErrCompacted, got %v", err)
	}
}

func TestLSMLogStorageCompactDeletesLogEntries(t *testing.T) {
	s := NewLSMLogStorage(newTestEngine(t))
	entries := []Entry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
		{Term: 1, Index: 3, Data: []byte("c")},
		{Term: 2, Index: 4, Data: []byte("d")},
		{Term: 2, Index: 5, Data: []byte("e")},
	}
	if err := s.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	// Save a snapshot at 3, then compact.
	if err := s.SaveSnapshot(Snapshot{Metadata: SnapshotMetadata{Index: 3, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Compact(3); err != nil {
		t.Fatal(err)
	}
	// Entries 1..3 should be unreadable, 4..5 still there.
	for _, idx := range []uint64{1, 2, 3} {
		if ents, err := s.Entries(idx, idx+1); err == nil && len(ents) > 0 {
			t.Errorf("entry %d should be compacted, got %+v", idx, ents[0])
		}
	}
	ents, err := s.Entries(4, 6)
	if err != nil {
		t.Fatalf("post-compact Entries(4,6): %v", err)
	}
	if len(ents) != 2 || string(ents[0].Data) != "d" {
		t.Fatalf("unexpected post-compact entries: %+v", ents)
	}
}

func TestLSMLogStorageCompactIdempotent(t *testing.T) {
	s := NewLSMLogStorage(newTestEngine(t))
	s.AppendEntries([]Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}})
	s.SaveSnapshot(Snapshot{Metadata: SnapshotMetadata{Index: 2, Term: 1}})
	if err := s.Compact(2); err != nil {
		t.Fatal(err)
	}
	// Second compact: harmless.
	if err := s.Compact(2); err != nil {
		t.Fatalf("repeat compact should be no-op, got %v", err)
	}
}

func TestLSMLogStorageLastIndexAfterCompactAll(t *testing.T) {
	// When every log entry is compacted away, LastIndex should still report
	// the snapshot's anchor so the Raft layer knows where the log "ends".
	s := NewLSMLogStorage(newTestEngine(t))
	s.AppendEntries([]Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2}})
	s.SaveSnapshot(Snapshot{Metadata: SnapshotMetadata{Index: 2, Term: 1}})
	if err := s.Compact(2); err != nil {
		t.Fatal(err)
	}
	last, _ := s.LastIndex()
	if last != 2 {
		t.Fatalf("LastIndex after compact-all want 2, got %d", last)
	}
}

// --- RaftLog.restore ---

func TestRaftLogRestoreSnapshot(t *testing.T) {
	l := newRaftLog(newMemStorage())
	l.append(Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2})
	ok := l.restore(Snapshot{Metadata: SnapshotMetadata{Index: 10, Term: 3}})
	if !ok {
		t.Fatal("restore should succeed when snapshot is ahead of committed")
	}
	if l.committed != 10 || l.applied != 10 {
		t.Fatalf("post-restore committed=%d applied=%d, want both 10",
			l.committed, l.applied)
	}
	if l.lastIndex() != 10 {
		t.Fatalf("post-restore lastIndex want 10, got %d", l.lastIndex())
	}
	if !l.hasPendingSnapshot() {
		t.Fatal("pendingSnapshot should be set")
	}
	// Term at snapshot index should answer from pendingSnapshot.
	term, err := l.term(10)
	if err != nil || term != 3 {
		t.Fatalf("term(10) want 3, got term=%d err=%v", term, err)
	}
}

func TestRaftLogRestoreNoOpWhenCaughtUp(t *testing.T) {
	l := newRaftLog(newMemStorage())
	l.append(Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2})
	l.commitTo(2)
	// Snapshot at index 1 — we're already past it.
	ok := l.restore(Snapshot{Metadata: SnapshotMetadata{Index: 1, Term: 1}})
	if ok {
		t.Fatal("restore should refuse a snapshot we've already committed past")
	}
}

// --- RawNode MsgSnap paths ---

func TestRawNodeReceivesSnapshot(t *testing.T) {
	ms := newMemStorage()
	rn := NewRawNode(1, []uint64{1, 2}, ms)
	rn.term = 5

	rn.Step(Message{
		Type:     MsgSnap,
		From:     2,
		To:       1,
		Term:     5,
		Snapshot: Snapshot{Metadata: SnapshotMetadata{Index: 100, Term: 4}, Data: []byte("state")},
	})

	if rn.state != StateFollower {
		t.Errorf("want StateFollower, got %d", rn.state)
	}
	if rn.log.committed != 100 {
		t.Errorf("committed want 100, got %d", rn.log.committed)
	}
	if !rn.log.hasPendingSnapshot() {
		t.Fatal("pendingSnapshot should be set")
	}

	// Snapshot must surface via Ready.
	if !rn.HasReady() {
		t.Fatal("HasReady should be true after MsgSnap")
	}
	rd := rn.Ready()
	if rd.Snapshot.IsEmpty() {
		t.Fatal("Ready.Snapshot should be non-empty")
	}
	if rd.Snapshot.Metadata.Index != 100 {
		t.Errorf("Ready.Snapshot.Index want 100, got %d", rd.Snapshot.Metadata.Index)
	}
	rn.Advance(rd)
	if rn.log.hasPendingSnapshot() {
		t.Fatal("pendingSnapshot should be cleared after Advance")
	}
}

func TestRawNodeRejectsStaleSnapshot(t *testing.T) {
	ms := newMemStorage()
	rn := NewRawNode(1, []uint64{1, 2}, ms)
	rn.term = 10

	// Snapshot with lower term: should be rejected.
	rn.Step(Message{
		Type:     MsgSnap,
		From:     2,
		To:       1,
		Term:     5, // older
		Snapshot: Snapshot{Metadata: SnapshotMetadata{Index: 100, Term: 4}},
	})

	if rn.log.hasPendingSnapshot() {
		t.Fatal("should not accept snapshot with lower term")
	}
	// Should have sent a MsgAppResp rejection.
	rd := rn.Ready()
	rejectedFound := false
	for _, m := range rd.Messages {
		if m.Type == MsgAppResp && m.Reject {
			rejectedFound = true
		}
	}
	if !rejectedFound {
		t.Fatal("expected MsgAppResp with Reject=true for stale-term snapshot")
	}
}

// --- Leader sends MsgSnap to lagging follower ---

func TestLeaderSendsSnapshotWhenFollowerBehindFirstIndex(t *testing.T) {
	ms := newMemStorage()
	// Build a state where the leader's log starts at index 100 (everything
	// below is "compacted"). The snapshot is what the leader has to ship.
	ms.snap = Snapshot{Metadata: SnapshotMetadata{Index: 100, Term: 2}, Data: []byte("snap")}
	ms.entries = []Entry{{Term: 2, Index: 100}, {Term: 2, Index: 101}, {Term: 2, Index: 102}}

	rn := NewRawNode(1, []uint64{1, 2}, ms)
	// Force into leader state manually.
	rn.term = 2
	rn.becomeLeader()
	// becomeLeader resets nextIndex to lastIndex+1; pretend follower 2 is way behind.
	rn.nextIndex[2] = 50 // < firstIndex (101)

	rn.sendAppend(2)

	// Drain messages.
	rd := rn.Ready()
	var snapMsg *Message
	for i := range rd.Messages {
		if rd.Messages[i].Type == MsgSnap {
			snapMsg = &rd.Messages[i]
			break
		}
	}
	if snapMsg == nil {
		t.Fatal("expected MsgSnap to be sent to lagging follower")
	}
	if snapMsg.Snapshot.Metadata.Index != 100 {
		t.Errorf("snapshot index want 100, got %d", snapMsg.Snapshot.Metadata.Index)
	}
	if string(snapMsg.Snapshot.Data) != "snap" {
		t.Errorf("snapshot data mismatch: %q", snapMsg.Snapshot.Data)
	}
	// Leader should have optimistically advanced nextIndex past the snapshot.
	if rn.nextIndex[2] != 101 {
		t.Errorf("nextIndex after MsgSnap want 101, got %d", rn.nextIndex[2])
	}
}
