package raft

import (
	"testing"
)

// memStorage is a minimal in-memory LogStorage for tests.
type memStorage struct {
	hs      HardState
	entries []Entry // entries[0] is a dummy sentinel at index 0
}

func newMemStorage() *memStorage {
	return &memStorage{entries: []Entry{{Term: 0, Index: 0}}}
}

func (ms *memStorage) InitialState() (HardState, error) { return ms.hs, nil }
func (ms *memStorage) FirstIndex() (uint64, error)       { return uint64(1), nil }
func (ms *memStorage) LastIndex() (uint64, error) {
	return uint64(len(ms.entries) - 1), nil
}
func (ms *memStorage) Term(index uint64) (uint64, error) {
	if index >= uint64(len(ms.entries)) {
		return 0, errUnavailable
	}
	return ms.entries[index].Term, nil
}
func (ms *memStorage) Entries(lo, hi uint64) ([]Entry, error) {
	if lo < 1 || hi > uint64(len(ms.entries)) {
		return nil, errUnavailable
	}
	return ms.entries[lo:hi], nil
}
func (ms *memStorage) AppendEntries(entries []Entry) error {
	for _, e := range entries {
		if uint64(len(ms.entries)) <= e.Index {
			ms.entries = append(ms.entries, e)
		} else {
			ms.entries[e.Index] = e
		}
	}
	return nil
}
func (ms *memStorage) SaveHardState(hs HardState) error { ms.hs = hs; return nil }

func TestRaftLogAppendAndRetrieve(t *testing.T) {
	l := newRaftLog(newMemStorage())
	e1 := Entry{Term: 1, Index: 1, Data: []byte("a")}
	e2 := Entry{Term: 1, Index: 2, Data: []byte("b")}
	l.append(e1, e2)
	if l.lastIndex() != 2 {
		t.Fatalf("want lastIndex=2, got %d", l.lastIndex())
	}
	ents := l.entries(1, 3)
	if len(ents) != 2 || string(ents[0].Data) != "a" {
		t.Fatalf("unexpected entries: %v", ents)
	}
}

func TestRaftLogTerm(t *testing.T) {
	l := newRaftLog(newMemStorage())
	l.append(Entry{Term: 2, Index: 1})
	term, err := l.term(1)
	if err != nil || term != 2 {
		t.Fatalf("want term=2, got term=%d err=%v", term, err)
	}
	_, err = l.term(99)
	if err == nil {
		t.Fatal("expected error for out-of-range index")
	}
}

func TestRaftLogCommitAndNextEntries(t *testing.T) {
	l := newRaftLog(newMemStorage())
	l.append(Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2}, Entry{Term: 1, Index: 3})
	l.commitTo(2)
	next := l.nextEntries()
	if len(next) != 2 {
		t.Fatalf("want 2 committed entries, got %d", len(next))
	}
	l.appliedTo(2)
	if len(l.nextEntries()) != 0 {
		t.Fatal("want no entries after appliedTo(2)")
	}
}

func TestRaftLogUnstableAndStableTo(t *testing.T) {
	l := newRaftLog(newMemStorage())
	l.append(Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2})
	unstable := l.unstableEntries()
	if len(unstable) != 2 {
		t.Fatalf("want 2 unstable entries, got %d", len(unstable))
	}
	l.stableTo(2, 1)
	if len(l.unstableEntries()) != 0 {
		t.Fatal("want 0 unstable entries after stableTo")
	}
}

func TestRaftLogMaybeCommit(t *testing.T) {
	l := newRaftLog(newMemStorage())
	l.append(Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2})
	committed := l.maybeCommit(2, 1)
	if !committed || l.committed != 2 {
		t.Fatalf("want committed=true and l.committed=2, got %v/%d", committed, l.committed)
	}
}

func TestRaftLogIsUpToDate(t *testing.T) {
	l := newRaftLog(newMemStorage())
	l.append(Entry{Term: 2, Index: 1}, Entry{Term: 2, Index: 2})
	// Same term, same index: up to date
	if !l.isUpToDate(2, 2) {
		t.Fatal("same term+index should be up to date")
	}
	// Higher term: up to date
	if !l.isUpToDate(1, 3) {
		t.Fatal("higher term should be up to date")
	}
	// Same term, lower index: not up to date
	if l.isUpToDate(1, 2) {
		t.Fatal("same term lower index should NOT be up to date")
	}
}

func TestNewRaftLogFromPersistedState(t *testing.T) {
	ms := newMemStorage()
	ms.hs = HardState{Term: 1, VotedFor: 1, Commit: 2}
	ms.entries = append(ms.entries,
		Entry{Term: 1, Index: 1},
		Entry{Term: 1, Index: 2},
		Entry{Term: 1, Index: 3},
	)
	l := newRaftLog(ms)
	if l.committed != 2 {
		t.Fatalf("want committed=2 from HardState, got %d", l.committed)
	}
}

func TestRaftLogStableToTermCheck(t *testing.T) {
	l := newRaftLog(newMemStorage())
	l.append(Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2})
	// Wrong term: should be a no-op
	l.stableTo(2, 99)
	if len(l.unstableEntries()) != 2 {
		t.Fatal("stableTo with wrong term should not remove entries")
	}
	// Correct term: should remove entries
	l.stableTo(2, 1)
	if len(l.unstableEntries()) != 0 {
		t.Fatal("stableTo with correct term should remove all entries")
	}
}

func TestRaftLogMaybeCommitTermMismatch(t *testing.T) {
	l := newRaftLog(newMemStorage())
	l.append(Entry{Term: 1, Index: 1}, Entry{Term: 1, Index: 2})
	committed := l.maybeCommit(2, 99) // wrong term
	if committed {
		t.Fatal("maybeCommit with wrong term should return false")
	}
	if l.committed != 0 {
		t.Fatalf("committed should remain 0, got %d", l.committed)
	}
}
