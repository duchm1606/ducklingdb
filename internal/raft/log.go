package raft

import "errors"

var errUnavailable = errors.New("raft: log entry unavailable")

// LogStorage is the stable storage interface for the Raft log.
// Implemented by LSMLogStorage; also by memStorage in tests.
type LogStorage interface {
	InitialState() (HardState, error)
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	Term(index uint64) (uint64, error)
	Entries(lo, hi uint64) ([]Entry, error)
	AppendEntries(entries []Entry) error
	SaveHardState(hs HardState) error
}

// RaftLog manages the Raft log — a combination of stable entries (persisted
// in LogStorage) and unstable entries (in memory, awaiting persistence).
type RaftLog struct {
	storage   LogStorage
	unstable  []Entry  // unstable entries; unstable[i].Index = offset+1+i
	offset    uint64   // last persisted index
	committed uint64
	applied   uint64
}

func newRaftLog(storage LogStorage) *RaftLog {
	lastIdx, err := storage.LastIndex()
	if err != nil {
		lastIdx = 0
	}
	committed, err := storage.LastIndex()
	if err != nil {
		committed = 0
	}
	hs, err := storage.InitialState()
	if err == nil && hs.Commit > 0 && hs.Commit <= committed {
		committed = hs.Commit
	} else if err != nil {
		committed = 0
	}
	return &RaftLog{
		storage:   storage,
		unstable:  nil,
		offset:    lastIdx,
		committed: committed,
		applied:   committed,
	}
}

// lastIndex returns the index of the last entry (stable or unstable).
func (l *RaftLog) lastIndex() uint64 {
	if n := len(l.unstable); n > 0 {
		return l.unstable[n-1].Index
	}
	idx, _ := l.storage.LastIndex()
	return idx
}

// lastTerm returns the term of the last entry.
func (l *RaftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

// term returns the term of the entry at index, searching unstable first.
func (l *RaftLog) term(index uint64) (uint64, error) {
	if index == 0 {
		return 0, nil
	}
	// Check unstable entries
	if len(l.unstable) > 0 {
		first := l.unstable[0].Index
		last := l.unstable[len(l.unstable)-1].Index
		if index >= first && index <= last {
			return l.unstable[index-first].Term, nil
		}
	}
	// Fallback to stable storage
	return l.storage.Term(index)
}

// append adds unstable entries to the log. Conflicting entries are truncated.
// Returns the last index after appending.
func (l *RaftLog) append(entries ...Entry) uint64 {
	if len(entries) == 0 {
		return l.lastIndex()
	}
	// Truncate conflicting unstable entries
	first := entries[0].Index
	if len(l.unstable) > 0 && first <= l.unstable[len(l.unstable)-1].Index {
		// Truncate at the conflict point
		cutAt := int(first - l.unstable[0].Index)
		if cutAt < 0 {
			cutAt = 0
		}
		if cutAt <= len(l.unstable) {
			l.unstable = l.unstable[:cutAt]
		}
	}
	l.unstable = append(l.unstable, entries...)
	return l.lastIndex()
}

// entries returns entries in [lo, hi) — lo inclusive, hi exclusive.
func (l *RaftLog) entries(lo, hi uint64) []Entry {
	if lo >= hi {
		return nil
	}
	var result []Entry
	// From stable storage
	stableMax, _ := l.storage.LastIndex()
	if lo <= stableMax {
		cap := hi
		if cap > stableMax+1 {
			cap = stableMax + 1
		}
		se, err := l.storage.Entries(lo, cap)
		if err == nil {
			result = append(result, se...)
		}
		lo = stableMax + 1
	}
	// From unstable entries
	if lo < hi && len(l.unstable) > 0 {
		first := l.unstable[0].Index
		last := l.unstable[len(l.unstable)-1].Index
		if lo <= last && hi > first {
			startOff := int(lo - first)
			if startOff < 0 {
				startOff = 0
			}
			endOff := int(hi - first)
			if endOff > len(l.unstable) {
				endOff = len(l.unstable)
			}
			result = append(result, l.unstable[startOff:endOff]...)
		}
	}
	return result
}

// commitTo advances the committed index.
func (l *RaftLog) commitTo(index uint64) {
	if index > l.committed {
		l.committed = index
	}
}

// appliedTo advances the applied index.
func (l *RaftLog) appliedTo(index uint64) {
	if index > l.applied {
		l.applied = index
	}
}

// nextEntries returns committed entries that have not yet been applied.
func (l *RaftLog) nextEntries() []Entry {
	off := l.applied + 1
	if l.committed >= off {
		return l.entries(off, l.committed+1)
	}
	return nil
}

// unstableEntries returns entries not yet persisted (index > offset).
func (l *RaftLog) unstableEntries() []Entry {
	if len(l.unstable) == 0 {
		return nil
	}
	return l.unstable
}

// stableTo marks entries up to (index, term) as persisted.
func (l *RaftLog) stableTo(index, term uint64) {
	if len(l.unstable) == 0 {
		return
	}
	first := l.unstable[0].Index
	if index >= first {
		i := int(index - first + 1)
		if i > len(l.unstable) {
			i = len(l.unstable)
		}
		l.unstable = l.unstable[i:]
		l.offset = index
	}
}

// isUpToDate returns true if (lastIndex, lastTerm) is at least as up-to-date
// as our log — used when deciding whether to grant a vote.
func (l *RaftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	myTerm := l.lastTerm()
	myIndex := l.lastIndex()
	if lastTerm != myTerm {
		return lastTerm > myTerm
	}
	return lastIndex >= myIndex
}

// maybeCommit advances committed to maxIndex if log[maxIndex].term == term.
// Returns true if committed index advanced.
func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed {
		t, err := l.term(maxIndex)
		if err == nil && t == term {
			l.commitTo(maxIndex)
			return true
		}
	}
	return false
}
