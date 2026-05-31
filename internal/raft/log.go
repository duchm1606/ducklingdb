package raft

import (
	"errors"
	"fmt"
)

var (
	errUnavailable = errors.New("raft: log entry unavailable")
	// ErrCompacted is returned when a caller requests log data that has been
	// compacted away by a snapshot. The caller should fall back to a snapshot
	// transfer.
	ErrCompacted = errors.New("raft: log compacted")
)

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
	// SaveApplied durably records the last log index applied to the state machine.
	// Called after every apply batch so crash recovery can replay missing entries.
	SaveApplied(index uint64) error
	// LoadApplied returns the last durably saved applied index (0 if never saved).
	LoadApplied() (uint64, error)
	// Snapshot returns the most recent persisted snapshot, or the zero
	// Snapshot if none exists.
	Snapshot() (Snapshot, error)
	// SaveSnapshot persists snap. After this returns, FirstIndex must report
	// snap.Metadata.Index + 1 and Term(snap.Metadata.Index) must return
	// snap.Metadata.Term. Existing log entries are not deleted; callers
	// follow up with Compact to reclaim space.
	SaveSnapshot(snap Snapshot) error
	// Compact removes log entries with index ≤ compactIndex. Idempotent.
	// Must only be called after a snapshot at compactIndex (or later) has
	// been saved, otherwise the log would lose entries the state machine
	// has not yet captured.
	Compact(compactIndex uint64) error
}

// RaftLog manages the Raft log — a combination of stable entries (persisted
// in LogStorage) and unstable entries (in memory, awaiting persistence).
//
// pendingSnapshot is non-empty when a leader has sent us a snapshot via
// MsgSnap. The caller drains it via Ready() and applies it to the state
// machine; Advance() clears it.
type RaftLog struct {
	storage         LogStorage
	unstable        []Entry // entries not yet persisted to LogStorage
	offset          uint64  // last persisted index
	committed       uint64
	applied         uint64
	pendingSnapshot Snapshot
}

func newRaftLog(storage LogStorage) *RaftLog {
	lastIdx, err := storage.LastIndex()
	if err != nil {
		lastIdx = 0
	}
	firstIdx, err := storage.FirstIndex()
	if err != nil {
		firstIdx = 1
	}
	// Floor for committed/applied: a node restarting after a snapshot has
	// implicitly committed and applied everything up through the snapshot's
	// anchor index. HardState's Commit may bump us higher.
	floor := firstIdx - 1
	committed := floor
	hs, hsErr := storage.InitialState()
	if hsErr == nil && hs.Commit > committed && hs.Commit <= lastIdx {
		committed = hs.Commit
	}
	return &RaftLog{
		storage:   storage,
		unstable:  nil,
		offset:    lastIdx,
		committed: committed,
		applied:   floor,
	}
}

// firstIndex returns the smallest log index still independently readable.
// Indices below this are covered by a snapshot.
func (l *RaftLog) firstIndex() uint64 {
	idx, _ := l.storage.FirstIndex()
	return idx
}

// hasPendingSnapshot reports whether a snapshot is waiting to be applied.
func (l *RaftLog) hasPendingSnapshot() bool {
	return !l.pendingSnapshot.IsEmpty()
}

// restore prepares the log to receive a snapshot. The caller (RawNode)
// invokes this when MsgSnap arrives. The snapshot becomes pendingSnapshot,
// committed/applied bump to the snapshot's index, and any unstable entries
// at or below the snapshot index are dropped.
//
// Returns false if the snapshot does not advance us — we already have an
// entry at or past snap.Index whose term matches snap.Term. In that case
// the leader's snapshot is redundant; we can keep replicating via the log.
func (l *RaftLog) restore(snap Snapshot) bool {
	if snap.Metadata.Index <= l.committed {
		// We've already committed past this snapshot.
		return false
	}
	l.pendingSnapshot = snap
	l.committed = snap.Metadata.Index
	if l.applied < snap.Metadata.Index {
		l.applied = snap.Metadata.Index
	}
	// Drop any unstable entries at or below the snapshot — the snapshot
	// supersedes them. Anything strictly above stays valid.
	if len(l.unstable) > 0 {
		i := 0
		for i < len(l.unstable) && l.unstable[i].Index <= snap.Metadata.Index {
			i++
		}
		l.unstable = l.unstable[i:]
	}
	if l.offset < snap.Metadata.Index {
		l.offset = snap.Metadata.Index
	}
	return true
}

// lastIndex returns the index of the last entry (stable, unstable, or covered
// by a pending snapshot).
func (l *RaftLog) lastIndex() uint64 {
	if n := len(l.unstable); n > 0 {
		return l.unstable[n-1].Index
	}
	idx, _ := l.storage.LastIndex()
	if l.pendingSnapshot.Metadata.Index > idx {
		return l.pendingSnapshot.Metadata.Index
	}
	return idx
}

// lastTerm returns the term of the last entry.
func (l *RaftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

// term returns the term of the entry at index, searching unstable first, then
// pendingSnapshot, then stable storage.
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
	// The pending snapshot answers for its anchor index — its underlying
	// log entry may have been compacted away on the leader's side.
	if !l.pendingSnapshot.IsEmpty() && index == l.pendingSnapshot.Metadata.Index {
		return l.pendingSnapshot.Metadata.Term, nil
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
		hiStable := hi
		if hiStable > stableMax+1 {
			hiStable = stableMax + 1
		}
		se, err := l.storage.Entries(lo, hiStable)
		if err != nil {
			return nil
		}
		result = append(result, se...)
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
	if index > l.committed {
		panic(fmt.Sprintf("raft: appliedTo(%d) > committed(%d)", index, l.committed))
	}
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
	if index < first {
		return
	}
	if l.unstable[index-first].Term != term {
		return
	}
	i := int(index - first + 1)
	if i > len(l.unstable) {
		i = len(l.unstable)
	}
	l.unstable = l.unstable[i:]
	l.offset = index
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
