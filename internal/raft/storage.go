package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

var (
	keyRaftHardState    = []byte("\x00raft/hardstate")
	keyRaftApplied      = []byte("\x00raft/applied")
	keyRaftLogPrefix    = []byte("\x00raft/log/")
	keyRaftSnapshotMeta = []byte("\x00raft/snapshot/meta")
	keyRaftSnapshotData = []byte("\x00raft/snapshot/data")
)

// snapshotMetaJSON is the persisted form of SnapshotMetadata.
// We split snapshot metadata from its (potentially large) data blob so
// FirstIndex/Term can be answered cheaply without loading the full snapshot.
type snapshotMetaJSON struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
}

var _ LogStorage = (*LSMLogStorage)(nil)

func raftLogKey(index uint64) []byte {
	key := make([]byte, len(keyRaftLogPrefix)+8)
	copy(key, keyRaftLogPrefix)
	binary.BigEndian.PutUint64(key[len(keyRaftLogPrefix):], index)
	return key
}

// LSMLogStorage persists Raft log entries and HardState in the LSM engine.
type LSMLogStorage struct {
	eng storage.Engine
}

// NewLSMLogStorage creates a LogStorage backed by the given engine.
func NewLSMLogStorage(eng storage.Engine) *LSMLogStorage {
	return &LSMLogStorage{eng: eng}
}

func (s *LSMLogStorage) InitialState() (HardState, error) {
	data, err := s.eng.Get(keyRaftHardState)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return HardState{}, nil
		}
		return HardState{}, fmt.Errorf("raft: read HardState: %w", err)
	}
	var hs HardState
	if err := json.Unmarshal(data, &hs); err != nil {
		return HardState{}, fmt.Errorf("raft: unmarshal HardState: %w", err)
	}
	return hs, nil
}

func (s *LSMLogStorage) SaveHardState(hs HardState) error {
	data, err := json.Marshal(hs)
	if err != nil {
		return err
	}
	return s.eng.Put(keyRaftHardState, data)
}

func (s *LSMLogStorage) FirstIndex() (uint64, error) {
	// After a snapshot at index N, log indices 1..N are compacted away.
	// FirstIndex is the smallest index still independently readable from the log;
	// the snapshot covers everything below it.
	snap, err := s.loadSnapshotMeta()
	if err != nil {
		return 0, err
	}
	if snap.Index > 0 {
		return snap.Index + 1, nil
	}
	return 1, nil
}

func (s *LSMLogStorage) LastIndex() (uint64, error) {
	iter, err := s.eng.NewIterator()
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	var last uint64
	for iter.Seek(keyRaftLogPrefix); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) < len(keyRaftLogPrefix)+8 || string(k[:len(keyRaftLogPrefix)]) != string(keyRaftLogPrefix) {
			break
		}
		idx := binary.BigEndian.Uint64(k[len(keyRaftLogPrefix):])
		if idx > last {
			last = idx
		}
	}
	// If the log has been fully compacted past, the snapshot's index is the
	// effective last index — callers asking "where does the log end" should
	// see the snapshot anchor, not zero.
	snap, err := s.loadSnapshotMeta()
	if err != nil {
		return 0, err
	}
	if snap.Index > last {
		last = snap.Index
	}
	return last, nil
}

func (s *LSMLogStorage) Term(index uint64) (uint64, error) {
	// The snapshot's anchor index has its term recorded separately — the
	// underlying log entry has been compacted away.
	snap, err := s.loadSnapshotMeta()
	if err != nil {
		return 0, err
	}
	if snap.Index > 0 && index == snap.Index {
		return snap.Term, nil
	}
	if snap.Index > 0 && index < snap.Index {
		return 0, ErrCompacted
	}
	data, err := s.eng.Get(raftLogKey(index))
	if err != nil {
		return 0, errUnavailable
	}
	var e Entry
	if err := json.Unmarshal(data, &e); err != nil {
		return 0, err
	}
	return e.Term, nil
}

func (s *LSMLogStorage) Entries(lo, hi uint64) ([]Entry, error) {
	result := make([]Entry, 0, hi-lo)
	for i := lo; i < hi; i++ {
		data, err := s.eng.Get(raftLogKey(i))
		if err != nil {
			return nil, fmt.Errorf("raft: entry %d not found: %w", i, err)
		}
		var e Entry
		if err := json.Unmarshal(data, &e); err != nil {
			return nil, err
		}
		result = append(result, e)
	}
	return result, nil
}

func (s *LSMLogStorage) SaveApplied(index uint64) error {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, index)
	return s.eng.Put(keyRaftApplied, buf)
}

func (s *LSMLogStorage) LoadApplied() (uint64, error) {
	data, err := s.eng.Get(keyRaftApplied)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}
	if len(data) != 8 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(data), nil
}

func (s *LSMLogStorage) AppendEntries(entries []Entry) error {
	for _, e := range entries {
		data, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if err := s.eng.Put(raftLogKey(e.Index), data); err != nil {
			return err
		}
	}
	return nil
}

// loadSnapshotMeta reads the persisted snapshot metadata. Returns the zero
// SnapshotMetadata (Index=0) if no snapshot exists.
func (s *LSMLogStorage) loadSnapshotMeta() (SnapshotMetadata, error) {
	data, err := s.eng.Get(keyRaftSnapshotMeta)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return SnapshotMetadata{}, nil
		}
		return SnapshotMetadata{}, fmt.Errorf("raft: read snapshot meta: %w", err)
	}
	var m snapshotMetaJSON
	if err := json.Unmarshal(data, &m); err != nil {
		return SnapshotMetadata{}, fmt.Errorf("raft: unmarshal snapshot meta: %w", err)
	}
	return SnapshotMetadata{Index: m.Index, Term: m.Term}, nil
}

// Snapshot returns the most recent persisted snapshot, or the zero Snapshot.
func (s *LSMLogStorage) Snapshot() (Snapshot, error) {
	meta, err := s.loadSnapshotMeta()
	if err != nil {
		return Snapshot{}, err
	}
	if meta.Index == 0 {
		return Snapshot{}, nil
	}
	data, err := s.eng.Get(keyRaftSnapshotData)
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			// Metadata exists but data is missing — treat as no snapshot.
			// In a healthy system this should not happen; defensive.
			return Snapshot{}, nil
		}
		return Snapshot{}, fmt.Errorf("raft: read snapshot data: %w", err)
	}
	return Snapshot{Metadata: meta, Data: data}, nil
}

// SaveSnapshot persists snap atomically from the caller's perspective.
//
// Write order: data first, then metadata. The metadata key is the
// "snapshot exists" signal — a crash between the two writes leaves the
// data orphaned (harmless; will be overwritten on the next save) but no
// reader sees a metadata entry pointing at missing data.
func (s *LSMLogStorage) SaveSnapshot(snap Snapshot) error {
	if snap.IsEmpty() {
		return fmt.Errorf("raft: cannot save empty snapshot")
	}
	if err := s.eng.Put(keyRaftSnapshotData, snap.Data); err != nil {
		return fmt.Errorf("raft: write snapshot data: %w", err)
	}
	metaBytes, err := json.Marshal(snapshotMetaJSON{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	})
	if err != nil {
		return err
	}
	if err := s.eng.Put(keyRaftSnapshotMeta, metaBytes); err != nil {
		return fmt.Errorf("raft: write snapshot meta: %w", err)
	}
	return nil
}

// Compact deletes log entries with index ≤ compactIndex. Idempotent; safe to
// call with an index that's already been compacted past.
//
// Walks the on-disk log keys via the iterator rather than blind-deleting
// 1..compactIndex so a re-run after partial completion only revisits live
// entries.
func (s *LSMLogStorage) Compact(compactIndex uint64) error {
	if compactIndex == 0 {
		return nil
	}
	iter, err := s.eng.NewIterator()
	if err != nil {
		return err
	}
	defer iter.Close()

	var toDelete [][]byte
	for iter.Seek(keyRaftLogPrefix); iter.Valid(); iter.Next() {
		k := iter.Key()
		if len(k) < len(keyRaftLogPrefix)+8 ||
			string(k[:len(keyRaftLogPrefix)]) != string(keyRaftLogPrefix) {
			break
		}
		idx := binary.BigEndian.Uint64(k[len(keyRaftLogPrefix):])
		if idx > compactIndex {
			break
		}
		// Copy the key — the iterator may reuse the slice on Next.
		kc := make([]byte, len(k))
		copy(kc, k)
		toDelete = append(toDelete, kc)
	}
	for _, k := range toDelete {
		if err := s.eng.Delete(k); err != nil {
			return fmt.Errorf("raft: compact delete %x: %w", k, err)
		}
	}
	return nil
}
