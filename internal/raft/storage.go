package raft

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

var (
	keyRaftHardState = []byte("\x00raft/hardstate")
	keyRaftApplied   = []byte("\x00raft/applied")
	keyRaftLogPrefix = []byte("\x00raft/log/")
)

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
	return last, nil
}

func (s *LSMLogStorage) Term(index uint64) (uint64, error) {
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
