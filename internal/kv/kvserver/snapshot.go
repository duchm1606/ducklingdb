package kvserver

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

// raftPrefix is the namespace this Replica owns for its Raft state
// (HardState, log entries, applied index, snapshot). Snapshot creation
// excludes these keys — they are per-replica, not part of the state machine.
var raftPrefix = []byte("\x00raft/")

// serializeEngineState walks the engine and writes every non-Raft key-value
// pair into a length-prefixed binary blob.
//
// Format:
//
//	[8 bytes: numEntries uint64 little-endian]
//	for each entry:
//	  [4 bytes: keyLen uint32 LE]
//	  [4 bytes: valLen uint32 LE]
//	  [keyLen bytes: key]
//	  [valLen bytes: value]
//
// Reading this back via deserializeEngineState gives an exact set of
// (key, value) pairs the apply path can write into a fresh engine.
//
// Note: this serializes the full current state. For large ranges, production
// systems stream snapshots in chunks; DucklingDB's M4 keeps everything in
// memory for simplicity. Range size in M5+ will be bounded by the 64 MB
// split threshold, keeping snapshots tractable.
func serializeEngineState(eng storage.Engine) ([]byte, error) {
	iter, err := eng.NewIterator()
	if err != nil {
		return nil, fmt.Errorf("snapshot: new iterator: %w", err)
	}
	defer iter.Close()

	var buf bytes.Buffer
	// Reserve space for the header; we'll back-fill the count once known.
	if _, err := buf.Write(make([]byte, 8)); err != nil {
		return nil, err
	}

	var count uint64
	scratch := make([]byte, 8)
	for ok := iter.Seek([]byte{}); ok; ok = iter.Next() {
		k := iter.Key()
		if bytes.HasPrefix(k, raftPrefix) {
			// Skip Raft-private state; it's per-replica and not part of the
			// state machine snapshot. The receiver maintains its own Raft state.
			continue
		}
		// Iterator-level tombstones are not surfaced as Keys here (the engine's
		// Get/Iterator already filters them at the layer where we live), but be
		// defensive — IsTombstone tells us whether the current entry is a
		// deletion marker that should be excluded.
		if iter.IsTombstone() {
			continue
		}
		v := iter.Value()
		binary.LittleEndian.PutUint32(scratch[0:4], uint32(len(k)))
		binary.LittleEndian.PutUint32(scratch[4:8], uint32(len(v)))
		if _, err := buf.Write(scratch); err != nil {
			return nil, err
		}
		if _, err := buf.Write(k); err != nil {
			return nil, err
		}
		if _, err := buf.Write(v); err != nil {
			return nil, err
		}
		count++
	}
	// Back-fill the count.
	out := buf.Bytes()
	binary.LittleEndian.PutUint64(out[0:8], count)
	return out, nil
}

// applyEngineSnapshot writes the (key, value) pairs encoded in data into eng.
// It does not clear the engine first — callers are expected to do that, or to
// accept that pre-existing keys not present in the snapshot remain. For the
// Raft snapshot install path the caller wipes user-data keys first.
func applyEngineSnapshot(eng storage.Engine, data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("snapshot: data too short (%d bytes)", len(data))
	}
	r := bytes.NewReader(data)
	header := make([]byte, 8)
	if _, err := io.ReadFull(r, header); err != nil {
		return fmt.Errorf("snapshot: read header: %w", err)
	}
	count := binary.LittleEndian.Uint64(header)

	scratch := make([]byte, 8)
	for i := uint64(0); i < count; i++ {
		if _, err := io.ReadFull(r, scratch); err != nil {
			return fmt.Errorf("snapshot: read entry %d header: %w", i, err)
		}
		klen := binary.LittleEndian.Uint32(scratch[0:4])
		vlen := binary.LittleEndian.Uint32(scratch[4:8])
		k := make([]byte, klen)
		if _, err := io.ReadFull(r, k); err != nil {
			return fmt.Errorf("snapshot: read entry %d key: %w", i, err)
		}
		v := make([]byte, vlen)
		if _, err := io.ReadFull(r, v); err != nil {
			return fmt.Errorf("snapshot: read entry %d value: %w", i, err)
		}
		if err := eng.Put(k, v); err != nil {
			return fmt.Errorf("snapshot: put entry %d: %w", i, err)
		}
	}
	return nil
}

// clearStateMachine deletes every non-Raft key from eng. Used before
// installing an incoming snapshot so the resulting state exactly matches
// the leader's at the snapshot's anchor index.
func clearStateMachine(eng storage.Engine) error {
	iter, err := eng.NewIterator()
	if err != nil {
		return err
	}
	defer iter.Close()

	// Collect keys first; deleting while iterating is not supported by the
	// LSM iterator's snapshot semantics.
	var toDelete [][]byte
	for ok := iter.Seek([]byte{}); ok; ok = iter.Next() {
		k := iter.Key()
		if bytes.HasPrefix(k, raftPrefix) {
			continue
		}
		kc := make([]byte, len(k))
		copy(kc, k)
		toDelete = append(toDelete, kc)
	}
	for _, k := range toDelete {
		if err := eng.Delete(k); err != nil {
			return fmt.Errorf("snapshot: clear key %x: %w", k, err)
		}
	}
	return nil
}
