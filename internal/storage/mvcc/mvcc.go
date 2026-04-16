package mvcc

import (
	"errors"

	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

var ErrWriteIntent = errors.New("mvcc: write intent")

// MVCC value encoding: a 1-byte tag prefix distinguishes live values from
// tombstones. The LSM engine stores raw bytes and doesn't know about MVCC,
// so we encode the distinction into the value itself.
const (
	mvccValueTagLive      byte = 0x01
	mvccValueTagTombstone byte = 0x00
)

// encodeMVCCValue wraps a user value with the live tag.
func encodeMVCCValue(value []byte) []byte {
	out := make([]byte, 1+len(value))
	out[0] = mvccValueTagLive
	copy(out[1:], value)
	return out
}

// encodeMVCCTombstone returns the single-byte tombstone marker.
func encodeMVCCTombstone() []byte {
	return []byte{mvccValueTagTombstone}
}

// decodeMVCCValue strips the tag prefix. Returns (value, isTombstone).
func decodeMVCCValue(raw []byte) ([]byte, bool) {
	if len(raw) == 0 || raw[0] == mvccValueTagTombstone {
		return nil, true
	}
	return raw[1:], false
}

// ReadOptions configures an MVCC read.
type ReadOptions struct {
	// Txn is the reading transaction's ID. nil means non-transactional.
	Txn *TxnID
}

// MVCCGet reads the newest version of key with timestamp ≤ the given timestamp.
func MVCCGet(engine storage.Engine, key []byte, timestamp hlc.Timestamp, opts ReadOptions) ([]byte, error) {
	// Step 1: Read the metadata entry — the gatekeeper.
	// If metadata doesn't exist, the key has never been written.
	metaKey := EncodeMeta(key)
	metaVal, err := engine.Get(metaKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil, storage.ErrKeyNotFound
	}
	if err != nil {
		return nil, err
	}

	meta, err := DecodeMetadata(metaVal)
	if err != nil {
		return nil, err
	}

	// Inline value: timestamp=0 reads the value directly from metadata.
	if timestamp.IsEmpty() {
		if meta.IsInline() {
			return copyBytes(meta.InlineValue), nil
		}
		return nil, storage.ErrKeyNotFound
	}

	// Step 2: Check for write intents.
	// An intent from another transaction blocks us — unless its timestamp
	// is after our read timestamp (we wouldn't see it anyway).
	if meta.HasIntent() {
		intentVisible := meta.TxnTimestamp.LessEq(timestamp)
		ownIntent := opts.Txn != nil && *opts.Txn == *meta.Txn

		if intentVisible && !ownIntent {
			return nil, ErrWriteIntent
		}
	}

	// Step 3: Seek to the newest version with timestamp ≤ our read timestamp.
	// Because timestamps are encoded descending, Seek(Encode(key, T)) lands
	// on the first version at or before T.
	iter, err := engine.NewIterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	seekKey := Encode(MVCCKey{Key: key, Timestamp: timestamp})
	if !iter.Seek(seekKey) {
		return nil, storage.ErrKeyNotFound
	}

	// Decode what we landed on and verify it's still our raw key.
	found, versioned, err := Decode(iter.Key())
	if err != nil {
		return nil, err
	}
	if !versioned || !bytesEqual(found.Key, key) {
		// We overshot to a different raw key or landed on metadata.
		return nil, storage.ErrKeyNotFound
	}

	// Decode the MVCC value — check if it's a tombstone.
	value, isTombstone := decodeMVCCValue(iter.Value())
	if isTombstone {
		return nil, storage.ErrKeyNotFound
	}

	return copyBytes(value), nil
}

// MVCCPut writes a new timestamped version of key. If txn is non-nil, the
// write is recorded as an intent belonging to that transaction.
func MVCCPut(engine storage.Engine, key []byte, timestamp hlc.Timestamp, value []byte, txn *TxnID) error {
	// Step 1: Read existing metadata (may not exist — first write to this key).
	metaKey := EncodeMeta(key)
	var meta MVCCMetadata
	if metaVal, err := engine.Get(metaKey); err == nil {
		meta, err = DecodeMetadata(metaVal)
		if err != nil {
			return err
		}
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		return err
	}

	// Inline value: timestamp=0 stores directly in metadata, no versioned entry.
	if timestamp.IsEmpty() {
		meta.InlineValue = append([]byte{}, value...)
		meta.KeyBytes = int64(len(key))
		meta.ValBytes = int64(len(value))
		metaData, err := meta.Encode()
		if err != nil {
			return err
		}
		return engine.Put(metaKey, metaData)
	}

	// Step 2: Check for conflicting intents.
	// Two different transactions cannot both hold intents on the same key.
	if meta.HasIntent() {
		ownIntent := txn != nil && *txn == *meta.Txn
		if !ownIntent {
			return ErrWriteIntent
		}
	}

	// Step 3: Write the versioned entry.
	encKey := Encode(MVCCKey{Key: key, Timestamp: timestamp})
	if err := engine.Put(encKey, encodeMVCCValue(value)); err != nil {
		return err
	}

	// Step 4: Update metadata.
	meta.Timestamp = timestamp
	meta.Deleted = false
	meta.InlineValue = nil // clear inline if previously set
	meta.KeyBytes = int64(len(key))
	meta.ValBytes = int64(len(value))
	if txn != nil {
		meta.Txn = txn
		meta.TxnTimestamp = timestamp
	} else {
		meta.Txn = nil
		meta.TxnTimestamp = hlc.Timestamp{}
	}

	metaData, err := meta.Encode()
	if err != nil {
		return err
	}
	return engine.Put(metaKey, metaData)
}

// MVCCDelete writes a tombstone version at the given timestamp.
// It is semantically identical to MVCCPut with a nil value — the same intent
// checks and metadata updates apply.
func MVCCDelete(engine storage.Engine, key []byte, timestamp hlc.Timestamp, txn *TxnID) error {
	// Step 1: Read existing metadata.
	metaKey := EncodeMeta(key)
	var meta MVCCMetadata
	if metaVal, err := engine.Get(metaKey); err == nil {
		meta, err = DecodeMetadata(metaVal)
		if err != nil {
			return err
		}
	} else if !errors.Is(err, storage.ErrKeyNotFound) {
		return err
	}

	// Step 2: Check for conflicting intents.
	if meta.HasIntent() {
		ownIntent := txn != nil && *txn == *meta.Txn
		if !ownIntent {
			return ErrWriteIntent
		}
	}

	// Step 3: Write a tombstone version.
	encKey := Encode(MVCCKey{Key: key, Timestamp: timestamp})
	if err := engine.Put(encKey, encodeMVCCTombstone()); err != nil {
		return err
	}

	// Step 4: Update metadata — mark as deleted.
	meta.Timestamp = timestamp
	meta.Deleted = true
	meta.KeyBytes = int64(len(key))
	meta.ValBytes = 0
	if txn != nil {
		meta.Txn = txn
		meta.TxnTimestamp = timestamp
	} else {
		meta.Txn = nil
		meta.TxnTimestamp = hlc.Timestamp{}
	}

	metaData, err := meta.Encode()
	if err != nil {
		return err
	}
	return engine.Put(metaKey, metaData)
}

// KeyValue is a single result from MVCCScan.
type KeyValue struct {
	Key   []byte
	Value []byte
}

// MVCCScan returns all key-value pairs in [start, end) as of the given
// timestamp. It walks metadata entries to discover which raw keys exist,
// then resolves each key's version at the read timestamp.
func MVCCScan(engine storage.Engine, start, end []byte, timestamp hlc.Timestamp, opts ReadOptions) ([]KeyValue, error) {
	iter, err := engine.NewIterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var results []KeyValue

	// Seek to the first metadata entry at or after start.
	seekKey := EncodeMeta(start)
	if !iter.Seek(seekKey) {
		return results, nil
	}

	for iter.Valid() {
		// Decode the current key. We only care about metadata entries —
		// they act as the index of which raw keys exist.
		mk, versioned, err := Decode(iter.Key())
		if err != nil {
			return nil, err
		}

		// Skip versioned entries — we'll resolve them via MVCCGet.
		if versioned {
			iter.Next()
			continue
		}

		// Metadata entry. Check if the raw key is still within [start, end).
		rawKey := mk.Key
		if end != nil && !bytesLess(rawKey, end) {
			break
		}

		// Resolve this key at the read timestamp using MVCCGet.
		// This handles intents, tombstones, and version selection.
		value, err := MVCCGet(engine, rawKey, timestamp, opts)
		if err == nil {
			results = append(results, KeyValue{
				Key:   copyBytes(rawKey),
				Value: copyBytes(value),
			})
		} else if !errors.Is(err, storage.ErrKeyNotFound) {
			// Propagate real errors (e.g., WriteIntentError).
			return nil, err
		}

		// Skip past all versions of this raw key to the next metadata entry.
		// The next raw key's metadata is at EncodeMeta(rawKey + 1 byte).
		// We use a key just past all possible encoded entries for this raw key.
		nextPrefix := nextKeyPrefix(rawKey)
		if nextPrefix == nil {
			break // rawKey is the maximum possible key
		}
		if !iter.Seek(EncodeMeta(nextPrefix)) {
			break
		}
	}

	return results, nil
}

// nextKeyPrefix returns the smallest byte slice strictly greater than key
// as a prefix. Used to skip past all MVCC entries for a given raw key.
func nextKeyPrefix(key []byte) []byte {
	// Append 0x00 — since our key encoding uses 0x00 as the separator,
	// EncodeMeta(key + 0x00) sorts after all versions of key.
	next := make([]byte, len(key)+1)
	copy(next, key)
	next[len(key)] = 0x00
	return next
}

// bytesLess reports whether a < b lexicographically.
func bytesLess(a, b []byte) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// bytesEqual is bytes.Equal without importing the bytes package.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// copyBytes returns a copy of b. Iterator values may be invalidated on the
// next call, so we must copy before returning.
func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
