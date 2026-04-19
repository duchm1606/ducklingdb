package mvcc

import (
	"errors"
	"fmt"
	"sync"

	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// mvccMu serializes read-modify-write sequences (MVCCPut, MVCCDelete,
// MVCCPushIntent, MVCCResolveWriteIntent). Each of these operations does
// multiple engine reads and writes whose correctness depends on no other
// MVCC-level mutation racing between them.
//
// This is a coarse single-node lock — production systems would use per-key
// striped locks or MVCC-level version/epoch-based optimistic concurrency.
// For M2's educational scope, correctness under concurrency is the
// priority; throughput optimizations can come later (M4+).
var mvccMu sync.Mutex

// ErrWriteIntent is the sentinel for write-intent conflicts. Callers that
// just need to detect "is this a write-intent error?" use errors.Is.
// Callers that need the conflict details (blocker's TxnID, intent timestamp,
// the key) use errors.As with *WriteIntentError.
var ErrWriteIntent = errors.New("mvcc: write intent")

// WriteIntentError describes a write-intent conflict. It identifies the
// blocking transaction so the conflict-resolution layer can look up the
// blocker's record and decide whether to push, abort, or retry.
//
// WriteIntentError wraps ErrWriteIntent, so legacy errors.Is(err, ErrWriteIntent)
// checks continue to match.
type WriteIntentError struct {
	Key       []byte
	TxnID     TxnID
	Timestamp hlc.Timestamp
}

// Error implements the error interface.
func (e *WriteIntentError) Error() string {
	return fmt.Sprintf("mvcc: write intent on %q by txn %x at %s", e.Key, e.TxnID[:4], e.Timestamp)
}

// Is reports that WriteIntentError matches the ErrWriteIntent sentinel.
func (e *WriteIntentError) Is(target error) bool {
	return target == ErrWriteIntent
}

// newWriteIntentError constructs a WriteIntentError from the metadata of the
// blocking key. Called from the three sites in mvcc.go that used to return
// ErrWriteIntent directly.
func newWriteIntentError(key []byte, meta MVCCMetadata) *WriteIntentError {
	err := &WriteIntentError{
		Key:       append([]byte(nil), key...),
		Timestamp: meta.TxnTimestamp,
	}
	if meta.Txn != nil {
		err.TxnID = *meta.Txn
	}
	return err
}

// ErrWriteTooOld is the sentinel for write-timestamp-too-old conflicts.
// Callers use errors.Is for detection and errors.As to recover the
// existing committed timestamp they need to push past.
var ErrWriteTooOld = errors.New("mvcc: write timestamp too old")

// WriteTooOldError is returned by MVCCPut / MVCCDelete when the requested
// write timestamp is at or below an already-committed version's timestamp.
// Inserting a new version below an existing committed one would break
// serializability: a reader scanning at an intermediate timestamp would see
// our "older" write as if it were the newest committed value.
//
// The resolution is to push the write timestamp past ExistingTimestamp and
// retry. The coordinator (Step 5) handles that loop.
type WriteTooOldError struct {
	Key               []byte
	RequestedTimestamp hlc.Timestamp
	ExistingTimestamp  hlc.Timestamp
}

// Error implements the error interface.
func (e *WriteTooOldError) Error() string {
	return fmt.Sprintf("mvcc: write too old on %q: requested %s, existing %s",
		e.Key, e.RequestedTimestamp, e.ExistingTimestamp)
}

// Is reports that WriteTooOldError matches the ErrWriteTooOld sentinel.
func (e *WriteTooOldError) Is(target error) bool {
	return target == ErrWriteTooOld
}

func newWriteTooOldError(key []byte, requested, existing hlc.Timestamp) *WriteTooOldError {
	return &WriteTooOldError{
		Key:                append([]byte(nil), key...),
		RequestedTimestamp: requested,
		ExistingTimestamp:  existing,
	}
}

// ErrReadUncertainty is the sentinel for uncertainty-window conflicts.
//
// When the reader's ReadTimestamp is ambiguous relative to another node's
// clock (within maxOffset), a committed value in the window
// (ReadTimestamp, ReadTimestamp + maxOffset] might have happened before we
// started, even though it looks later by timestamp. Rather than return a
// potentially-wrong snapshot, we force the transaction to restart past the
// uncertain value.
//
// On a single node with zero skew, this error never fires — the check is
// here so distributed scenarios (M4) plug in without additional changes.
var ErrReadUncertainty = errors.New("mvcc: read uncertainty")

// UncertaintyError identifies a version within the reader's uncertainty
// window. ExistingTimestamp is the witnessed timestamp; restart paths bump
// ReadTimestamp past this value.
type UncertaintyError struct {
	Key               []byte
	ReadTimestamp     hlc.Timestamp
	MaxTimestamp      hlc.Timestamp
	ExistingTimestamp hlc.Timestamp
}

// Error implements error.
func (e *UncertaintyError) Error() string {
	return fmt.Sprintf("mvcc: uncertainty on %q: read at %s with max %s, existing %s",
		e.Key, e.ReadTimestamp, e.MaxTimestamp, e.ExistingTimestamp)
}

// Is reports sentinel equivalence for errors.Is.
func (e *UncertaintyError) Is(target error) bool {
	return target == ErrReadUncertainty
}

func newUncertaintyError(key []byte, readTS, maxTS, existingTS hlc.Timestamp) *UncertaintyError {
	return &UncertaintyError{
		Key:               append([]byte(nil), key...),
		ReadTimestamp:     readTS,
		MaxTimestamp:      maxTS,
		ExistingTimestamp: existingTS,
	}
}

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
	// MaxTimestamp is the upper bound of the reader's uncertainty window.
	// A committed or foreign-intent entry with timestamp in
	// (ReadTimestamp, MaxTimestamp] is treated as "happened before but we
	// cannot tell" and surfaces an UncertaintyError. Zero disables the
	// check (single-node or non-transactional reads).
	MaxTimestamp hlc.Timestamp
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
			return nil, newWriteIntentError(key, meta)
		}
	}

	// Step 2b: Uncertainty-window check.
	// If the caller supplied a MaxTimestamp and the newest version (committed
	// or foreign intent) lands in (ReadTimestamp, MaxTimestamp], surface an
	// UncertaintyError so the caller restarts past the uncertain write.
	//
	// Our own intent is excluded — we always see our own writes regardless
	// of timestamp, and restarting because of ourselves would loop forever.
	if !opts.MaxTimestamp.IsEmpty() &&
		timestamp.Less(meta.Timestamp) &&
		meta.Timestamp.LessEq(opts.MaxTimestamp) {
		ownIntent := meta.HasIntent() && opts.Txn != nil && *opts.Txn == *meta.Txn
		if !ownIntent {
			return nil, newUncertaintyError(key, timestamp, opts.MaxTimestamp, meta.Timestamp)
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
	mvccMu.Lock()
	defer mvccMu.Unlock()
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

	// Step 2a: Check for conflicting intents.
	// Two different transactions cannot both hold intents on the same key.
	if meta.HasIntent() {
		ownIntent := txn != nil && *txn == *meta.Txn
		if !ownIntent {
			return newWriteIntentError(key, meta)
		}
	} else {
		// Step 2b: Check for a committed version at or after our timestamp.
		// Writing a new version below a committed one would break
		// serializability. The caller must push forward and retry.
		//
		// Guarded on "no intent" so that writing our own intent a second
		// time at the same timestamp (e.g., coordinator retrying) is allowed.
		if !meta.Timestamp.IsEmpty() && timestamp.LessEq(meta.Timestamp) {
			return newWriteTooOldError(key, timestamp, meta.Timestamp)
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
	mvccMu.Lock()
	defer mvccMu.Unlock()
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

	// Step 2a: Check for conflicting intents.
	if meta.HasIntent() {
		ownIntent := txn != nil && *txn == *meta.Txn
		if !ownIntent {
			return newWriteIntentError(key, meta)
		}
	} else {
		// Step 2b: Same write-too-old protection as MVCCPut.
		if !meta.Timestamp.IsEmpty() && timestamp.LessEq(meta.Timestamp) {
			return newWriteTooOldError(key, timestamp, meta.Timestamp)
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
