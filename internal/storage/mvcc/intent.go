package mvcc

import (
	"errors"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// TxnStatus represents the outcome of a transaction.
type TxnStatus int

const (
	TxnCommitted TxnStatus = iota
	TxnAborted
)

// MVCCResolveWriteIntent resolves an intent left by a transaction on key.
//
// On commit: the intent becomes a regular committed version. If commitTimestamp
// differs from the intent's original timestamp, the version is rewritten at
// the new timestamp and the old entry is deleted.
//
// On abort: the intent's version is deleted and metadata is reverted to the
// previous committed version (or removed entirely if no prior version exists).
func MVCCResolveWriteIntent(
	engine storage.Engine,
	key []byte,
	txnID TxnID,
	status TxnStatus,
	commitTimestamp hlc.Timestamp,
) error {
	// Step 1: Read and validate metadata.
	metaKey := EncodeMeta(key)
	metaVal, err := engine.Get(metaKey)
	if errors.Is(err, storage.ErrKeyNotFound) {
		return nil // no metadata → nothing to resolve
	}
	if err != nil {
		return err
	}

	meta, err := DecodeMetadata(metaVal)
	if err != nil {
		return err
	}

	// The intent must belong to this transaction.
	if !meta.HasIntent() || *meta.Txn != txnID {
		return nil // not our intent → nothing to do
	}

	intentTimestamp := meta.TxnTimestamp

	switch status {
	case TxnCommitted:
		return resolveCommit(engine, key, metaKey, meta, intentTimestamp, commitTimestamp)
	case TxnAborted:
		return resolveAbort(engine, key, metaKey, intentTimestamp)
	default:
		return fmt.Errorf("mvcc: unknown txn status %d", status)
	}
}

// resolveCommit makes the intent a permanent committed version.
// If commitTimestamp != intentTimestamp, the version is moved to the new slot.
func resolveCommit(
	engine storage.Engine,
	key, metaKey []byte,
	meta MVCCMetadata,
	intentTimestamp, commitTimestamp hlc.Timestamp,
) error {
	// If the commit timestamp was pushed, rewrite the version at the new
	// timestamp and delete the old entry.
	if !intentTimestamp.Equal(commitTimestamp) {
		oldKey := Encode(MVCCKey{Key: key, Timestamp: intentTimestamp})
		oldVal, err := engine.Get(oldKey)
		if err != nil {
			return fmt.Errorf("read intent value at %v: %w", intentTimestamp, err)
		}

		// Write at the new timestamp.
		newKey := Encode(MVCCKey{Key: key, Timestamp: commitTimestamp})
		if err := engine.Put(newKey, oldVal); err != nil {
			return err
		}

		// Delete the old slot. The LSM engine's Delete writes a tombstone
		// at the old key, which will be cleaned up during compaction.
		if err := engine.Delete(oldKey); err != nil {
			return err
		}
	}

	// Clear the intent from metadata, update timestamp to commit timestamp.
	meta.Txn = nil
	meta.TxnTimestamp = hlc.Timestamp{}
	meta.Timestamp = commitTimestamp

	metaData, err := meta.Encode()
	if err != nil {
		return err
	}
	return engine.Put(metaKey, metaData)
}

// resolveAbort removes the intent and reverts metadata to the previous
// committed version. If no prior version exists, metadata is deleted.
func resolveAbort(
	engine storage.Engine,
	key, metaKey []byte,
	intentTimestamp hlc.Timestamp,
) error {
	// Find the previous committed version BEFORE deleting the intent.
	// After deletion the iterator's DeletedFilterIterator would skip the
	// tombstone and Seek+Next would overshoot.
	prevTimestamp, prevExists, prevIsTombstone, err := findPreviousVersion(engine, key, intentTimestamp)
	if err != nil {
		return err
	}

	// Now delete the intent's versioned entry.
	intentKey := Encode(MVCCKey{Key: key, Timestamp: intentTimestamp})
	if err := engine.Delete(intentKey); err != nil {
		return err
	}

	if !prevExists {
		// No prior version — delete metadata entirely. The key goes back
		// to "never written."
		return engine.Delete(metaKey)
	}

	// Revert metadata to the previous version's state.
	meta := MVCCMetadata{
		Timestamp: prevTimestamp,
		Deleted:   prevIsTombstone,
		KeyBytes:  int64(len(key)),
	}

	metaData, err := meta.Encode()
	if err != nil {
		return err
	}
	return engine.Put(metaKey, metaData)
}

// findPreviousVersion scans for the version immediately older than afterTimestamp.
// Returns (timestamp, exists, isTombstone, error).
func findPreviousVersion(
	engine storage.Engine,
	key []byte,
	afterTimestamp hlc.Timestamp,
) (hlc.Timestamp, bool, bool, error) {
	iter, err := engine.NewIterator()
	if err != nil {
		return hlc.Timestamp{}, false, false, err
	}
	defer iter.Close()

	// Seek to the intent's position, then advance past it.
	// The next versioned entry for the same raw key is the previous version
	// (timestamps are descending, so the next entry in iteration order is older).
	seekKey := Encode(MVCCKey{Key: key, Timestamp: afterTimestamp})
	if !iter.Seek(seekKey) {
		return hlc.Timestamp{}, false, false, nil
	}

	// The seek may land on the intent itself — advance past it.
	if !iter.Next() {
		return hlc.Timestamp{}, false, false, nil
	}

	// Decode and verify it's still the same raw key.
	found, versioned, err := Decode(iter.Key())
	if err != nil || !versioned || !bytesEqual(found.Key, key) {
		return hlc.Timestamp{}, false, false, err
	}

	_, isTombstone := decodeMVCCValue(iter.Value())
	return found.Timestamp, true, isTombstone, nil
}
