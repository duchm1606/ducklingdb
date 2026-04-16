package mvcc

import (
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// MVCCGarbageCollect removes old MVCC versions that are no longer needed.
//
// For each raw key, it applies these rules:
//  1. Keep all versions with timestamp > gcThreshold.
//  2. Keep the newest version even if it's ≤ gcThreshold (it's the current state).
//  3. Drop all other versions ≤ gcThreshold.
//  4. If the newest version is a tombstone AND it's ≤ gcThreshold, drop the
//     tombstone too and delete the metadata — the key is fully dead.
//
// This is a stop-the-world scan. For M1 this is acceptable; production systems
// run GC incrementally during compaction.
func MVCCGarbageCollect(engine storage.Engine, gcThreshold hlc.Timestamp) error {
	iter, err := engine.NewIterator()
	if err != nil {
		return err
	}
	defer iter.Close()

	if !iter.Seek([]byte{}) {
		return nil // empty engine
	}

	for iter.Valid() {
		// We expect to land on a metadata entry.
		mk, versioned, err := Decode(iter.Key())
		if err != nil {
			// Skip entries we can't decode.
			iter.Next()
			continue
		}
		if versioned {
			// Skip versioned entries — we process them per raw key below.
			iter.Next()
			continue
		}

		rawKey := mk.Key
		if err := gcKey(engine, rawKey, gcThreshold); err != nil {
			return err
		}

		// Advance to the next raw key's metadata.
		nextPrefix := nextKeyPrefix(rawKey)
		if nextPrefix == nil {
			break
		}
		if !iter.Seek(EncodeMeta(nextPrefix)) {
			break
		}
	}

	return nil
}

// gcKey processes a single raw key: collects all its versions, decides which
// to keep and which to drop, then deletes the dropped versions.
func gcKey(engine storage.Engine, key []byte, gcThreshold hlc.Timestamp) error {
	// Collect all versions for this key.
	versions, err := collectVersions(engine, key)
	if err != nil {
		return err
	}
	if len(versions) == 0 {
		return nil
	}

	// versions[0] is the newest (earliest in iteration order due to descending encoding).
	newest := versions[0]

	// Rule 4: If the newest version is a tombstone older than threshold,
	// drop everything — the key is fully dead.
	if newest.isTombstone && newest.timestamp.LessEq(gcThreshold) {
		for _, v := range versions {
			if err := engine.Delete(Encode(MVCCKey{Key: key, Timestamp: v.timestamp})); err != nil {
				return err
			}
		}
		// Delete metadata too.
		return engine.Delete(EncodeMeta(key))
	}

	// Rules 1-3: keep newest always, keep versions > threshold, drop the rest.
	for i := 1; i < len(versions); i++ {
		v := versions[i]
		if v.timestamp.LessEq(gcThreshold) {
			if err := engine.Delete(Encode(MVCCKey{Key: key, Timestamp: v.timestamp})); err != nil {
				return err
			}
		}
	}

	return nil
}

// versionInfo holds the timestamp and tombstone status of a single version.
type versionInfo struct {
	timestamp   hlc.Timestamp
	isTombstone bool
}

// collectVersions returns all MVCC versions for key, newest first.
func collectVersions(engine storage.Engine, key []byte) ([]versionInfo, error) {
	iter, err := engine.NewIterator()
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	// Seek past the metadata entry to the first version.
	// EncodeMeta(key) is <key>0x00. The first versioned entry is <key>0x00<12 bytes>.
	metaKey := EncodeMeta(key)
	if !iter.Seek(metaKey) {
		return nil, nil
	}

	// Skip the metadata entry itself.
	mk, versioned, err := Decode(iter.Key())
	if err != nil {
		return nil, err
	}
	if !versioned {
		// We landed on metadata — advance to the first version.
		if !iter.Next() {
			return nil, nil
		}
	} else if !bytesEqual(mk.Key, key) {
		return nil, nil // different raw key
	}

	var versions []versionInfo
	for iter.Valid() {
		mk, versioned, err := Decode(iter.Key())
		if err != nil || !versioned || !bytesEqual(mk.Key, key) {
			break // past this key's versions
		}

		_, isTombstone := decodeMVCCValue(iter.Value())
		versions = append(versions, versionInfo{
			timestamp:   mk.Timestamp,
			isTombstone: isTombstone,
		})

		iter.Next()
	}

	return versions, nil
}
