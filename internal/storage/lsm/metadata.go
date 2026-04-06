package lsm

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
)

const (
	metaSlot0 = "meta0"
	metaSlot1 = "meta1"
)

// MetaData describes the current set of SSTable files across all levels.
// Levels[0] = L0 (newest, may have overlapping key ranges).
// Levels[1], [2], ... = L1, L2, ... (non-overlapping within each level).
type MetaData struct {
	Version uint64     `json:"version"`
	Levels  [][]string `json:"levels"`
}

// MetaStore is a crash-safe two-slot metadata store.
//
// It maintains two slot files (meta0, meta1) in the LSM directory. Each slot
// holds a JSON-encoded MetaData followed by a CRC32 checksum. Writes always
// target the slot with the lower version, so the other slot retains the last
// known-good state. A crash mid-write leaves at most one slot corrupt; Load
// always finds the other.
type MetaStore struct {
	dir string
}

// NewMetaStore returns a MetaStore rooted at dir.
// dir must already exist.
func NewMetaStore(dir string) *MetaStore {
	return &MetaStore{dir: dir}
}

// Save writes meta to the older slot and fsyncs.
// The version field is set internally to max(existing versions) + 1;
// the caller does not need to manage it.
func (ms *MetaStore) Save(meta MetaData) error {
	slot0, _ := ms.readSlot(metaSlot0) // invalid slot → zero-value MetaData (Version=0)
	slot1, _ := ms.readSlot(metaSlot1)

	// Overwrite the slot with the lower version — the other slot keeps the
	// previous valid state in case we crash during this write.
	target := metaSlot0
	if slot1.Version < slot0.Version {
		target = metaSlot1
	}

	meta.Version = max(slot0.Version, slot1.Version) + 1
	return ms.writeSlot(target, meta)
}

// Load reads both slots and returns the one with the higher valid version.
// Returns an empty MetaData (no error) when neither slot exists yet.
func (ms *MetaStore) Load() (MetaData, error) {
	slot0, err0 := ms.readSlot(metaSlot0)
	slot1, err1 := ms.readSlot(metaSlot1)

	if err0 != nil && err1 != nil {
		return MetaData{}, nil // fresh directory — no metadata yet
	}
	if err0 != nil {
		return slot1, nil
	}
	if err1 != nil {
		return slot0, nil
	}
	if slot1.Version > slot0.Version {
		return slot1, nil
	}
	return slot0, nil
}

// writeSlot serializes meta to JSON, appends a 4-byte CRC32, and fsyncs.
//
// Slot format: [json_bytes...][crc32:4]
// The last 4 bytes are always the checksum of everything before them.
func (ms *MetaStore) writeSlot(name string, meta MetaData) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("metadata marshal: %w", err)
	}

	checksum := crc32.ChecksumIEEE(data)
	buf := make([]byte, len(data)+4)
	copy(buf, data)
	binary.LittleEndian.PutUint32(buf[len(data):], checksum)

	fp, err := os.OpenFile(ms.slotPath(name), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("open meta slot %s: %w", name, err)
	}
	defer fp.Close()

	if _, err := fp.Write(buf); err != nil {
		return fmt.Errorf("write meta slot %s: %w", name, err)
	}
	return fp.Sync()
}

// readSlot reads a slot file, verifies the CRC32, and decodes the JSON.
func (ms *MetaStore) readSlot(name string) (MetaData, error) {
	buf, err := os.ReadFile(ms.slotPath(name))
	if err != nil {
		return MetaData{}, err
	}
	if len(buf) < 4 {
		return MetaData{}, fmt.Errorf("meta slot %s: too small (%d bytes)", name, len(buf))
	}

	data := buf[:len(buf)-4]
	stored := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	computed := crc32.ChecksumIEEE(data)
	if stored != computed {
		return MetaData{}, fmt.Errorf("meta slot %s: checksum mismatch (stored=%08x computed=%08x)", name, stored, computed)
	}

	var meta MetaData
	if err := json.Unmarshal(data, &meta); err != nil {
		return MetaData{}, fmt.Errorf("meta slot %s: unmarshal: %w", name, err)
	}
	return meta, nil
}

func (ms *MetaStore) slotPath(name string) string {
	return filepath.Join(ms.dir, name)
}
