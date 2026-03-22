package lsm

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	OpPut uint8 = iota
	OpDelete
)

const entryHeaderSize = 13

// ErrChecksumMismatch is returned when a decoded entry checksum does not match.
var ErrChecksumMismatch = errors.New("entry checksum mismatch")

/*
┌──────────────┬──────────────┬──────────────┬──────┬──────────────┬────────────────┐
│   Checksum   │   Key Len    │  Value Len   │  Op  │     Key      │     Value      │
│   4 bytes    │   4 bytes    │   4 bytes    │ 1 B  │  KeyLen B    │  ValLen B      │
│   CRC32-IEEE │   uint32 LE  │  uint32 LE   │      │              │                │
│              │              │              │      │              │                │
│  offset: 0   │  offset: 4   │  offset: 8   │ 12   │     13       │  13 + KeyLen   │
└──────────────┴──────────────┴──────────────┴──────┴──────────────┴────────────────┘
 ◄─ checksum ─►◄──────────── checksum covers everything here ─────────────────────►
*/

// Entry is the unit written to and read from the write-ahead log.
type Entry struct {
	Key   []byte
	Value []byte
	Op    uint8
}

// Encode serializes the entry into the WAL binary format.
func (ent Entry) Encode() []byte {
	encoded := make([]byte, entryHeaderSize+len(ent.Key)+len(ent.Value))
	binary.LittleEndian.PutUint32(encoded[4:8], uint32(len(ent.Key)))
	binary.LittleEndian.PutUint32(encoded[8:12], uint32(len(ent.Value)))
	encoded[12] = ent.Op

	copy(encoded[entryHeaderSize:], ent.Key)
	copy(encoded[entryHeaderSize+len(ent.Key):], ent.Value)

	checksum := crc32.ChecksumIEEE(encoded[4:])
	binary.LittleEndian.PutUint32(encoded[0:4], checksum)

	return encoded
}

// Decode reads and decodes a single entry from r.
func (ent *Entry) Decode(r io.Reader) error {
	header := make([]byte, entryHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return err
	}

	keyLen := binary.LittleEndian.Uint32(header[4:8])
	valueLen := binary.LittleEndian.Uint32(header[8:12])
	payload := make([]byte, int(keyLen+valueLen))
	if _, err := io.ReadFull(r, payload); err != nil {
		return err
	}

	storedChecksum := binary.LittleEndian.Uint32(header[0:4])
	computedChecksum := crc32.Update(0, crc32.IEEETable, header[4:])
	computedChecksum = crc32.Update(computedChecksum, crc32.IEEETable, payload)
	if computedChecksum != storedChecksum {
		return ErrChecksumMismatch
	}

	ent.Op = header[12]
	ent.Key = append(ent.Key[:0], payload[:keyLen]...)
	ent.Value = append(ent.Value[:0], payload[keyLen:]...)

	return nil
}
