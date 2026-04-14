package mvcc

import (
	"encoding/binary"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// MVCC key layout (CockroachDB-inspired):
//
//   <raw_key> 0x00 <wall_time_desc:8> <logical_desc:4>
//                 └─ metadata sentinel (a key with ONLY this suffix is metadata)
//
// The timestamp bytes are stored in DESCENDING order so that for the same raw
// key, newer versions sort BEFORE older versions in the LSM engine. This
// matters because a point read at timestamp T does:
//   Seek(Encode(key, T))
// and wants the first hit to be the newest version with timestamp <= T.
// Descending encoding makes that naturally fall out of a forward scan.
//
// Descending is implemented by bit-complementing each byte (XOR with 0xff).
// The 0x00 separator between the raw key and the timestamp is what lets us
// distinguish a metadata entry (no timestamp suffix) from versioned entries.

const (
	// keySeparator is the single byte between the raw key and the timestamp.
	keySeparator = 0x00

	// tsSize is the number of bytes used to encode a timestamp: 8 for WallTime,
	// 4 for Logical.
	tsSize = 12
)

// MVCCKey is a raw key + HLC timestamp. The zero Timestamp represents an
// "inline" key (metadata or untimed value).
type MVCCKey struct {
	Key       []byte
	Timestamp hlc.Timestamp
}

// Encode serializes k. Inline keys (zero timestamp) are encoded as metadata
// keys: <raw_key> 0x00. Versioned keys get the full 12-byte timestamp suffix.
func Encode(k MVCCKey) []byte {
	if k.Timestamp.IsEmpty() {
		return EncodeMeta(k.Key)
	}

	out := make([]byte, len(k.Key)+1+tsSize)
	copy(out, k.Key)
	out[len(k.Key)] = keySeparator
	encodeTimestampDesc(out[len(k.Key)+1:], k.Timestamp)
	return out
}

// EncodeMeta returns the metadata sentinel for a raw key: <raw_key> 0x00.
// Metadata sorts before every versioned entry for the same raw key because
// the 12 timestamp bytes are strictly greater than nothing.
func EncodeMeta(key []byte) []byte {
	out := make([]byte, len(key)+1)
	copy(out, key)
	out[len(key)] = keySeparator
	return out
}

// Decode parses an encoded MVCC key. Returns the raw key, timestamp, and
// whether the key was a versioned entry (true) or metadata (false).
func Decode(encoded []byte) (MVCCKey, bool, error) {
	if len(encoded) == 0 {
		return MVCCKey{}, false, fmt.Errorf("mvcc: decode empty key")
	}

	// A well-formed encoded key ends with either:
	//   <raw_key> 0x00                    — metadata (len = len(raw_key) + 1)
	//   <raw_key> 0x00 <12 bytes>         — versioned
	//
	// The separator 0x00 can also appear inside raw_key, so we must locate it
	// by length: the timestamp suffix is exactly 12 bytes when present.

	// Case 1: versioned. The 13th-to-last byte (index len-1-tsSize) must be 0x00.
	if len(encoded) > tsSize {
		sepIdx := len(encoded) - tsSize - 1
		if encoded[sepIdx] == keySeparator {
			ts := decodeTimestampDesc(encoded[sepIdx+1:])
			rawKey := append([]byte{}, encoded[:sepIdx]...)
			return MVCCKey{Key: rawKey, Timestamp: ts}, true, nil
		}
	}

	// Case 2: metadata. Last byte must be 0x00.
	if encoded[len(encoded)-1] == keySeparator {
		rawKey := append([]byte{}, encoded[:len(encoded)-1]...)
		return MVCCKey{Key: rawKey}, false, nil
	}

	return MVCCKey{}, false, fmt.Errorf("mvcc: malformed key (no separator)")
}

// encodeTimestampDesc writes ts into dst (length must be 12) with each byte
// bit-complemented so that larger timestamps encode to smaller byte sequences.
func encodeTimestampDesc(dst []byte, ts hlc.Timestamp) {
	// Wall time: uint64 big-endian, then bitwise complement.
	binary.BigEndian.PutUint64(dst[0:8], uint64(ts.WallTime))
	for i := 0; i < 8; i++ {
		dst[i] = ^dst[i]
	}
	// Logical: int32 big-endian, then bitwise complement.
	binary.BigEndian.PutUint32(dst[8:12], uint32(ts.Logical))
	for i := 8; i < 12; i++ {
		dst[i] = ^dst[i]
	}
}

// decodeTimestampDesc reverses encodeTimestampDesc. src must be exactly 12 bytes.
func decodeTimestampDesc(src []byte) hlc.Timestamp {
	wall := make([]byte, 8)
	for i := 0; i < 8; i++ {
		wall[i] = ^src[i]
	}
	logical := make([]byte, 4)
	for i := 0; i < 4; i++ {
		logical[i] = ^src[8+i]
	}
	return hlc.Timestamp{
		WallTime: int64(binary.BigEndian.Uint64(wall)),
		Logical:  int32(binary.BigEndian.Uint32(logical)),
	}
}
