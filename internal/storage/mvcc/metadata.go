package mvcc

import (
	"encoding/json"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// TxnID is a transaction identifier. 16 bytes mirrors a UUID but avoids an
// external dependency; callers can populate it from any source of uniqueness
// (real UUIDs, monotonic counters, etc.).
type TxnID [16]byte

// IsZero reports whether t is the zero TxnID.
func (t TxnID) IsZero() bool {
	for _, b := range t {
		if b != 0 {
			return false
		}
	}
	return true
}

// MVCCMetadata is the per-key index entry. It lives at the unversioned key
// (`EncodeMeta(rawKey)`) in the LSM engine and points at the newest version
// of that key plus any outstanding write intent.
//
// Invariants:
//   - Timestamp is the timestamp of the newest committed version, or the
//     intent timestamp when an intent is present and no prior version exists.
//   - When Txn is non-nil, the newest version at (rawKey, TxnTimestamp) is an
//     uncommitted write intent belonging to that transaction.
//   - Deleted is true iff the newest version is a tombstone.
type MVCCMetadata struct {
	Timestamp    hlc.Timestamp `json:"ts"`
	Deleted      bool          `json:"del,omitempty"`
	Txn          *TxnID        `json:"txn,omitempty"`
	TxnTimestamp hlc.Timestamp `json:"txn_ts,omitempty"`
	KeyBytes     int64         `json:"kb,omitempty"`
	ValBytes     int64         `json:"vb,omitempty"`
	InlineValue  []byte        `json:"inline,omitempty"`
}

// IsInline reports whether m holds an inline (unversioned) value.
func (m *MVCCMetadata) IsInline() bool {
	return m.InlineValue != nil
}

// HasIntent reports whether m describes an outstanding write intent.
func (m *MVCCMetadata) HasIntent() bool {
	return m.Txn != nil && !m.Txn.IsZero()
}

// Encode serializes m to bytes suitable for storing as a value in the engine.
func (m *MVCCMetadata) Encode() ([]byte, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, fmt.Errorf("mvcc metadata marshal: %w", err)
	}
	return b, nil
}

// DecodeMetadata parses bytes previously produced by Encode.
func DecodeMetadata(data []byte) (MVCCMetadata, error) {
	var m MVCCMetadata
	if err := json.Unmarshal(data, &m); err != nil {
		return MVCCMetadata{}, fmt.Errorf("mvcc metadata unmarshal: %w", err)
	}
	return m, nil
}
