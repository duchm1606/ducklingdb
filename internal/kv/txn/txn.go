// Package txn implements single-node ACID transactions layered on top of the
// MVCC engine. It provides the transaction record (this file), the coordinator
// that drives a transaction's lifecycle, and the conflict-resolution rules.
package txn

import (
	"encoding/json"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// TxnStatus is the lifecycle state of a transaction.
//
// Status transitions:
//
//	TxnPending → TxnCommitted   (successful commit)
//	TxnPending → TxnAborted     (explicit abort, conflict loss, or timeout)
//
// Once a transaction leaves TxnPending the status is terminal — no further
// writes or commits are allowed on that transaction instance.
type TxnStatus int

const (
	TxnPending TxnStatus = iota
	TxnCommitted
	TxnAborted
)

// String returns a short human-readable label for the status.
func (s TxnStatus) String() string {
	switch s {
	case TxnPending:
		return "PENDING"
	case TxnCommitted:
		return "COMMITTED"
	case TxnAborted:
		return "ABORTED"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// IsolationLevel selects between Snapshot Isolation and Serializable Snapshot
// Isolation. See M2.11 / M2.12 in docs/SCOPE.md for the semantic difference:
// under SI a pushed commit timestamp is acceptable, under SSI it forces a
// restart.
type IsolationLevel int

const (
	// SSI is Serializable Snapshot Isolation — the default. No anomalies; a
	// pushed WriteTimestamp forces the transaction to restart.
	SSI IsolationLevel = iota
	// SI is Snapshot Isolation. A pushed WriteTimestamp is accepted and the
	// transaction commits. Write skew is possible.
	SI
)

// String returns "SI" or "SSI".
func (i IsolationLevel) String() string {
	switch i {
	case SSI:
		return "SSI"
	case SI:
		return "SI"
	default:
		return fmt.Sprintf("unknown(%d)", int(i))
	}
}

// Transaction is the durable record describing a single transaction's
// identity and current state. It is stored in the engine as an inline MVCC
// value under TxnRecordKey(ID) so every participant — the coordinator itself,
// readers encountering intents, other writers racing for the same key — sees
// one consistent truth.
//
// Field roles:
//
//   - ID: unique identifier, stable across restarts of the same logical
//     transaction. An aborted transaction must start fresh with a new ID.
//
//   - ReadTimestamp: the snapshot the transaction reads from. Reads see all
//     data with timestamp ≤ ReadTimestamp.
//
//   - WriteTimestamp: the candidate commit timestamp. Starts equal to
//     ReadTimestamp and may be pushed forward by conflicts. At commit time:
//     SSI → must still equal ReadTimestamp, otherwise restart.
//     SI  → any value ≥ ReadTimestamp is fine.
//
//   - MaxTimestamp: ReadTimestamp + Clock.MaxOffset. A committed value with
//     timestamp inside (ReadTimestamp, MaxTimestamp] may have been written
//     before the transaction began — its true ordering is uncertain and the
//     transaction must restart past it. Used in Step 9.
//
//   - Priority: random int assigned at Begin. Higher priority wins
//     priority-based conflicts. Restarts bump priority so the retry is less
//     likely to lose the same fight again.
//
//   - IntentKeys: every key on which this transaction has written an intent.
//     Commit/Abort iterates this list to resolve each intent.
//
//   - LastHeartbeat: liveness indicator used by M4 once transactions outlive a
//     single coordinator. Set on every coordinator action; stale heartbeats
//     let other txns infer this one has died and abort it.
type Transaction struct {
	ID             mvcc.TxnID     `json:"id"`
	Status         TxnStatus      `json:"status"`
	Isolation      IsolationLevel `json:"iso"`
	ReadTimestamp  hlc.Timestamp  `json:"read_ts"`
	WriteTimestamp hlc.Timestamp  `json:"write_ts"`
	MaxTimestamp   hlc.Timestamp  `json:"max_ts,omitzero"`
	Priority       int32          `json:"pri"`
	IntentKeys     [][]byte       `json:"intents,omitempty"`
	LastHeartbeat  hlc.Timestamp  `json:"hb,omitzero"`
}

// IsFinalized reports whether the transaction has reached a terminal status.
func (t *Transaction) IsFinalized() bool {
	return t.Status == TxnCommitted || t.Status == TxnAborted
}

// AddIntentKey records that this transaction wrote an intent at key. Duplicate
// entries are skipped — multiple writes to the same key produce a single
// intent in the engine, so only one cleanup is needed at commit/abort time.
func (t *Transaction) AddIntentKey(key []byte) {
	for _, k := range t.IntentKeys {
		if bytesEqual(k, key) {
			return
		}
	}
	// Copy the key — the caller may reuse the slice.
	cp := make([]byte, len(key))
	copy(cp, key)
	t.IntentKeys = append(t.IntentKeys, cp)
}

// Encode serializes the transaction record for storage as an inline MVCC
// value. JSON is used for the same reason MVCCMetadata uses it: easy
// inspection, schema tolerance, good enough for an educational project.
func (t *Transaction) Encode() ([]byte, error) {
	b, err := json.Marshal(t)
	if err != nil {
		return nil, fmt.Errorf("txn record marshal: %w", err)
	}
	return b, nil
}

// DecodeTransaction parses bytes previously produced by Encode.
func DecodeTransaction(data []byte) (Transaction, error) {
	var t Transaction
	if err := json.Unmarshal(data, &t); err != nil {
		return Transaction{}, fmt.Errorf("txn record unmarshal: %w", err)
	}
	return t, nil
}

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
