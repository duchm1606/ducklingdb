package txn

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// ErrTxnFinalized is returned when an operation is attempted on a transaction
// that has already committed or aborted. A finalized coordinator is a dead
// object — callers must start a new transaction.
var ErrTxnFinalized = errors.New("txn: transaction is already finalized")

// TxnCoordSender drives a single transaction from Begin through Commit or
// Abort. One coordinator corresponds to one Transaction record in the engine.
//
// The coordinator is not safe for concurrent use — a single caller drives it
// sequentially. (CockroachDB's real TxnCoordSender is more elaborate because
// it fans work out across multiple ranges; a single-node educational version
// does not need that machinery.)
type TxnCoordSender struct {
	engine storage.Engine
	clock  *hlc.Clock
	txn    Transaction
}

// Begin starts a new transaction. It allocates a fresh TxnID, a read/write
// timestamp from clock, a random priority, and durably writes a PENDING
// transaction record to the engine.
//
// After Begin returns successfully, the caller can perform Get/Put/Delete on
// the returned coordinator until Commit or Abort is called.
func Begin(engine storage.Engine, clock *hlc.Clock, iso IsolationLevel) (*TxnCoordSender, error) {
	id, err := newTxnID()
	if err != nil {
		return nil, fmt.Errorf("txn: generate id: %w", err)
	}

	priority, err := randomPriority()
	if err != nil {
		return nil, fmt.Errorf("txn: generate priority: %w", err)
	}

	ts := clock.Now()
	maxTS := hlc.Timestamp{
		WallTime: ts.WallTime + int64(clock.MaxOffset()),
		Logical:  ts.Logical,
	}

	tc := &TxnCoordSender{
		engine: engine,
		clock:  clock,
		txn: Transaction{
			ID:             id,
			Status:         TxnPending,
			Isolation:      iso,
			ReadTimestamp:  ts,
			WriteTimestamp: ts,
			MaxTimestamp:   maxTS,
			Priority:       priority,
			LastHeartbeat:  ts,
		},
	}

	if err := tc.writeTxnRecord(); err != nil {
		return nil, fmt.Errorf("txn: write initial record: %w", err)
	}
	return tc, nil
}

// ID returns this transaction's identifier. Exposed for conflict resolution
// and testing.
func (tc *TxnCoordSender) ID() mvcc.TxnID { return tc.txn.ID }

// Status returns the transaction's current lifecycle state.
func (tc *TxnCoordSender) Status() TxnStatus { return tc.txn.Status }

// ReadTimestamp returns the snapshot timestamp reads are performed at.
func (tc *TxnCoordSender) ReadTimestamp() hlc.Timestamp { return tc.txn.ReadTimestamp }

// WriteTimestamp returns the current candidate commit timestamp. This may be
// pushed forward by conflicts before commit.
func (tc *TxnCoordSender) WriteTimestamp() hlc.Timestamp { return tc.txn.WriteTimestamp }

// Snapshot returns a copy of the underlying transaction record. Callers must
// treat the returned value as read-only — mutating it has no effect on the
// coordinator. Useful for conflict resolution, where another coordinator
// reads our record to decide whether to push or abort us.
func (tc *TxnCoordSender) Snapshot() Transaction {
	// IntentKeys is a slice of slices — shallow copy leaves the inner slices
	// aliased, but callers should never mutate them either.
	snapshot := tc.txn
	if len(tc.txn.IntentKeys) > 0 {
		snapshot.IntentKeys = make([][]byte, len(tc.txn.IntentKeys))
		copy(snapshot.IntentKeys, tc.txn.IntentKeys)
	}
	return snapshot
}

// Get reads a key at this transaction's ReadTimestamp. If the key has an
// intent from another transaction, the error is propagated up — the caller
// (or a future conflict-resolution layer) decides what to do. Intents owned
// by this transaction are visible to Get (the txn sees its own writes).
func (tc *TxnCoordSender) Get(key []byte) ([]byte, error) {
	if tc.txn.IsFinalized() {
		return nil, ErrTxnFinalized
	}
	return mvcc.MVCCGet(tc.engine, key, tc.txn.ReadTimestamp, mvcc.ReadOptions{
		Txn: &tc.txn.ID,
	})
}

// Put writes value at key as a write intent owned by this transaction. The
// key is tracked in the transaction's IntentKeys list so it can be resolved
// at Commit or Abort time.
//
// If another transaction already has an intent on key, MVCCPut returns
// ErrWriteIntent, which is propagated for the conflict layer (Step 4) to
// handle.
func (tc *TxnCoordSender) Put(key, value []byte) error {
	return tc.writeLocked(key, value, false)
}

// Delete writes a tombstone intent at key.
func (tc *TxnCoordSender) Delete(key []byte) error {
	return tc.writeLocked(key, nil, true)
}

// writeLocked is the shared path for Put and Delete. Naming mirrors standard
// Go convention even though this coordinator is not internally locked — the
// contract is "caller ensures no concurrent use."
//
// If the underlying MVCC call returns a WriteIntentError, we delegate to
// resolveForeignIntent (Step 4): a blocker that's COMMITTED or ABORTED is
// finalized and we retry; a PENDING blocker triggers a priority fight. The
// retry loop is bounded so a pathological case can't hang forever.
func (tc *TxnCoordSender) writeLocked(key, value []byte, isDelete bool) error {
	if tc.txn.IsFinalized() {
		return ErrTxnFinalized
	}

	for range maxWriteIntentResolutions {
		var err error
		if isDelete {
			err = mvcc.MVCCDelete(tc.engine, key, tc.txn.WriteTimestamp, &tc.txn.ID)
		} else {
			err = mvcc.MVCCPut(tc.engine, key, tc.txn.WriteTimestamp, value, &tc.txn.ID)
		}
		if err == nil {
			break
		}

		intentErr := extractWriteIntent(err)
		if intentErr == nil {
			return err
		}

		retry, resolveErr := tc.resolveForeignIntent(intentErr)
		if resolveErr != nil {
			return resolveErr
		}
		if !retry {
			// Unreachable: resolveForeignIntent returns (false, nil) never —
			// (false, err) is the retry=false path. Defensive.
			return fmt.Errorf("txn: unexpected non-retry with nil error")
		}
		// retry=true — loop around and try the MVCC write again.
	}

	tc.txn.AddIntentKey(key)
	// Persist the updated IntentKeys list. If we crashed between writing the
	// intent and updating the record, the intent would be orphaned — the
	// commit/abort path iterates IntentKeys to resolve them, so an unrecorded
	// intent is effectively a leak. A real system would batch record updates;
	// for clarity we flush after every write.
	if err := tc.writeTxnRecord(); err != nil {
		return fmt.Errorf("txn: record update after write: %w", err)
	}
	return nil
}

// Commit finalizes the transaction. Order of operations matters:
//  1. Flip the record to COMMITTED and persist it. This is the single
//     atomic commit point: once the record is COMMITTED, the transaction's
//     writes are considered durable even if intent resolution has not yet
//     happened.
//  2. Resolve each intent. If we crash mid-loop, unresolved intents are not
//     lost — any reader that encounters one will see the COMMITTED record and
//     finalize the intent themselves.
func (tc *TxnCoordSender) Commit() error {
	if tc.txn.IsFinalized() {
		return ErrTxnFinalized
	}

	tc.txn.Status = TxnCommitted
	tc.txn.LastHeartbeat = tc.clock.Now()
	if err := tc.writeTxnRecord(); err != nil {
		return fmt.Errorf("txn: commit record: %w", err)
	}

	return tc.resolveIntents(mvcc.TxnCommitted, tc.txn.WriteTimestamp)
}

// Abort finalizes the transaction as ABORTED. Same two-phase ordering as
// Commit: mark the record first, then clean up intents.
func (tc *TxnCoordSender) Abort() error {
	if tc.txn.IsFinalized() {
		return ErrTxnFinalized
	}

	tc.txn.Status = TxnAborted
	tc.txn.LastHeartbeat = tc.clock.Now()
	if err := tc.writeTxnRecord(); err != nil {
		return fmt.Errorf("txn: abort record: %w", err)
	}

	return tc.resolveIntents(mvcc.TxnAborted, hlc.Timestamp{})
}

func (tc *TxnCoordSender) resolveIntents(status mvcc.TxnStatus, commitTS hlc.Timestamp) error {
	for _, key := range tc.txn.IntentKeys {
		if err := mvcc.MVCCResolveWriteIntent(tc.engine, key, tc.txn.ID, status, commitTS); err != nil {
			return fmt.Errorf("txn: resolve intent %q: %w", key, err)
		}
	}
	return nil
}

// writeTxnRecord persists the current transaction record as an inline MVCC
// value. Inline means timestamp=0 — there is one record per transaction that
// is updated in place, never versioned.
func (tc *TxnCoordSender) writeTxnRecord() error {
	data, err := tc.txn.Encode()
	if err != nil {
		return err
	}
	return mvcc.MVCCPut(tc.engine, TxnRecordKey(tc.txn.ID), hlc.Timestamp{}, data, nil)
}

// LoadTxnRecord reads the transaction record for id from engine. Used by the
// conflict layer (Step 4) to look up another transaction's state.
func LoadTxnRecord(engine storage.Engine, id mvcc.TxnID) (Transaction, bool, error) {
	data, err := mvcc.MVCCGet(engine, TxnRecordKey(id), hlc.Timestamp{}, mvcc.ReadOptions{})
	if err != nil {
		if errors.Is(err, storage.ErrKeyNotFound) {
			return Transaction{}, false, nil
		}
		return Transaction{}, false, err
	}
	t, err := DecodeTransaction(data)
	if err != nil {
		return Transaction{}, false, err
	}
	return t, true, nil
}

// newTxnID returns a cryptographically random 16-byte TxnID.
func newTxnID() (mvcc.TxnID, error) {
	var id mvcc.TxnID
	if _, err := rand.Read(id[:]); err != nil {
		return mvcc.TxnID{}, err
	}
	return id, nil
}

// randomPriority returns a non-negative int32 drawn from crypto/rand.
// Using crypto/rand keeps tests and production on the same path (no global
// math/rand seeding required).
func randomPriority() (int32, error) {
	var buf [4]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return 0, err
	}
	// Clear the top bit so the result is non-negative.
	return int32(binary.BigEndian.Uint32(buf[:]) & 0x7FFFFFFF), nil
}
