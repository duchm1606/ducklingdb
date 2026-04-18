package txn

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/kv/tscache"
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
	engine  storage.Engine
	clock   *hlc.Clock
	tscache *tscache.Cache // may be nil for tests that don't exercise push rules
	txn     Transaction
}

// Begin starts a new transaction. It allocates a fresh TxnID, a read/write
// timestamp from clock, a random priority, and durably writes a PENDING
// transaction record to the engine.
//
// cache is the shared timestamp cache used for push rules (Step 5). Pass
// nil to disable cache integration; push-past-cached-reads and
// register-reads-in-cache become no-ops, which is useful for tests that
// exercise other code paths in isolation.
//
// After Begin returns successfully, the caller can perform Get/Put/Delete on
// the returned coordinator until Commit or Abort is called.
func Begin(engine storage.Engine, clock *hlc.Clock, cache *tscache.Cache, iso IsolationLevel) (*TxnCoordSender, error) {
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
		engine:  engine,
		clock:   clock,
		tscache: cache,
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

// Get reads a key at this transaction's ReadTimestamp.
//
// If the read encounters a foreign intent, Step 5's read-intent resolver
// either finalizes a ghost blocker and retries, or pushes the blocker's
// commit timestamp past our ReadTimestamp so the intent becomes invisible
// to us. We don't block — we make the problem go away.
//
// On success, the read is registered in the timestamp cache so future
// writers will push past our ReadTimestamp rather than slipping a write
// below it (the retroactive-write anomaly — see blog 15).
func (tc *TxnCoordSender) Get(key []byte) ([]byte, error) {
	if tc.txn.IsFinalized() {
		return nil, ErrTxnFinalized
	}

	var val []byte
	for range maxWriteIntentResolutions {
		var err error
		val, err = mvcc.MVCCGet(tc.engine, key, tc.txn.ReadTimestamp, mvcc.ReadOptions{
			Txn:          &tc.txn.ID,
			MaxTimestamp: tc.txn.MaxTimestamp,
		})
		if err == nil {
			// Success: register the read so future writers push past us.
			if tc.tscache != nil {
				tc.tscache.Add(key, tc.txn.ReadTimestamp)
			}
			return val, nil
		}

		intentErr := extractWriteIntent(err)
		if intentErr == nil {
			return nil, err
		}

		// Read encountered a foreign intent. Resolve via push / ghost cleanup.
		retry, resolveErr := tc.handleReadIntent(intentErr)
		if resolveErr != nil {
			return nil, resolveErr
		}
		if !retry {
			return nil, fmt.Errorf("txn: unexpected non-retry with nil error")
		}
	}
	return nil, fmt.Errorf("txn: read intent resolution exceeded %d attempts", maxWriteIntentResolutions)
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
// Three push rules apply before the MVCC call (Step 5):
//  1. Timestamp-cache push: if any earlier read of this key was at a
//     timestamp ≥ WriteTimestamp, bump WriteTimestamp past it.
//
// Two conflict paths can appear on the MVCC call itself:
//  2. WriteIntentError (Step 4): resolve the foreign intent — ghost cleanup
//     or priority fight — then retry.
//  3. WriteTooOldError (Step 5): bump WriteTimestamp past the existing
//     committed version, then retry.
func (tc *TxnCoordSender) writeLocked(key, value []byte, isDelete bool) error {
	if tc.txn.IsFinalized() {
		return ErrTxnFinalized
	}

	// Rule 1: push past any cached read of this key.
	tc.pushPastCachedRead(key)

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

		// Rule 3: WriteTooOldError — bump WriteTimestamp and retry.
		if too := extractWriteTooOld(err); too != nil {
			tc.bumpWriteTimestampTo(too.ExistingTimestamp.Next())
			continue
		}

		// Rule 2: WriteIntentError — delegate to Step 4's resolver.
		intentErr := extractWriteIntent(err)
		if intentErr == nil {
			return err
		}
		retry, resolveErr := tc.resolveForeignIntent(intentErr)
		if resolveErr != nil {
			return resolveErr
		}
		if !retry {
			return fmt.Errorf("txn: unexpected non-retry with nil error")
		}
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
//  1. Refresh the in-memory record from its persisted copy. Another
//     transaction may have pushed our WriteTimestamp forward (Step 5's
//     reader-push path) or aborted us outright (Step 4's priority fight).
//     Our in-memory state is not authoritative after the first Put.
//  2. If the persisted record says ABORTED, surface a retry error — the
//     caller must start a new transaction with a new ID.
//  3. Apply isolation-level semantics:
//     SI  (Step 6) → pushed WriteTimestamp is acceptable; commit at it.
//     SSI (Step 7) → pushed WriteTimestamp > ReadTimestamp forces restart.
//  4. Flip the record to COMMITTED and persist. This is the single atomic
//     commit point — once this write lands, the transaction's effects are
//     durable even if intent resolution has not yet run.
//  5. Resolve each intent at the final (possibly pushed) WriteTimestamp.
//     If we crash mid-loop, any reader that hits an unresolved intent will
//     see the COMMITTED record and finalize the intent themselves.
func (tc *TxnCoordSender) Commit() error {
	if tc.txn.IsFinalized() {
		return ErrTxnFinalized
	}

	// Step 1: refresh from the persisted record.
	if err := tc.refreshRecord(); err != nil {
		return err
	}

	// Step 2: detect an abort by another transaction.
	if tc.txn.Status == TxnAborted {
		return &TxnRetryError{
			TxnID:  tc.txn.ID,
			Reason: "transaction was aborted by another coordinator",
		}
	}

	// Step 3: isolation-level commit check.
	if err := tc.checkCommitIsolation(); err != nil {
		return err
	}

	// Steps 4–5: flip to COMMITTED, resolve intents.
	tc.txn.Status = TxnCommitted
	tc.txn.LastHeartbeat = tc.clock.Now()
	if err := tc.writeTxnRecord(); err != nil {
		return fmt.Errorf("txn: commit record: %w", err)
	}
	return tc.resolveIntents(mvcc.TxnCommitted, tc.txn.WriteTimestamp)
}

// Abort finalizes the transaction as ABORTED. Same two-phase ordering as
// Commit: mark the record first, then clean up intents.
//
// Abort also refreshes the record so that double-abort (we were already
// aborted by another txn) is idempotent and still cleans up our intents.
func (tc *TxnCoordSender) Abort() error {
	if tc.txn.IsFinalized() {
		return ErrTxnFinalized
	}

	if err := tc.refreshRecord(); err != nil {
		return err
	}

	tc.txn.Status = TxnAborted
	tc.txn.LastHeartbeat = tc.clock.Now()
	if err := tc.writeTxnRecord(); err != nil {
		return fmt.Errorf("txn: abort record: %w", err)
	}
	return tc.resolveIntents(mvcc.TxnAborted, hlc.Timestamp{})
}

// refreshRecord re-reads this transaction's record from storage and merges
// the authoritative fields (Status, WriteTimestamp, Priority) into the
// coordinator's in-memory state. Local-only fields (IntentKeys) are kept
// because the on-disk record is updated after each Put anyway, and the
// in-memory list is what drives intent resolution.
//
// Returns an error only for engine-level failures. A missing record means
// no conflicts found us — preserve the in-memory state.
func (tc *TxnCoordSender) refreshRecord() error {
	persisted, found, err := LoadTxnRecord(tc.engine, tc.txn.ID)
	if err != nil {
		return fmt.Errorf("txn: refresh record: %w", err)
	}
	if !found {
		return nil
	}
	// Pick up any changes made by other transactions. Intent keys remain our
	// local list; the persisted record's IntentKeys is written by us on each
	// Put and should match our local copy.
	tc.txn.Status = persisted.Status
	tc.txn.WriteTimestamp = persisted.WriteTimestamp
	tc.txn.Priority = persisted.Priority
	return nil
}

// checkCommitIsolation applies the isolation-level rule for whether a
// pushed WriteTimestamp is acceptable at commit time.
//
//	SI  → any WriteTimestamp ≥ ReadTimestamp is fine. Write skew is permitted
//	      (see blog 18 for the canonical sum-invariant example).
//	SSI → WriteTimestamp must equal ReadTimestamp; any push forces a restart.
//	      This closes the write-skew loophole at the cost of more retries.
//
// Both branches share this function because the rule is "one line each" and
// splitting them across separate methods would obscure the contrast.
func (tc *TxnCoordSender) checkCommitIsolation() error {
	switch tc.txn.Isolation {
	case SI:
		return nil
	case SSI:
		if tc.txn.ReadTimestamp.Less(tc.txn.WriteTimestamp) {
			return &TxnRetryError{
				TxnID:           tc.txn.ID,
				Reason:          fmt.Sprintf("SSI commit timestamp pushed from %s to %s", tc.txn.ReadTimestamp, tc.txn.WriteTimestamp),
				SuggestedMinPri: tc.txn.Priority,
			}
		}
		return nil
	default:
		return fmt.Errorf("txn: unknown isolation level %v", tc.txn.Isolation)
	}
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
