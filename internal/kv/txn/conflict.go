package txn

import (
	"errors"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// maxWriteIntentResolutions bounds how many times a single Put may cascade
// into resolving foreign intents before giving up. In a healthy system the
// loop terminates within 1–2 iterations — either the blocker is dead (one
// resolve + retry) or we hit a live conflict (one outcome check). A stuck
// loop indicates a bug; this constant turns it into a visible error instead
// of an infinite hang.
const maxWriteIntentResolutions = 8

// TxnRetryError signals that the transaction lost a conflict and must be
// restarted (with a bumped priority) if the caller wants to make progress.
// Step 8 will consume this error inside a RunTransaction retry wrapper;
// for now callers that don't want to retry can propagate it up.
type TxnRetryError struct {
	TxnID           mvcc.TxnID
	Reason          string
	SuggestedMinPri int32 // caller should bump priority to at least this value
}

// Error implements error.
func (e *TxnRetryError) Error() string {
	return fmt.Sprintf("txn %x retry: %s", e.TxnID[:4], e.Reason)
}

// resolveForeignIntent is called from Put/Delete when MVCC returns a
// WriteIntentError. It inspects the blocker's record and takes action:
//
//   - Blocker is COMMITTED → finalize its intent as committed, caller retries
//   - Blocker is ABORTED   → finalize its intent as aborted, caller retries
//   - No record found      → treat as aborted (the coordinator died before
//     ever persisting a record, or the record was GC'd)
//   - Blocker is PENDING and we have higher priority → abort the blocker,
//     finalize its intent, caller retries
//   - Blocker is PENDING and our priority is ≤ theirs → return a TxnRetryError
//     so the caller can restart with a bumped priority
//
// Returns (retry, err):
//   - retry=true, err=nil  → caller should retry the original write
//   - retry=false, err=... → caller should propagate err up
func (tc *TxnCoordSender) resolveForeignIntent(intentErr *mvcc.WriteIntentError) (retry bool, err error) {
	blocker, found, err := LoadTxnRecord(tc.engine, intentErr.TxnID)
	if err != nil {
		return false, fmt.Errorf("txn: load blocker record: %w", err)
	}

	// No record (or already terminal): clean up the stale intent and retry.
	if !found || blocker.Status == TxnAborted {
		return true, tc.finalizeForeignIntent(intentErr, mvcc.TxnAborted, hlc.Timestamp{})
	}
	if blocker.Status == TxnCommitted {
		return true, tc.finalizeForeignIntent(intentErr, mvcc.TxnCommitted, blocker.WriteTimestamp)
	}

	// PENDING — priority fight.
	// Higher priority wins. On a tie, the current txn (us) loses and must
	// back off and retry; otherwise two equal-priority peers would ping-pong
	// aborts forever.
	if tc.txn.Priority > blocker.Priority {
		if err := tc.abortBlocker(blocker); err != nil {
			return false, err
		}
		return true, tc.finalizeForeignIntent(intentErr, mvcc.TxnAborted, hlc.Timestamp{})
	}

	return false, &TxnRetryError{
		TxnID:           tc.txn.ID,
		Reason:          fmt.Sprintf("write-write conflict on %q with txn %x (priority %d ≤ %d)", intentErr.Key, intentErr.TxnID[:4], tc.txn.Priority, blocker.Priority),
		SuggestedMinPri: blocker.Priority + 1,
	}
}

// finalizeForeignIntent resolves the blocking intent via MVCC and returns any
// error. This is the cleanup step that follows every "yes we should retry"
// decision.
func (tc *TxnCoordSender) finalizeForeignIntent(intentErr *mvcc.WriteIntentError, status mvcc.TxnStatus, commitTS hlc.Timestamp) error {
	err := mvcc.MVCCResolveWriteIntent(tc.engine, intentErr.Key, intentErr.TxnID, status, commitTS)
	if err != nil {
		return fmt.Errorf("txn: resolve foreign intent: %w", err)
	}
	return nil
}

// abortBlocker forcibly marks another transaction as ABORTED. This is called
// when we win a priority fight against a PENDING transaction.
//
// Note: we only flip the *record* here. The blocker's remaining intents on
// other keys stay in place until some future transaction encounters them and
// resolves them via this same mechanism. That's intentional — a lazy
// distributed cleanup pattern. It means we don't need to enumerate the
// blocker's IntentKeys or coordinate with the blocker's coordinator.
func (tc *TxnCoordSender) abortBlocker(blocker Transaction) error {
	blocker.Status = TxnAborted
	blocker.LastHeartbeat = tc.clock.Now()

	data, err := blocker.Encode()
	if err != nil {
		return fmt.Errorf("txn: encode aborted blocker: %w", err)
	}
	// Write the record as an inline MVCC value (same as the blocker's own
	// coordinator would). nil txn → not an intent, just an in-place update.
	return mvcc.MVCCPut(tc.engine, TxnRecordKey(blocker.ID), hlc.Timestamp{}, data, nil)
}

// extractWriteIntent pulls a *WriteIntentError out of an error chain, or
// returns nil if the error is not (or does not wrap) a write-intent conflict.
func extractWriteIntent(err error) *mvcc.WriteIntentError {
	var wi *mvcc.WriteIntentError
	if errors.As(err, &wi) {
		return wi
	}
	return nil
}

// Compile-time assertions that the error types satisfy error.
var (
	_ error = (*TxnRetryError)(nil)
	_ error = (*mvcc.WriteIntentError)(nil)
)
