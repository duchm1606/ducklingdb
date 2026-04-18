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

// extractWriteTooOld pulls a *WriteTooOldError from an error chain.
func extractWriteTooOld(err error) *mvcc.WriteTooOldError {
	var too *mvcc.WriteTooOldError
	if errors.As(err, &too) {
		return too
	}
	return nil
}

// pushPastCachedRead bumps tc.txn.WriteTimestamp past any cached read on key.
// A no-op if there is no cache (tests) or the cache already sits below our
// current WriteTimestamp. See blog 15 for the underlying anomaly.
func (tc *TxnCoordSender) pushPastCachedRead(key []byte) {
	if tc.tscache == nil {
		return
	}
	cached := tc.tscache.GetMax(key)
	if tc.txn.WriteTimestamp.LessEq(cached) {
		tc.bumpWriteTimestampTo(cached.Next())
	}
}

// bumpWriteTimestampTo advances WriteTimestamp if ts is strictly greater than
// the current value. The transaction record is not persisted here — callers
// persist via writeTxnRecord after the surrounding MVCC operation succeeds.
func (tc *TxnCoordSender) bumpWriteTimestampTo(ts hlc.Timestamp) {
	if tc.txn.WriteTimestamp.Less(ts) {
		tc.txn.WriteTimestamp = ts
	}
}

// handleReadIntent is the read-side counterpart to resolveForeignIntent.
// When a read encounters an intent from another transaction, four outcomes
// are possible:
//
//   - Blocker COMMITTED: finalize its intent as committed, retry the read.
//   - Blocker ABORTED / missing: finalize as aborted, retry.
//   - Blocker PENDING, push is safe for them: push the blocker's
//     WriteTimestamp and intent timestamp past our ReadTimestamp so the
//     intent is "in the future" for us. The blocker commits at the pushed
//     timestamp (SI), or if the push doesn't cross their ReadTimestamp, no
//     restart is triggered either way.
//   - Blocker PENDING, push would force an SSI restart: priority fight.
//     Higher-priority side wins — the reader either aborts the blocker and
//     pushes, or restarts itself with a bumped priority suggestion.
//
// The last case is what distinguishes SSI from SI for reads. Under SI, any
// blocker is happy to be pushed. Under SSI, a pushed writer whose
// WriteTimestamp moves past its ReadTimestamp must restart — so a reader
// pushing an SSI writer effectively aborts them. Priority arbitration
// prevents arbitrary readers from killing long-running SSI writers.
func (tc *TxnCoordSender) handleReadIntent(intentErr *mvcc.WriteIntentError) (retry bool, err error) {
	blocker, found, err := LoadTxnRecord(tc.engine, intentErr.TxnID)
	if err != nil {
		return false, fmt.Errorf("txn: load blocker record: %w", err)
	}
	if !found || blocker.Status == TxnAborted {
		return true, tc.finalizeForeignIntent(intentErr, mvcc.TxnAborted, hlc.Timestamp{})
	}
	if blocker.Status == TxnCommitted {
		return true, tc.finalizeForeignIntent(intentErr, mvcc.TxnCommitted, blocker.WriteTimestamp)
	}

	// PENDING. Compute where we'd need to push them.
	pushTo := tc.txn.ReadTimestamp.Next()

	// Would pushing to this timestamp force the blocker to restart under SSI?
	// Only true when (a) they are SSI, and (b) the push moves their
	// WriteTimestamp past their ReadTimestamp.
	pushWouldForceRestart := blocker.Isolation == SSI &&
		blocker.ReadTimestamp.Less(pushTo)

	if pushWouldForceRestart {
		// Priority fight, reader vs. SSI writer.
		if tc.txn.Priority > blocker.Priority {
			if err := tc.abortBlocker(blocker); err != nil {
				return false, err
			}
			return true, tc.finalizeForeignIntent(intentErr, mvcc.TxnAborted, hlc.Timestamp{})
		}
		// We lose. Return a retry error so the caller restarts us with a
		// priority high enough to beat the blocker next time.
		return false, &TxnRetryError{
			TxnID:           tc.txn.ID,
			Reason:          fmt.Sprintf("read-write conflict with SSI txn %x (priority %d ≤ %d)", intentErr.TxnID[:4], tc.txn.Priority, blocker.Priority),
			SuggestedMinPri: blocker.Priority + 1,
		}
	}

	// Safe push: either the blocker is SI (pushed commit is fine) or the
	// push doesn't cross their ReadTimestamp (no SSI restart triggered).
	if blocker.WriteTimestamp.Less(pushTo) {
		blocker.WriteTimestamp = pushTo
		blocker.LastHeartbeat = tc.clock.Now()
		data, encErr := blocker.Encode()
		if encErr != nil {
			return false, fmt.Errorf("txn: encode pushed blocker: %w", encErr)
		}
		if err := mvcc.MVCCPut(tc.engine, TxnRecordKey(blocker.ID), hlc.Timestamp{}, data, nil); err != nil {
			return false, fmt.Errorf("txn: persist pushed blocker record: %w", err)
		}
	}
	if err := mvcc.MVCCPushIntent(tc.engine, intentErr.Key, intentErr.TxnID, pushTo); err != nil {
		return false, fmt.Errorf("txn: push intent: %w", err)
	}
	return true, nil
}

// Compile-time assertions that the error types satisfy error.
var (
	_ error = (*TxnRetryError)(nil)
	_ error = (*mvcc.WriteIntentError)(nil)
)
