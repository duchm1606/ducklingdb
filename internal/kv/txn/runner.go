package txn

import (
	"errors"
	"fmt"

	"github.com/duchm1606/ducklingdb/internal/kv/tscache"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// defaultMaxRetries bounds RunTransaction's retry loop. A healthy workload
// typically commits in 1–3 attempts; hitting this limit implies either
// pathological contention on a hot key or a bug in retry logic. Surfacing
// a concrete error beats looping forever.
const defaultMaxRetries = 50

// ErrTooManyRetries is returned when RunTransaction exhausts its retry
// budget. The wrapped cause is the last TxnRetryError observed.
var ErrTooManyRetries = errors.New("txn: retry budget exhausted")

// TxnFunc is the user-supplied closure that describes a transaction's work.
// It is invoked with a fresh *TxnCoordSender on each attempt. The closure
// must not retain references to the coordinator after it returns — a new
// coordinator (with a new TxnID) is allocated on every retry.
//
// Return values:
//   - nil → the wrapper calls Commit; if that returns nil, the transaction
//     succeeded and RunTransaction returns nil.
//   - TxnRetryError → retry the closure with a bumped priority.
//   - any other error → permanent failure. The wrapper aborts the txn and
//     returns the error to the caller without retrying.
type TxnFunc func(tc *TxnCoordSender) error

// RunOptions tunes RunTransaction's behavior. The zero value is a valid
// configuration: SSI isolation, no cache, default retry budget.
type RunOptions struct {
	Isolation  IsolationLevel
	Cache      *tscache.Cache
	MaxRetries int // 0 → defaultMaxRetries
}

// RunTransaction executes fn inside a transaction, transparently retrying
// on TxnRetryError. This is the idiomatic user-facing API: callers write
// the transaction's logic once and the wrapper handles retries.
//
// Retry flow on each iteration:
//  1. Allocate a fresh coordinator via Begin. If priority hints accumulated
//     from prior attempts, apply them to the new transaction record.
//  2. Call fn. If it returns nil, call Commit. If it returns a TxnRetryError
//     (or Commit returns one), fall through to the retry step.
//  3. On retry: bump priority to max(SuggestedMinPri, oldPriority + 1),
//     loop.
//  4. On non-retry errors: Abort (best-effort) and return the error.
//
// Every retry uses a new TxnID. The previous attempt's intents, if any,
// will be cleaned up lazily when other transactions encounter them
// (Step 4's foreign-intent resolution).
func RunTransaction(
	engine storage.Engine,
	clock *hlc.Clock,
	opts RunOptions,
	fn TxnFunc,
) error {
	maxRetries := opts.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultMaxRetries
	}

	var (
		nextPriorityFloor int32
		lastRetry         *TxnRetryError
	)

	for attempt := 0; attempt < maxRetries; attempt++ {
		tc, err := Begin(engine, clock, opts.Cache, opts.Isolation)
		if err != nil {
			return fmt.Errorf("txn: begin attempt %d: %w", attempt, err)
		}
		if nextPriorityFloor > 0 {
			applyPriorityFloor(tc, nextPriorityFloor)
		}

		// Phase 1: run the user's closure.
		fnErr := fn(tc)

		// Phase 2a: uncertainty-window restart.
		// Advance the clock past the witnessed uncertain timestamp so the
		// next attempt's fresh ReadTimestamp sits above the window.
		if unc := extractUncertaintyError(fnErr); unc != nil {
			clock.Update(unc.ExistingTimestamp)
			_ = tc.Abort()
			continue
		}

		// Phase 2b: retry-error handling.
		if retry := extractRetryError(fnErr); retry != nil {
			nextPriorityFloor = bumpPriorityFloor(tc.txn.Priority, retry.SuggestedMinPri)
			lastRetry = retry
			// Advance the shared clock past our (possibly pushed) WriteTimestamp
			// so the next attempt's Begin sees a ReadTimestamp above any
			// committed version that caused the push. Without this, retries can
			// loop seeing the same WriteTooOld conditions under contention.
			clock.Update(tc.txn.WriteTimestamp)
			_ = tc.Abort()
			continue
		}
		// Non-retry error from the closure → permanent failure.
		if fnErr != nil {
			_ = tc.Abort()
			return fnErr
		}

		// Phase 3: closure succeeded, commit it.
		commitErr := tc.Commit()
		if commitErr == nil {
			return nil
		}
		if retry := extractRetryError(commitErr); retry != nil {
			nextPriorityFloor = bumpPriorityFloor(tc.txn.Priority, retry.SuggestedMinPri)
			lastRetry = retry
			// See comment above: advance the clock past our WriteTimestamp so
			// the next Begin picks a ReadTimestamp that doesn't immediately
			// re-hit the same push.
			clock.Update(tc.txn.WriteTimestamp)
			_ = tc.Abort()
			continue
		}
		// Non-retry commit error → permanent failure.
		_ = tc.Abort()
		return commitErr
	}

	if lastRetry != nil {
		return fmt.Errorf("%w after %d attempts: %w", ErrTooManyRetries, maxRetries, lastRetry)
	}
	return fmt.Errorf("%w after %d attempts", ErrTooManyRetries, maxRetries)
}

// extractRetryError pulls a *TxnRetryError from an error chain, or returns
// nil if the error is not (or does not wrap) a retry signal.
func extractRetryError(err error) *TxnRetryError {
	var retry *TxnRetryError
	if errors.As(err, &retry) {
		return retry
	}
	return nil
}

// extractUncertaintyError pulls a *UncertaintyError from an error chain.
func extractUncertaintyError(err error) *mvcc.UncertaintyError {
	var unc *mvcc.UncertaintyError
	if errors.As(err, &unc) {
		return unc
	}
	return nil
}

// bumpPriorityFloor returns the priority the next attempt should start with.
// The floor is the maximum of the blocker's suggestion and our own current
// priority + 1 — ensuring at least a monotonic advance even when the
// suggestion is lower than our current value.
func bumpPriorityFloor(currentPriority, suggested int32) int32 {
	bump := currentPriority + 1
	if suggested > bump {
		return suggested
	}
	return bump
}

// applyPriorityFloor sets the coordinator's priority to at least floor and
// persists the updated record. Called from RunTransaction at the start of
// each retry iteration so conflicts resolved by priority see the bumped
// value on their first LoadTxnRecord.
func applyPriorityFloor(tc *TxnCoordSender, floor int32) {
	if tc.txn.Priority < floor {
		tc.txn.Priority = floor
		_ = tc.writeTxnRecord()
	}
}
