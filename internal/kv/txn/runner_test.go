package txn

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/kv/tscache"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// --- Happy path: single attempt --------------------------------------------

func TestRunTransaction_SuccessFirstTry(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	err := RunTransaction(engine, clock, RunOptions{Isolation: SSI},
		func(tc *TxnCoordSender) error {
			return tc.Put([]byte("k"), []byte("v"))
		})
	if err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}

	// The key is visible at a post-commit timestamp.
	got, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 1_000_000_000_000}, mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get = %q, want v", got)
	}
}

// --- Retry: closure returns a retry error ----------------------------------

func TestRunTransaction_RetriesOnClosureRetryError(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	attempts := 0
	err := RunTransaction(engine, clock, RunOptions{Isolation: SSI},
		func(tc *TxnCoordSender) error {
			attempts++
			if attempts < 3 {
				return &TxnRetryError{
					TxnID:           tc.ID(),
					Reason:          "synthetic",
					SuggestedMinPri: 100,
				}
			}
			return tc.Put([]byte("k"), []byte("v"))
		})
	if err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

// --- Priority floor advances on each retry ---------------------------------

func TestRunTransaction_PriorityBumpedOnRetry(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	var priorities []int32
	_ = RunTransaction(engine, clock, RunOptions{Isolation: SSI},
		func(tc *TxnCoordSender) error {
			priorities = append(priorities, tc.txn.Priority)
			if len(priorities) < 4 {
				return &TxnRetryError{
					TxnID:           tc.ID(),
					Reason:          "test",
					SuggestedMinPri: priorities[len(priorities)-1] + 50,
				}
			}
			return nil
		})

	if len(priorities) != 4 {
		t.Fatalf("priorities seen = %d, want 4", len(priorities))
	}
	for i := 1; i < len(priorities); i++ {
		if priorities[i] <= priorities[i-1] {
			t.Fatalf("priority did not advance: %v", priorities)
		}
	}
}

// --- Non-retry errors from closure are permanent ---------------------------

var errBusinessLogic = errors.New("business: insufficient funds")

func TestRunTransaction_NonRetryErrorIsPermanent(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	attempts := 0
	err := RunTransaction(engine, clock, RunOptions{Isolation: SSI},
		func(tc *TxnCoordSender) error {
			attempts++
			return errBusinessLogic
		})

	if !errors.Is(err, errBusinessLogic) {
		t.Fatalf("err = %v, want errBusinessLogic", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1 (no retry for non-retry errors)", attempts)
	}
}

// --- Non-retry errors from closure cause Abort (intent cleanup) ------------

func TestRunTransaction_NonRetryErrorCleansUpIntents(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Prior committed value so we can detect that our intent was cleaned up
	// (not left behind).
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("prior"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_ = RunTransaction(engine, clock, RunOptions{Isolation: SSI},
		func(tc *TxnCoordSender) error {
			if err := tc.Put([]byte("k"), []byte("doomed")); err != nil {
				t.Fatalf("Put: %v", err)
			}
			return errBusinessLogic
		})

	// Abort reverted the metadata to point at the prior version.
	got, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 1_000_000_000_000}, mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(got, []byte("prior")) {
		t.Fatalf("Get = %q, want 'prior' (aborted intent should revert to prior version)", got)
	}
}

// --- Retry budget exhausted ------------------------------------------------

func TestRunTransaction_ExhaustsRetryBudget(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	attempts := 0
	err := RunTransaction(engine, clock, RunOptions{Isolation: SSI, MaxRetries: 3},
		func(tc *TxnCoordSender) error {
			attempts++
			return &TxnRetryError{
				TxnID:           tc.ID(),
				Reason:          "forever",
				SuggestedMinPri: 1,
			}
		})

	if !errors.Is(err, ErrTooManyRetries) {
		t.Fatalf("err = %v, want ErrTooManyRetries", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

// --- Commit-time retry (SSI pushed commit) retries transparently ----------

func TestRunTransaction_RetriesOnSSIPushedCommit(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Seed a committed version far in the future. The first attempt's Put
	// will bump WriteTimestamp past it; at commit SSI refuses and returns
	// a retry error. The wrapper restarts; the second attempt's writeTS is
	// already above the existing version, so it commits cleanly.
	future := hlc.Timestamp{WallTime: 9_000_000_000}
	if err := mvcc.MVCCPut(engine, []byte("k"), future, []byte("future"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	attempts := 0
	err := RunTransaction(engine, clock, RunOptions{Isolation: SSI},
		func(tc *TxnCoordSender) error {
			attempts++
			return tc.Put([]byte("k"), []byte("ours"))
		})

	// Without restart bookkeeping (epoch), our second attempt starts with a
	// fresh ReadTimestamp from clock.Now(). If the clock has advanced past
	// the future seed, the second attempt's Put will succeed at its own
	// read timestamp without a push. Otherwise it hits the same too-old
	// condition and retries again.
	if err != nil && !errors.Is(err, ErrTooManyRetries) {
		t.Fatalf("RunTransaction: %v", err)
	}
	if attempts < 2 {
		t.Fatalf("expected ≥ 2 attempts (first hit SSI push), got %d", attempts)
	}
}

// --- New TxnID on each retry ------------------------------------------------

func TestRunTransaction_NewTxnIDOnRestart(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	seen := make(map[mvcc.TxnID]struct{})
	_ = RunTransaction(engine, clock, RunOptions{Isolation: SSI, MaxRetries: 5},
		func(tc *TxnCoordSender) error {
			if _, dup := seen[tc.ID()]; dup {
				t.Fatalf("TxnID reused: %x", tc.ID())
			}
			seen[tc.ID()] = struct{}{}
			if len(seen) < 3 {
				return &TxnRetryError{TxnID: tc.ID(), Reason: "t"}
			}
			return nil
		})
	if len(seen) != 3 {
		t.Fatalf("seen = %d, want 3", len(seen))
	}
}

// --- End-to-end: two SSI transactions contend, one survives via RunTransaction

func TestRunTransaction_SSIConflict_Resolves(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)
	cache := tscache.New(100)

	// Seed key.
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("0"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Outer: hold a high-priority intent that blocks inner until we finish.
	outer, err := Begin(engine, clock, cache, SSI)
	if err != nil {
		t.Fatalf("Begin outer: %v", err)
	}
	forcePriority(outer, 10) // lower
	if err := outer.writeTxnRecord(); err != nil {
		t.Fatalf("persist outer: %v", err)
	}
	if err := outer.Put([]byte("k"), []byte("outer")); err != nil {
		t.Fatalf("outer Put: %v", err)
	}

	// Inner runs via RunTransaction with high priority. It should abort the
	// outer via the write-write priority fight and commit successfully.
	var innerAttempts int
	innerErr := RunTransaction(engine, clock, RunOptions{Isolation: SSI, Cache: cache},
		func(tc *TxnCoordSender) error {
			innerAttempts++
			forcePriority(tc, 1000)
			_ = tc.writeTxnRecord()
			return tc.Put([]byte("k"), []byte(fmt.Sprintf("inner-%d", innerAttempts)))
		})
	if innerErr != nil {
		t.Fatalf("inner RunTransaction: %v", innerErr)
	}

	// The outer's record is ABORTED; its subsequent Commit would return retry.
	outerRec, _, _ := LoadTxnRecord(engine, outer.ID())
	if outerRec.Status != TxnAborted {
		t.Fatalf("outer status = %v, want ABORTED", outerRec.Status)
	}
}
