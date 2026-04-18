package txn

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// TestMVCCGet_Uncertainty_ValueInWindow verifies the MVCC-level check:
// a committed value with timestamp in (readTS, maxTS] surfaces an
// UncertaintyError.
func TestMVCCGet_Uncertainty_ValueInWindow(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)

	// Committed value at t=150.
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 150}, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Read at 100 with max=200. The committed 150 sits in the window
	// (100, 200], so we can't tell if it happened before our read or after.
	_, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 100},
		mvcc.ReadOptions{MaxTimestamp: hlc.Timestamp{WallTime: 200}})

	var unc *mvcc.UncertaintyError
	if !errors.As(err, &unc) {
		t.Fatalf("expected *UncertaintyError, got %T: %v", err, err)
	}
	if unc.ExistingTimestamp != (hlc.Timestamp{WallTime: 150}) {
		t.Fatalf("ExistingTimestamp = %v, want 150", unc.ExistingTimestamp)
	}
	if !errors.Is(err, mvcc.ErrReadUncertainty) {
		t.Fatal("errors.Is(err, ErrReadUncertainty) should match")
	}
}

// TestMVCCGet_Uncertainty_NoMaxTimestamp_NoCheck shows that when the reader
// doesn't supply a MaxTimestamp (zero), the uncertainty check is skipped
// entirely — this is the single-node / non-transactional default.
func TestMVCCGet_Uncertainty_NoMaxTimestamp_NoCheck(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 150}, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 100},
		mvcc.ReadOptions{}) // no MaxTimestamp

	// At read=100, existing=150, the version is simply invisible (too new)
	// and MVCC returns ErrKeyNotFound. Uncertainty check does not fire.
	if !errors.Is(err, mvcc.ErrReadUncertainty) == false {
		// test fine either way — just check we didn't get UncertaintyError
	}
	var unc *mvcc.UncertaintyError
	if errors.As(err, &unc) {
		t.Fatalf("should not fire uncertainty check without MaxTimestamp: %v", err)
	}
}

// TestMVCCGet_Uncertainty_ValueAboveMax_NoCheck: values strictly above
// MaxTimestamp are "far future" and not in the uncertainty window.
func TestMVCCGet_Uncertainty_ValueAboveMax_NoCheck(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	// Value at 500 — way above the uncertainty window ceiling.
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 100},
		mvcc.ReadOptions{MaxTimestamp: hlc.Timestamp{WallTime: 200}})

	var unc *mvcc.UncertaintyError
	if errors.As(err, &unc) {
		t.Fatalf("value above MaxTimestamp should not be uncertain: %v", err)
	}
}

// TestMVCCGet_Uncertainty_OwnIntent_NoCheck: our own intent in the window
// must not trigger uncertainty — we always see our own writes.
func TestMVCCGet_Uncertainty_OwnIntent_NoCheck(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	txn := mvcc.TxnID{1}

	// Our own intent at t=150, in the uncertainty window of a read at 100.
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 150}, []byte("v"), &txn); err != nil {
		t.Fatalf("seed: %v", err)
	}

	_, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 100},
		mvcc.ReadOptions{
			Txn:          &txn,
			MaxTimestamp: hlc.Timestamp{WallTime: 200},
		})

	var unc *mvcc.UncertaintyError
	if errors.As(err, &unc) {
		t.Fatalf("own intent should not trigger uncertainty: %v", err)
	}
}

// TestRunTransaction_RetriesOnUncertainty: the wrapper catches an
// UncertaintyError, advances the clock past the uncertain timestamp, and
// retries. After the retry, the read's fresh ReadTimestamp is above the
// previously-uncertain value and the check no longer fires.
func TestRunTransaction_RetriesOnUncertainty(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	// Manual clock so we can observe the clock.Update() effect.
	manual := hlc.NewManualClock(1_000_000_000) // 1s
	clock := hlc.NewClock(manual, 250*time.Millisecond)

	// Seed a committed value that is slightly in the future — within the
	// uncertainty window of a read at 1s (max = 1s + 250ms = 1.25s).
	ts := hlc.Timestamp{WallTime: 1_100_000_000} // 1.1s
	if err := mvcc.MVCCPut(engine, []byte("k"), ts, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	attempts := 0
	err := RunTransaction(engine, clock, RunOptions{Isolation: SSI},
		func(tc *TxnCoordSender) error {
			attempts++
			_, err := tc.Get([]byte("k"))
			return err
		})

	if err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}
	if attempts < 2 {
		t.Fatalf("expected ≥ 2 attempts (first hit uncertainty), got %d", attempts)
	}
	// After retry, Read succeeded — meaning ReadTimestamp advanced past the
	// uncertain value. We can sanity-check by reading the value directly.
	got, err := mvcc.MVCCGet(engine, []byte("k"),
		hlc.Timestamp{WallTime: 2_000_000_000}, mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("sanity Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("Get = %q, want v", got)
	}
}

// TestRunTransaction_Uncertainty_ClockAdvancesPastExisting: verify the
// clock.Update hook actually moves the clock past the uncertain timestamp.
func TestRunTransaction_Uncertainty_ClockAdvancesPastExisting(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	manual := hlc.NewManualClock(1_000_000_000)
	clock := hlc.NewClock(manual, 250*time.Millisecond)

	uncertainTS := hlc.Timestamp{WallTime: 1_150_000_000}
	if err := mvcc.MVCCPut(engine, []byte("k"), uncertainTS, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Record observed ReadTimestamps across attempts.
	var reads []hlc.Timestamp
	err := RunTransaction(engine, clock, RunOptions{Isolation: SSI},
		func(tc *TxnCoordSender) error {
			reads = append(reads, tc.ReadTimestamp())
			_, err := tc.Get([]byte("k"))
			return err
		})
	if err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}

	// First attempt should have ReadTimestamp ≤ uncertainTS (the manual clock
	// was at 1s; ReadTimestamp = 1s).
	// Second attempt should have ReadTimestamp > uncertainTS (clock advanced
	// via clock.Update).
	if len(reads) < 2 {
		t.Fatalf("expected ≥ 2 attempts, got %d reads: %v", len(reads), reads)
	}
	if !uncertainTS.Less(reads[1]) {
		t.Fatalf("second attempt ReadTimestamp %v should be > uncertainTS %v",
			reads[1], uncertainTS)
	}
}

// TestCoordinator_Get_PropagatesUncertainty: the coordinator's Get does not
// swallow UncertaintyError; it must bubble up so RunTransaction (or the
// caller) can handle it.
func TestCoordinator_Get_PropagatesUncertainty(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Seed a value in the uncertainty window of a coordinator started via
	// testClock (1s) with maxOffset 250ms — so window = (1s, 1.25s].
	ts := hlc.Timestamp{WallTime: 1_100_000_000}
	if err := mvcc.MVCCPut(engine, []byte("k"), ts, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tc, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	_, err = tc.Get([]byte("k"))
	var unc *mvcc.UncertaintyError
	if !errors.As(err, &unc) {
		t.Fatalf("expected UncertaintyError from coordinator Get, got %T: %v", err, err)
	}
}
