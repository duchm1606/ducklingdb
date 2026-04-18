package txn

import (
	"bytes"
	"errors"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// forcePriority overrides a coordinator's priority — useful for deterministic
// conflict tests that need to stage "winner" and "loser" explicitly. Relies on
// the test being in the same package (internal field access).
func forcePriority(tc *TxnCoordSender, p int32) {
	tc.txn.Priority = p
}

// TestConflict_AbortedBlocker_Retries checks that if a foreign intent belongs
// to an ABORTED transaction, the coordinator cleans up the orphan intent and
// proceeds with its own write — no retry error surfaces.
func TestConflict_AbortedBlocker_Retries(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Blocker: begin, write an intent, abort.
	blocker, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin blocker: %v", err)
	}
	if err := blocker.Put([]byte("k"), []byte("blocker-value")); err != nil {
		t.Fatalf("blocker Put: %v", err)
	}

	// Instead of a clean Abort() (which would also resolve the intent),
	// simulate the coordinator dying after flipping the record but before
	// cleaning up. The leftover intent is what we want to exercise.
	rec, _, _ := LoadTxnRecord(engine, blocker.ID())
	rec.Status = TxnAborted
	data, _ := rec.Encode()
	if err := mvcc.MVCCPut(engine, TxnRecordKey(blocker.ID()), hlc.Timestamp{}, data, nil); err != nil {
		t.Fatalf("flip blocker record: %v", err)
	}

	// Writer: sees the intent, reads the ABORTED record, cleans up, proceeds.
	writer, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin writer: %v", err)
	}
	if err := writer.Put([]byte("k"), []byte("writer-value")); err != nil {
		t.Fatalf("writer Put should succeed after finalizing aborted blocker: %v", err)
	}

	// Verify the committed value is ours, not the blocker's.
	if err := writer.Commit(); err != nil {
		t.Fatalf("writer Commit: %v", err)
	}
	got, err := mvcc.MVCCGet(engine, []byte("k"), writer.WriteTimestamp().Next(), mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("writer-value")) {
		t.Fatalf("committed value = %q, want %q", got, []byte("writer-value"))
	}
}

// TestConflict_MissingBlockerRecord_Retries tests the edge case where the
// blocker's record does not exist at all (coordinator died before writing it,
// or the record was GC'd). We treat this as aborted and clean up.
func TestConflict_MissingBlockerRecord_Retries(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Manually craft a stray intent owned by a TxnID that has no record.
	strayID := mvcc.TxnID{0xFE, 0xED}
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("stray"), &strayID); err != nil {
		t.Fatalf("seed stray intent: %v", err)
	}

	writer, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin writer: %v", err)
	}
	if err := writer.Put([]byte("k"), []byte("writer-value")); err != nil {
		t.Fatalf("writer Put should clean up orphan intent and proceed: %v", err)
	}
}

// TestConflict_HigherPriorityWins aborts the lower-priority blocker.
func TestConflict_HigherPriorityWins(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	loser, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin loser: %v", err)
	}
	forcePriority(loser, 10)
	if err := loser.writeTxnRecord(); err != nil {
		t.Fatalf("persist loser priority: %v", err)
	}
	if err := loser.Put([]byte("k"), []byte("loser-value")); err != nil {
		t.Fatalf("loser Put: %v", err)
	}

	winner, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin winner: %v", err)
	}
	forcePriority(winner, 100)
	if err := winner.writeTxnRecord(); err != nil {
		t.Fatalf("persist winner priority: %v", err)
	}

	if err := winner.Put([]byte("k"), []byte("winner-value")); err != nil {
		t.Fatalf("winner Put: %v", err)
	}

	// The loser's record must be flipped to ABORTED.
	rec, ok, err := LoadTxnRecord(engine, loser.ID())
	if err != nil || !ok {
		t.Fatalf("LoadTxnRecord(loser): ok=%v err=%v", ok, err)
	}
	if rec.Status != TxnAborted {
		t.Fatalf("loser status = %v, want ABORTED", rec.Status)
	}

	// The winner can commit and the final value is theirs.
	if err := winner.Commit(); err != nil {
		t.Fatalf("winner Commit: %v", err)
	}
	got, err := mvcc.MVCCGet(engine, []byte("k"), winner.WriteTimestamp().Next(), mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("winner-value")) {
		t.Fatalf("committed value = %q, want %q", got, []byte("winner-value"))
	}
}

// TestConflict_LowerPriorityReturnsRetryError shows the opposite direction:
// the current txn has lower priority and must restart.
func TestConflict_LowerPriorityReturnsRetryError(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	holder, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin holder: %v", err)
	}
	forcePriority(holder, 1000)
	if err := holder.writeTxnRecord(); err != nil {
		t.Fatalf("persist holder priority: %v", err)
	}
	if err := holder.Put([]byte("k"), []byte("holder")); err != nil {
		t.Fatalf("holder Put: %v", err)
	}

	underdog, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin underdog: %v", err)
	}
	forcePriority(underdog, 10)
	if err := underdog.writeTxnRecord(); err != nil {
		t.Fatalf("persist underdog priority: %v", err)
	}

	err = underdog.Put([]byte("k"), []byte("underdog"))
	if err == nil {
		t.Fatal("underdog Put unexpectedly succeeded")
	}

	var retry *TxnRetryError
	if !errors.As(err, &retry) {
		t.Fatalf("expected *TxnRetryError, got %T: %v", err, err)
	}
	if retry.TxnID != underdog.ID() {
		t.Fatalf("retry.TxnID = %x, want %x", retry.TxnID, underdog.ID())
	}
	if retry.SuggestedMinPri <= 1000 {
		t.Fatalf("SuggestedMinPri = %d, want > 1000 (bump past blocker)", retry.SuggestedMinPri)
	}

	// The holder's intent must still be intact — we didn't abort a higher-
	// priority transaction.
	hrec, ok, _ := LoadTxnRecord(engine, holder.ID())
	if !ok || hrec.Status != TxnPending {
		t.Fatalf("holder record = %+v, want PENDING", hrec)
	}
}

// TestConflict_EqualPriorityCurrentLoses: on a tie the current transaction
// backs off, preventing mutual-abort deadlock.
func TestConflict_EqualPriorityCurrentLoses(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	holder, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin holder: %v", err)
	}
	forcePriority(holder, 500)
	if err := holder.writeTxnRecord(); err != nil {
		t.Fatalf("persist holder priority: %v", err)
	}
	if err := holder.Put([]byte("k"), []byte("h")); err != nil {
		t.Fatalf("holder Put: %v", err)
	}

	tie, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin tie: %v", err)
	}
	forcePriority(tie, 500)
	if err := tie.writeTxnRecord(); err != nil {
		t.Fatalf("persist tie priority: %v", err)
	}

	err = tie.Put([]byte("k"), []byte("t"))
	var retry *TxnRetryError
	if !errors.As(err, &retry) {
		t.Fatalf("expected *TxnRetryError on tie, got %v", err)
	}
}

// TestWriteIntentError_CarriesTxnID verifies the MVCC layer's upgrade: the
// error now carries the blocker's TxnID so the conflict layer can look up
// the record.
func TestWriteIntentError_CarriesTxnID(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)

	txn1 := mvcc.TxnID{0x11, 0x22}
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v"), &txn1); err != nil {
		t.Fatalf("seed intent: %v", err)
	}

	txn2 := mvcc.TxnID{0x99}
	err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 200}, []byte("v2"), &txn2)
	if err == nil {
		t.Fatal("expected write-intent error")
	}

	var wi *mvcc.WriteIntentError
	if !errors.As(err, &wi) {
		t.Fatalf("expected *WriteIntentError, got %T: %v", err, err)
	}
	if wi.TxnID != txn1 {
		t.Fatalf("wi.TxnID = %x, want %x", wi.TxnID, txn1)
	}
	if wi.Timestamp != (hlc.Timestamp{WallTime: 100}) {
		t.Fatalf("wi.Timestamp = %v, want 100", wi.Timestamp)
	}
	// Legacy errors.Is still matches the sentinel.
	if !errors.Is(err, mvcc.ErrWriteIntent) {
		t.Fatal("errors.Is(err, ErrWriteIntent) should still match")
	}
}
