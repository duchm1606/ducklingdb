package txn

import (
	"bytes"
	"errors"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// --- SI: pushed commits are accepted -----------------------------------------

// TestSI_CommitsAtPushedTimestamp verifies the core SI contract: if
// WriteTimestamp has been pushed past ReadTimestamp (by the timestamp cache,
// a reader, or a committed-too-old conflict), SI commits anyway at the
// pushed timestamp.
func TestSI_CommitsAtPushedTimestamp(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Seed a committed version far in the future. Any write below this will
	// trigger the "write-too-old" push from Step 5.
	future := hlc.Timestamp{WallTime: 9_000_000_000}
	if err := mvcc.MVCCPut(engine, []byte("k"), future, []byte("future"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tc, err := Begin(engine, clock, nil, SI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	originalReadTS := tc.ReadTimestamp()

	if err := tc.Put([]byte("k"), []byte("ours")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// WriteTimestamp was pushed past the seeded committed version.
	if !future.Less(tc.WriteTimestamp()) {
		t.Fatalf("WriteTimestamp %v not pushed past future %v", tc.WriteTimestamp(), future)
	}
	// ReadTimestamp is unchanged.
	if tc.ReadTimestamp() != originalReadTS {
		t.Fatalf("ReadTimestamp changed: was %v, now %v", originalReadTS, tc.ReadTimestamp())
	}

	// SI accepts the pushed WriteTimestamp and commits.
	if err := tc.Commit(); err != nil {
		t.Fatalf("SI Commit with pushed WriteTimestamp should succeed: %v", err)
	}

	// The committed value is readable at the pushed timestamp.
	got, err := mvcc.MVCCGet(engine, []byte("k"), tc.WriteTimestamp().Next(), mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("ours")) {
		t.Fatalf("committed value = %q, want 'ours'", got)
	}
}

// --- SSI: pushed commits trigger a retry error -------------------------------

// TestSSI_RefusesPushedCommit verifies the complement: under SSI, the same
// push scenario forces a restart.
func TestSSI_RefusesPushedCommit(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	future := hlc.Timestamp{WallTime: 9_000_000_000}
	if err := mvcc.MVCCPut(engine, []byte("k"), future, []byte("future"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tc, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tc.Put([]byte("k"), []byte("ours")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if !tc.ReadTimestamp().Less(tc.WriteTimestamp()) {
		t.Fatalf("test setup: expected push to happen (read %v, write %v)",
			tc.ReadTimestamp(), tc.WriteTimestamp())
	}

	err = tc.Commit()
	var retry *TxnRetryError
	if !errors.As(err, &retry) {
		t.Fatalf("SSI commit after push: got %v, want *TxnRetryError", err)
	}
	if retry.TxnID != tc.ID() {
		t.Fatalf("retry.TxnID mismatch: %x vs %x", retry.TxnID, tc.ID())
	}
}

// --- Re-read record: pick up a push from another txn -------------------------

// TestCommit_PicksUpExternalPush verifies that a push applied by another
// transaction (via Step 5's reader-push path) is observed at commit time.
// The coordinator's in-memory WriteTimestamp becomes stale; refreshRecord
// pulls in the authoritative value.
func TestCommit_PicksUpExternalPush(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Writer starts low, writes an intent.
	writer, err := Begin(engine, clock, nil, SI)
	if err != nil {
		t.Fatalf("Begin writer: %v", err)
	}
	writer.txn.ReadTimestamp = hlc.Timestamp{WallTime: 500}
	writer.txn.WriteTimestamp = hlc.Timestamp{WallTime: 500}
	if err := writer.writeTxnRecord(); err != nil {
		t.Fatalf("persist writer: %v", err)
	}
	if err := writer.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("writer Put: %v", err)
	}

	// Simulate an external push by overwriting the persisted record with a
	// bumped WriteTimestamp — what a reader would do via handleReadIntent.
	rec, _, _ := LoadTxnRecord(engine, writer.ID())
	rec.WriteTimestamp = hlc.Timestamp{WallTime: 5000}
	data, _ := rec.Encode()
	_ = mvcc.MVCCPut(engine, TxnRecordKey(writer.ID()), hlc.Timestamp{}, data, nil)

	// Writer's in-memory WriteTimestamp is still 500 — stale.
	if writer.WriteTimestamp() != (hlc.Timestamp{WallTime: 500}) {
		t.Fatal("test setup: in-memory WriteTimestamp should still be 500")
	}

	// Commit must refresh, observe 5000, and commit at 5000.
	if err := writer.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if writer.WriteTimestamp() != (hlc.Timestamp{WallTime: 5000}) {
		t.Fatalf("post-commit WriteTimestamp = %v, want 5000", writer.WriteTimestamp())
	}

	// And the committed value lives at the pushed timestamp, not the original.
	got, err := mvcc.MVCCGet(engine, []byte("k"), (hlc.Timestamp{WallTime: 5000}).Next(), mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("post-commit Get: %v", err)
	}
	if !bytes.Equal(got, []byte("v")) {
		t.Fatalf("value = %q, want v", got)
	}
	// Reading at the original timestamp must NOT see our value — it was
	// committed at the pushed timestamp, not the original.
	_, err = mvcc.MVCCGet(engine, []byte("k"), hlc.Timestamp{WallTime: 1000}, mvcc.ReadOptions{})
	if err == nil {
		t.Fatal("read at 1000 should not see value committed at 5000")
	}
}

// --- Commit after external abort returns retry ------------------------------

// TestCommit_DetectsExternalAbort verifies that when another transaction
// has flipped our record to ABORTED (won a priority fight), our Commit
// returns a TxnRetryError instead of silently committing.
func TestCommit_DetectsExternalAbort(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	tc, err := Begin(engine, clock, nil, SI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tc.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// External actor flips our record to ABORTED.
	rec, _, _ := LoadTxnRecord(engine, tc.ID())
	rec.Status = TxnAborted
	data, _ := rec.Encode()
	_ = mvcc.MVCCPut(engine, TxnRecordKey(tc.ID()), hlc.Timestamp{}, data, nil)

	err = tc.Commit()
	var retry *TxnRetryError
	if !errors.As(err, &retry) {
		t.Fatalf("Commit after external abort: got %v, want *TxnRetryError", err)
	}
}

// --- Write skew is permitted under SI ---------------------------------------

// TestSI_AllowsWriteSkew reproduces the canonical write-skew anomaly to
// show that SI permits it. Two concurrent transactions read overlapping
// data, make disjoint decisions based on their independent snapshots, and
// both commit — producing a state that violates the invariant each read
// verified individually. This is the price of SI's "pushed commit is OK"
// rule: the same relaxation that avoids unnecessary restarts also allows
// write skew.
func TestSI_AllowsWriteSkew(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Seed: balances x=10, y=10. Sum = 20 (invariant: sum ≥ 0).
	seed := hlc.Timestamp{WallTime: 100}
	if err := mvcc.MVCCPut(engine, []byte("x"), seed, []byte("10"), nil); err != nil {
		t.Fatalf("seed x: %v", err)
	}
	if err := mvcc.MVCCPut(engine, []byte("y"), seed, []byte("10"), nil); err != nil {
		t.Fatalf("seed y: %v", err)
	}

	// Txn A reads both, writes x=0.
	txnA, err := Begin(engine, clock, nil, SI)
	if err != nil {
		t.Fatalf("Begin A: %v", err)
	}
	if _, err := txnA.Get([]byte("x")); err != nil {
		t.Fatalf("A Get x: %v", err)
	}
	if _, err := txnA.Get([]byte("y")); err != nil {
		t.Fatalf("A Get y: %v", err)
	}

	// Txn B, started concurrently with A (same ReadTimestamp window), reads
	// both, writes y=0. B does not see A's intent because A hasn't written yet.
	txnB, err := Begin(engine, clock, nil, SI)
	if err != nil {
		t.Fatalf("Begin B: %v", err)
	}
	if _, err := txnB.Get([]byte("x")); err != nil {
		t.Fatalf("B Get x: %v", err)
	}
	if _, err := txnB.Get([]byte("y")); err != nil {
		t.Fatalf("B Get y: %v", err)
	}

	// Both write their respective keys.
	if err := txnA.Put([]byte("x"), []byte("0")); err != nil {
		t.Fatalf("A Put x: %v", err)
	}
	if err := txnB.Put([]byte("y"), []byte("0")); err != nil {
		t.Fatalf("B Put y: %v", err)
	}

	// Both commit under SI.
	if err := txnA.Commit(); err != nil {
		t.Fatalf("A Commit: %v", err)
	}
	if err := txnB.Commit(); err != nil {
		t.Fatalf("B Commit: %v", err)
	}

	// Observe the anomaly: sum = 0, yet each transaction committed while
	// believing sum ≥ 20.
	xFinal, _ := mvcc.MVCCGet(engine, []byte("x"),
		hlc.Timestamp{WallTime: 1_000_000_000_000}, mvcc.ReadOptions{})
	yFinal, _ := mvcc.MVCCGet(engine, []byte("y"),
		hlc.Timestamp{WallTime: 1_000_000_000_000}, mvcc.ReadOptions{})
	if !bytes.Equal(xFinal, []byte("0")) {
		t.Fatalf("x final = %q, want 0", xFinal)
	}
	if !bytes.Equal(yFinal, []byte("0")) {
		t.Fatalf("y final = %q, want 0", yFinal)
	}
	// SI permits this. Under SSI, Step 7 would force one of them to restart.
}
