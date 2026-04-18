package txn

import (
	"bytes"
	"errors"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// TestSSI_ReaderVsSSIBlocker_ReaderWins: the reader has higher priority, so
// the blocker is aborted and the read proceeds. Mirror of Step 4's
// write-write case, but for a read-vs-intent conflict.
func TestSSI_ReaderVsSSIBlocker_ReaderWins(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Prior committed version so the reader has a fallback value.
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// SSI blocker at ReadTs=700, priority=5.
	blocker, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin blocker: %v", err)
	}
	blocker.txn.ReadTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.WriteTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.Priority = 5
	if err := blocker.writeTxnRecord(); err != nil {
		t.Fatalf("persist blocker: %v", err)
	}
	if err := blocker.Put([]byte("k"), []byte("pending")); err != nil {
		t.Fatalf("blocker Put: %v", err)
	}

	// Higher-priority SSI reader at a later timestamp.
	reader, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin reader: %v", err)
	}
	reader.txn.ReadTimestamp = hlc.Timestamp{WallTime: 2_000_000_000}
	reader.txn.Priority = 1000
	if err := reader.writeTxnRecord(); err != nil {
		t.Fatalf("persist reader: %v", err)
	}

	// Read must abort the blocker and return the prior committed value.
	val, err := reader.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("old")) {
		t.Fatalf("Get = %q, want old", val)
	}

	// Blocker's record is now ABORTED.
	rec, ok, _ := LoadTxnRecord(engine, blocker.ID())
	if !ok || rec.Status != TxnAborted {
		t.Fatalf("blocker status = %+v, want ABORTED", rec)
	}
}

// TestSSI_ReaderVsSSIBlocker_ReaderLoses: the reader has lower priority, so
// the reader gets a TxnRetryError with a SuggestedMinPri above the blocker's.
// The blocker keeps their intent untouched.
func TestSSI_ReaderVsSSIBlocker_ReaderLoses(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// High-priority SSI blocker.
	blocker, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin blocker: %v", err)
	}
	blocker.txn.ReadTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.WriteTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.Priority = 1000
	if err := blocker.writeTxnRecord(); err != nil {
		t.Fatalf("persist blocker: %v", err)
	}
	if err := blocker.Put([]byte("k"), []byte("pending")); err != nil {
		t.Fatalf("blocker Put: %v", err)
	}
	blockerOriginalWriteTS := blocker.WriteTimestamp()

	// Low-priority SSI reader.
	reader, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin reader: %v", err)
	}
	reader.txn.ReadTimestamp = hlc.Timestamp{WallTime: 2_000_000_000}
	reader.txn.Priority = 10
	if err := reader.writeTxnRecord(); err != nil {
		t.Fatalf("persist reader: %v", err)
	}

	_, err = reader.Get([]byte("k"))
	var retry *TxnRetryError
	if !errors.As(err, &retry) {
		t.Fatalf("expected *TxnRetryError, got %T: %v", err, err)
	}
	if retry.SuggestedMinPri <= 1000 {
		t.Fatalf("SuggestedMinPri = %d, want > 1000", retry.SuggestedMinPri)
	}

	// The blocker's intent and timestamps are intact — no push happened.
	brec, _, _ := LoadTxnRecord(engine, blocker.ID())
	if brec.Status != TxnPending {
		t.Fatalf("blocker status = %v, want PENDING", brec.Status)
	}
	if brec.WriteTimestamp != blockerOriginalWriteTS {
		t.Fatalf("blocker WriteTimestamp changed: was %v, is %v (expected no push)",
			blockerOriginalWriteTS, brec.WriteTimestamp)
	}
}

// TestSSI_ReaderVsSIBlocker_StillPushes: the old Step-5 push path is
// preserved when the blocker is SI — no priority fight, just push forward.
func TestSSI_ReaderVsSIBlocker_StillPushes(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// SI blocker with LOW priority — priority is irrelevant for SI blockers
	// because SI accepts pushes.
	blocker, err := Begin(engine, clock, nil, SI)
	if err != nil {
		t.Fatalf("Begin blocker: %v", err)
	}
	blocker.txn.ReadTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.WriteTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.Priority = 1 // very low
	if err := blocker.writeTxnRecord(); err != nil {
		t.Fatalf("persist blocker: %v", err)
	}
	if err := blocker.Put([]byte("k"), []byte("pending")); err != nil {
		t.Fatalf("blocker Put: %v", err)
	}
	originalWriteTS := blocker.WriteTimestamp()

	// SSI reader with VERY LOW priority — would lose a priority fight if
	// one were triggered. The test exercises "no priority check for SI
	// blockers" — the reader pushes successfully despite lower priority.
	reader, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin reader: %v", err)
	}
	reader.txn.ReadTimestamp = hlc.Timestamp{WallTime: 2_000_000_000}
	reader.txn.Priority = 0
	if err := reader.writeTxnRecord(); err != nil {
		t.Fatalf("persist reader: %v", err)
	}

	val, err := reader.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("old")) {
		t.Fatalf("Get = %q, want old", val)
	}

	// Blocker was pushed, not aborted.
	brec, _, _ := LoadTxnRecord(engine, blocker.ID())
	if brec.Status != TxnPending {
		t.Fatalf("blocker status = %v, want PENDING (SI push, not abort)", brec.Status)
	}
	if !originalWriteTS.Less(brec.WriteTimestamp) {
		t.Fatalf("blocker WriteTimestamp not pushed: was %v, is %v",
			originalWriteTS, brec.WriteTimestamp)
	}
}

// TestSSI_ReaderBlockerAlreadyPastReadTS: if the blocker's ReadTimestamp is
// already ≥ pushTo, the push would not actually force them to restart.
// The code path falls through to the "safe push" branch and proceeds without
// a priority fight.
func TestSSI_ReaderBlockerAlreadyPastReadTS_NoFight(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// SSI blocker whose ReadTimestamp is already very high (higher than
	// reader's ReadTimestamp.Next()). Pushing them will not move their
	// WriteTimestamp past their ReadTimestamp.
	blocker, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin blocker: %v", err)
	}
	blocker.txn.ReadTimestamp = hlc.Timestamp{WallTime: 10_000_000_000}
	blocker.txn.WriteTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.Priority = 1 // low, shouldn't matter
	if err := blocker.writeTxnRecord(); err != nil {
		t.Fatalf("persist blocker: %v", err)
	}
	if err := blocker.Put([]byte("k"), []byte("pending")); err != nil {
		t.Fatalf("blocker Put: %v", err)
	}

	reader, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin reader: %v", err)
	}
	reader.txn.ReadTimestamp = hlc.Timestamp{WallTime: 2_000_000_000}
	reader.txn.Priority = 0
	if err := reader.writeTxnRecord(); err != nil {
		t.Fatalf("persist reader: %v", err)
	}

	// The reader's push target (reader.ReadTS.Next() = ~2e9+1) is below the
	// blocker's ReadTS (1e10). So the push is "safe" — no SSI restart would
	// follow, and the priority check doesn't fire.
	_, err = reader.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get should succeed (no priority fight): %v", err)
	}

	brec, _, _ := LoadTxnRecord(engine, blocker.ID())
	if brec.Status != TxnPending {
		t.Fatalf("blocker status = %v, want PENDING", brec.Status)
	}
}

// TestSSI_PushedWriter_Restarts: the complementary end-to-end view —
// when a lower-priority SSI reader wins a priority fight (by virtue of
// being higher-priority), the aborted SSI writer's Commit surfaces a
// retry error.
func TestSSI_PushedWriter_RestartsOnCommit(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	blocker, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin blocker: %v", err)
	}
	blocker.txn.ReadTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.WriteTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.Priority = 5
	if err := blocker.writeTxnRecord(); err != nil {
		t.Fatalf("persist blocker: %v", err)
	}
	if err := blocker.Put([]byte("k"), []byte("pending")); err != nil {
		t.Fatalf("blocker Put: %v", err)
	}

	// Higher-priority SSI reader forces the blocker to be aborted.
	reader, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin reader: %v", err)
	}
	reader.txn.ReadTimestamp = hlc.Timestamp{WallTime: 2_000_000_000}
	reader.txn.Priority = 1000
	if err := reader.writeTxnRecord(); err != nil {
		t.Fatalf("persist reader: %v", err)
	}
	if _, err := reader.Get([]byte("k")); err != nil {
		t.Fatalf("reader Get: %v", err)
	}

	// The blocker, oblivious, now tries to commit. The commit path
	// refreshes the record, sees ABORTED, and returns a retry error.
	err = blocker.Commit()
	var retry *TxnRetryError
	if !errors.As(err, &retry) {
		t.Fatalf("blocker Commit: got %v, want *TxnRetryError", err)
	}
}
