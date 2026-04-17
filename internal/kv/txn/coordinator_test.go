package txn

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// testEngine opens a fresh LSM engine in a temporary directory. Returns both
// the engine and a cleanup function.
func testEngine(t *testing.T) storage.Engine {
	t.Helper()
	dir := t.TempDir()
	engine, err := lsm.OpenLSM(lsm.LSMOptions{Dir: dir})
	if err != nil {
		t.Fatalf("open LSM: %v", err)
	}
	t.Cleanup(func() {
		_ = engine.Close()
	})
	return engine
}

// testClock returns a Clock driven by a ManualClock starting at wall=1s. Using
// a manual clock makes timestamp assertions deterministic.
func testClock(t *testing.T) *hlc.Clock {
	t.Helper()
	return hlc.NewClock(hlc.NewManualClock(1_000_000_000), 250*time.Millisecond)
}

func TestBegin_WritesPendingRecord(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	tc, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if tc.Status() != TxnPending {
		t.Fatalf("status = %v, want PENDING", tc.Status())
	}
	if tc.ID() == (mvcc.TxnID{}) {
		t.Fatal("Begin produced zero TxnID")
	}
	if tc.ReadTimestamp() != tc.WriteTimestamp() {
		t.Fatalf("initial read (%v) and write (%v) timestamps should be equal",
			tc.ReadTimestamp(), tc.WriteTimestamp())
	}

	rec, ok, err := LoadTxnRecord(engine, tc.ID())
	if err != nil {
		t.Fatalf("LoadTxnRecord: %v", err)
	}
	if !ok {
		t.Fatal("record not found")
	}
	if rec.Status != TxnPending {
		t.Fatalf("stored status = %v, want PENDING", rec.Status)
	}
	if rec.Isolation != SSI {
		t.Fatalf("stored isolation = %v, want SSI", rec.Isolation)
	}
}

func TestBegin_UniqueIDs(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	seen := make(map[mvcc.TxnID]struct{})
	for i := range 50 {
		tc, err := Begin(engine, clock, SSI)
		if err != nil {
			t.Fatalf("Begin %d: %v", i, err)
		}
		if _, dup := seen[tc.ID()]; dup {
			t.Fatalf("duplicate TxnID on iteration %d: %x", i, tc.ID())
		}
		seen[tc.ID()] = struct{}{}
	}
}

func TestPut_Get_SameTransaction(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	tc, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if err := tc.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// A transaction must see its own writes.
	got, err := tc.Get([]byte("k1"))
	if err != nil {
		t.Fatalf("Get own write: %v", err)
	}
	if !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("Get own write = %q, want %q", got, []byte("v1"))
	}
}

func TestCommit_MakesIntentsVisible(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	tc, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	writeTS := tc.WriteTimestamp()

	if err := tc.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := tc.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put b: %v", err)
	}

	if err := tc.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// After commit, a plain (non-txn) reader at the commit timestamp sees the
	// values — intents have been resolved to committed versions.
	readTS := writeTS.Next()
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		got, err := mvcc.MVCCGet(engine, []byte(kv.k), readTS, mvcc.ReadOptions{})
		if err != nil {
			t.Fatalf("post-commit Get %q: %v", kv.k, err)
		}
		if !bytes.Equal(got, []byte(kv.v)) {
			t.Fatalf("post-commit Get %q = %q, want %q", kv.k, got, kv.v)
		}
	}

	// The txn record has been updated to COMMITTED.
	rec, ok, err := LoadTxnRecord(engine, tc.ID())
	if err != nil || !ok {
		t.Fatalf("LoadTxnRecord after commit: ok=%v err=%v", ok, err)
	}
	if rec.Status != TxnCommitted {
		t.Fatalf("record status = %v, want COMMITTED", rec.Status)
	}
}

func TestAbort_CleansUpIntents(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// A prior committed value exists so we can verify abort reverts cleanly.
	if err := mvcc.MVCCPut(engine, []byte("a"), hlc.Timestamp{WallTime: 500}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tc, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tc.Put([]byte("a"), []byte("new")); err != nil {
		t.Fatalf("Put a: %v", err)
	}
	if err := tc.Put([]byte("b"), []byte("new")); err != nil {
		t.Fatalf("Put b: %v", err)
	}

	if err := tc.Abort(); err != nil {
		t.Fatalf("Abort: %v", err)
	}

	// After abort, 'a' reverts to the old committed version.
	got, err := mvcc.MVCCGet(engine, []byte("a"), tc.WriteTimestamp().Next(), mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("post-abort Get a: %v", err)
	}
	if !bytes.Equal(got, []byte("old")) {
		t.Fatalf("post-abort Get a = %q, want %q", got, []byte("old"))
	}

	// 'b' had no prior version, so it should be gone entirely.
	_, err = mvcc.MVCCGet(engine, []byte("b"), tc.WriteTimestamp().Next(), mvcc.ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("post-abort Get b: got %v, want ErrKeyNotFound", err)
	}

	// Record is ABORTED.
	rec, ok, err := LoadTxnRecord(engine, tc.ID())
	if err != nil || !ok {
		t.Fatalf("LoadTxnRecord after abort: ok=%v err=%v", ok, err)
	}
	if rec.Status != TxnAborted {
		t.Fatalf("record status = %v, want ABORTED", rec.Status)
	}
}

func TestDelete_AsIntent(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tc, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tc.Delete([]byte("k")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if err := tc.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Reader at commit time sees the key gone (tombstone).
	_, err = mvcc.MVCCGet(engine, []byte("k"), tc.WriteTimestamp().Next(), mvcc.ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("post-commit delete: got %v, want ErrKeyNotFound", err)
	}
}

func TestCommit_TracksAllIntentKeys(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	tc, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	keys := [][]byte{[]byte("k1"), []byte("k2"), []byte("k3")}
	for _, k := range keys {
		if err := tc.Put(k, []byte("v")); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	// Multiple writes to the same key should not create duplicate intent entries.
	if err := tc.Put([]byte("k1"), []byte("v-updated")); err != nil {
		t.Fatalf("Put k1 second time: %v", err)
	}

	snap := tc.Snapshot()
	if len(snap.IntentKeys) != 3 {
		t.Fatalf("expected 3 tracked keys, got %d", len(snap.IntentKeys))
	}

	if err := tc.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
}

func TestOperations_AfterFinalized(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	tc, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tc.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := tc.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// All operations on a finalized coordinator must return ErrTxnFinalized.
	if _, err := tc.Get([]byte("k")); !errors.Is(err, ErrTxnFinalized) {
		t.Fatalf("Get after commit: got %v, want ErrTxnFinalized", err)
	}
	if err := tc.Put([]byte("k"), []byte("v2")); !errors.Is(err, ErrTxnFinalized) {
		t.Fatalf("Put after commit: got %v, want ErrTxnFinalized", err)
	}
	if err := tc.Delete([]byte("k")); !errors.Is(err, ErrTxnFinalized) {
		t.Fatalf("Delete after commit: got %v, want ErrTxnFinalized", err)
	}
	if err := tc.Commit(); !errors.Is(err, ErrTxnFinalized) {
		t.Fatalf("double commit: got %v, want ErrTxnFinalized", err)
	}
	if err := tc.Abort(); !errors.Is(err, ErrTxnFinalized) {
		t.Fatalf("abort after commit: got %v, want ErrTxnFinalized", err)
	}
}

func TestLoadTxnRecord_Missing(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	_, ok, err := LoadTxnRecord(engine, mvcc.TxnID{0xFF})
	if err != nil {
		t.Fatalf("LoadTxnRecord: %v", err)
	}
	if ok {
		t.Fatal("expected ok=false for missing record")
	}
}

func TestPut_OtherTxnIntent_ReturnsErrWriteIntent(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Txn 1 writes an intent and does NOT commit.
	tc1, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin 1: %v", err)
	}
	if err := tc1.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatalf("Put 1: %v", err)
	}

	// Txn 2 tries to write the same key → should hit ErrWriteIntent. Step 4
	// will handle this; for now we just verify the error propagates up.
	tc2, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin 2: %v", err)
	}
	err = tc2.Put([]byte("k"), []byte("v2"))
	if !errors.Is(err, mvcc.ErrWriteIntent) {
		t.Fatalf("Put over foreign intent: got %v, want ErrWriteIntent", err)
	}
}

func TestCommit_WriteRegisteredBeforePossibleCrash(t *testing.T) {
	t.Parallel()

	// The record must be updated after every Put — if we crashed without this,
	// the commit path would not know which intents to resolve.
	engine := testEngine(t)
	clock := testClock(t)

	tc, err := Begin(engine, clock, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tc.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Read the record back from the engine (simulating what a recovery path
	// would do) and verify the intent is listed.
	rec, ok, err := LoadTxnRecord(engine, tc.ID())
	if err != nil || !ok {
		t.Fatalf("LoadTxnRecord: ok=%v err=%v", ok, err)
	}
	if len(rec.IntentKeys) != 1 || !bytes.Equal(rec.IntentKeys[0], []byte("k")) {
		t.Fatalf("record.IntentKeys = %v, want [\"k\"]", rec.IntentKeys)
	}
}
