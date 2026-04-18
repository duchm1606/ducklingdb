package txn

import (
	"bytes"
	"errors"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/kv/tscache"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// --- Rule 1: writer pushed past cached read ---------------------------------

func TestPush_CachedRead_PushesWriter(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)
	cache := tscache.New(100)

	// Pre-seed the cache to simulate a prior reader at a high timestamp.
	cache.Add([]byte("k"), hlc.Timestamp{WallTime: 5_000_000_000}) // 5s

	tc, err := Begin(engine, clock, cache, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	initialWriteTS := tc.WriteTimestamp()
	if !initialWriteTS.Less(hlc.Timestamp{WallTime: 5_000_000_000}) {
		t.Fatalf("test bug: initial writeTS %v should be below cached 5s", initialWriteTS)
	}

	if err := tc.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// WriteTimestamp must have been pushed past the cached read.
	pushed := tc.WriteTimestamp()
	if !(hlc.Timestamp{WallTime: 5_000_000_000}).Less(pushed) {
		t.Fatalf("WriteTimestamp %v was not pushed past cached read 5s", pushed)
	}
}

// --- Get registers the read in the cache -----------------------------------

func TestPush_GetRegistersRead(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)
	cache := tscache.New(100)

	// Seed a committed value so Get succeeds.
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tc, err := Begin(engine, clock, cache, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	readTS := tc.ReadTimestamp()

	if _, err := tc.Get([]byte("k")); err != nil {
		t.Fatalf("Get: %v", err)
	}

	// The cache should now report readTS as the max for this key.
	if got := cache.GetMax([]byte("k")); got != readTS {
		t.Fatalf("cache.GetMax = %v, want %v", got, readTS)
	}
}

// --- Rule 3: writer pushed past existing committed version -----------------

func TestPush_WriterPushedPastCommittedVersion(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Committed value far in the future (simulates another node's write).
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 9_999_999_999}, []byte("future"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	tc, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}

	if err := tc.Put([]byte("k"), []byte("ours")); err != nil {
		t.Fatalf("Put should succeed after bumping past committed version: %v", err)
	}

	// Our WriteTimestamp must now exceed the pre-existing version.
	if !(hlc.Timestamp{WallTime: 9_999_999_999}).Less(tc.WriteTimestamp()) {
		t.Fatalf("WriteTimestamp %v should exceed 9_999_999_999", tc.WriteTimestamp())
	}
}

// --- Reader pushes a pending blocker ---------------------------------------

func TestPush_ReaderPushesPendingBlocker(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Prior committed version so the reader has something to fall back to.
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Blocker begins early with a low WriteTimestamp.
	blocker, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin blocker: %v", err)
	}
	// Force the blocker's timestamps to be well below the reader's.
	blocker.txn.ReadTimestamp = hlc.Timestamp{WallTime: 700}
	blocker.txn.WriteTimestamp = hlc.Timestamp{WallTime: 700}
	if err := blocker.writeTxnRecord(); err != nil {
		t.Fatalf("persist blocker: %v", err)
	}
	if err := blocker.Put([]byte("k"), []byte("pending")); err != nil {
		t.Fatalf("blocker Put: %v", err)
	}
	blockerOriginalWriteTS := blocker.WriteTimestamp()

	// Reader at a much later timestamp.
	reader, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin reader: %v", err)
	}
	reader.txn.ReadTimestamp = hlc.Timestamp{WallTime: 2_000_000_000} // 2s
	if err := reader.writeTxnRecord(); err != nil {
		t.Fatalf("persist reader: %v", err)
	}

	// Read sees the intent → pushes blocker → retries → returns prior value.
	val, err := reader.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("old")) {
		t.Fatalf("Get = %q, want old (pre-intent version)", val)
	}

	// The blocker's record now reflects the push.
	blockerRec, _, _ := LoadTxnRecord(engine, blocker.ID())
	if !blockerOriginalWriteTS.Less(blockerRec.WriteTimestamp) {
		t.Fatalf("blocker WriteTimestamp not pushed: was %v, is %v",
			blockerOriginalWriteTS, blockerRec.WriteTimestamp)
	}
	if !(reader.ReadTimestamp()).Less(blockerRec.WriteTimestamp) {
		t.Fatalf("pushed blocker %v should be > reader %v",
			blockerRec.WriteTimestamp, reader.ReadTimestamp())
	}
}

// --- Ghost intents on the read path -----------------------------------------

func TestPush_ReadOverCommittedIntent(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Writer creates an intent and flips its own record to COMMITTED but dies
	// before resolution. A reader should finalize and see the value.
	writer, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin writer: %v", err)
	}
	if err := writer.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatalf("writer Put: %v", err)
	}
	rec, _, _ := LoadTxnRecord(engine, writer.ID())
	rec.Status = TxnCommitted
	data, _ := rec.Encode()
	_ = mvcc.MVCCPut(engine, TxnRecordKey(writer.ID()), hlc.Timestamp{}, data, nil)

	reader, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin reader: %v", err)
	}
	// Reader's ReadTimestamp is after the writer's WriteTimestamp.
	reader.txn.ReadTimestamp = writer.WriteTimestamp().Next()
	if err := reader.writeTxnRecord(); err != nil {
		t.Fatalf("persist reader: %v", err)
	}

	val, err := reader.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("v")) {
		t.Fatalf("Get = %q, want %q", val, []byte("v"))
	}
}

func TestPush_ReadOverAbortedIntent(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)
	clock := testClock(t)

	// Prior committed version.
	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	writer, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin writer: %v", err)
	}
	if err := writer.Put([]byte("k"), []byte("doomed")); err != nil {
		t.Fatalf("writer Put: %v", err)
	}
	// Flip writer record to ABORTED without resolving intent.
	rec, _, _ := LoadTxnRecord(engine, writer.ID())
	rec.Status = TxnAborted
	data, _ := rec.Encode()
	_ = mvcc.MVCCPut(engine, TxnRecordKey(writer.ID()), hlc.Timestamp{}, data, nil)

	reader, err := Begin(engine, clock, nil, SSI)
	if err != nil {
		t.Fatalf("Begin reader: %v", err)
	}

	val, err := reader.Get([]byte("k"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !bytes.Equal(val, []byte("old")) {
		t.Fatalf("Get = %q, want old (aborted intent cleaned up)", val)
	}
}

// --- WriteTooOldError surfaces correctly when no retry helps ----------------

func TestPush_WriteTooOld_CarriesDetails(t *testing.T) {
	t.Parallel()

	engine := testEngine(t)

	if err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 1000}, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := mvcc.MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 500}, []byte("v'"), nil)
	var too *mvcc.WriteTooOldError
	if !errors.As(err, &too) {
		t.Fatalf("expected *WriteTooOldError, got %T", err)
	}
	if too.ExistingTimestamp != (hlc.Timestamp{WallTime: 1000}) {
		t.Fatalf("ExistingTimestamp = %v, want 1000", too.ExistingTimestamp)
	}
}
