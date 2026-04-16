package mvcc

import (
	"errors"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// newTestEngine creates a fresh LSM engine in a temp directory.
func newTestEngine(t *testing.T) storage.Engine {
	t.Helper()
	engine, err := lsm.OpenLSM(lsm.LSMOptions{Dir: t.TempDir(), MemTableThreshold: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { engine.Close() })
	return engine
}

// seedVersion writes an MVCC version directly into the engine (bypassing MVCCPut).
func seedVersion(t *testing.T, engine storage.Engine, key []byte, timestamp hlc.Timestamp, value []byte) {
	t.Helper()
	encKey := Encode(MVCCKey{Key: key, Timestamp: timestamp})
	if err := engine.Put(encKey, encodeMVCCValue(value)); err != nil {
		t.Fatal(err)
	}
}

// seedTombstone writes an MVCC tombstone version directly.
func seedTombstone(t *testing.T, engine storage.Engine, key []byte, timestamp hlc.Timestamp) {
	t.Helper()
	encKey := Encode(MVCCKey{Key: key, Timestamp: timestamp})
	if err := engine.Put(encKey, encodeMVCCTombstone()); err != nil {
		t.Fatal(err)
	}
}

// seedMeta writes an MVCCMetadata entry directly.
func seedMeta(t *testing.T, engine storage.Engine, key []byte, meta MVCCMetadata) {
	t.Helper()
	metaKey := EncodeMeta(key)
	data, err := meta.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Put(metaKey, data); err != nil {
		t.Fatal(err)
	}
}

func TestMVCCGet_BasicRead(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	seedMeta(t, engine, []byte("A"), MVCCMetadata{Timestamp: ts(20, 0)})
	seedVersion(t, engine, []byte("A"), ts(10, 0), []byte("v1"))
	seedVersion(t, engine, []byte("A"), ts(20, 0), []byte("v2"))

	// Read at t=25 → newest version ≤ 25 is t=20 → "v2"
	got, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v2" {
		t.Fatalf("got %q, want %q", got, "v2")
	}

	// Read at t=15 → newest version ≤ 15 is t=10 → "v1"
	got, err = MVCCGet(engine, []byte("A"), ts(15, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v1" {
		t.Fatalf("got %q, want %q", got, "v1")
	}
}

func TestMVCCGet_ReadBeforeAnyVersion(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	seedMeta(t, engine, []byte("A"), MVCCMetadata{Timestamp: ts(10, 0)})
	seedVersion(t, engine, []byte("A"), ts(10, 0), []byte("v1"))

	// Read at t=5 → no version ≤ 5
	_, err := MVCCGet(engine, []byte("A"), ts(5, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestMVCCGet_KeyNeverWritten(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	_, err := MVCCGet(engine, []byte("ghost"), ts(100, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}
}

func TestMVCCGet_TombstoneReturnsNotFound(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	seedMeta(t, engine, []byte("A"), MVCCMetadata{Timestamp: ts(20, 0), Deleted: true})
	seedVersion(t, engine, []byte("A"), ts(10, 0), []byte("v1"))
	seedTombstone(t, engine, []byte("A"), ts(20, 0))

	// Read at t=25 → newest ≤ 25 is t=20 → tombstone → not found
	_, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got %v", err)
	}

	// Read at t=15 → newest ≤ 15 is t=10 → "v1" (before the delete)
	got, err := MVCCGet(engine, []byte("A"), ts(15, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v1" {
		t.Fatalf("got %q, want %q", got, "v1")
	}
}

func TestMVCCGet_IntentBlocks(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}
	seedMeta(t, engine, []byte("A"), MVCCMetadata{
		Timestamp:    ts(20, 0),
		Txn:          &txn1,
		TxnTimestamp: ts(20, 0),
	})
	seedVersion(t, engine, []byte("A"), ts(20, 0), []byte("intent-val"))
	seedVersion(t, engine, []byte("A"), ts(10, 0), []byte("v1"))

	// Another transaction reads → WriteIntentError
	txn2 := TxnID{2}
	_, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{Txn: &txn2})
	if !errors.Is(err, ErrWriteIntent) {
		t.Fatalf("expected ErrWriteIntent, got %v", err)
	}
}

func TestMVCCGet_OwnIntentAllowed(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}
	seedMeta(t, engine, []byte("A"), MVCCMetadata{
		Timestamp:    ts(20, 0),
		Txn:          &txn1,
		TxnTimestamp: ts(20, 0),
	})
	seedVersion(t, engine, []byte("A"), ts(20, 0), []byte("my-intent"))

	// Same transaction reads → sees its own intent value
	got, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{Txn: &txn1})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "my-intent" {
		t.Fatalf("got %q, want %q", got, "my-intent")
	}
}

// ---------------------------------------------------------------------------
// MVCCPut tests
// ---------------------------------------------------------------------------

func TestMVCCPut_NewKey(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	if err := MVCCPut(engine, []byte("A"), ts(10, 0), []byte("v1"), nil); err != nil {
		t.Fatal(err)
	}

	got, err := MVCCGet(engine, []byte("A"), ts(10, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v1" {
		t.Fatalf("got %q, want %q", got, "v1")
	}
}

func TestMVCCPut_MultipleVersions(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("v1"), nil)
	MVCCPut(engine, []byte("A"), ts(20, 0), []byte("v2"), nil)
	MVCCPut(engine, []byte("A"), ts(30, 0), []byte("v3"), nil)

	// Read at each timestamp.
	for _, tc := range []struct {
		wall int64
		want string
	}{
		{10, "v1"}, {15, "v1"}, {20, "v2"}, {25, "v2"}, {30, "v3"}, {100, "v3"},
	} {
		got, err := MVCCGet(engine, []byte("A"), ts(tc.wall, 0), ReadOptions{})
		if err != nil {
			t.Fatalf("Get(t=%d): %v", tc.wall, err)
		}
		if string(got) != tc.want {
			t.Fatalf("Get(t=%d) = %q, want %q", tc.wall, got, tc.want)
		}
	}
}

func TestMVCCPut_IntentConflict(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}
	txn2 := TxnID{2}

	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("v1"), &txn1)

	// Different transaction tries to write → blocked.
	err := MVCCPut(engine, []byte("A"), ts(20, 0), []byte("v2"), &txn2)
	if !errors.Is(err, ErrWriteIntent) {
		t.Fatalf("expected ErrWriteIntent, got %v", err)
	}
}

func TestMVCCPut_OverwriteOwnIntent(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}

	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("first"), &txn1)
	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("second"), &txn1)

	got, err := MVCCGet(engine, []byte("A"), ts(10, 0), ReadOptions{Txn: &txn1})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "second" {
		t.Fatalf("got %q, want %q", got, "second")
	}
}

// ---------------------------------------------------------------------------
// MVCCDelete tests
// ---------------------------------------------------------------------------

func TestMVCCDelete_Basic(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("v1"), nil)
	MVCCDelete(engine, []byte("A"), ts(20, 0), nil)

	// At t=25, key is deleted.
	_, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected not found after delete, got %v", err)
	}

	// At t=15, key is still visible (before the delete).
	got, err := MVCCGet(engine, []byte("A"), ts(15, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v1" {
		t.Fatalf("got %q, want %q", got, "v1")
	}
}

func TestMVCCDelete_IntentConflict(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}
	txn2 := TxnID{2}

	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("v1"), &txn1)

	err := MVCCDelete(engine, []byte("A"), ts(20, 0), &txn2)
	if !errors.Is(err, ErrWriteIntent) {
		t.Fatalf("expected ErrWriteIntent, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// MVCCScan tests
// ---------------------------------------------------------------------------

func TestMVCCScan_BasicRange(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	MVCCPut(engine, []byte("a"), ts(10, 0), []byte("va"), nil)
	MVCCPut(engine, []byte("b"), ts(10, 0), []byte("vb"), nil)
	MVCCPut(engine, []byte("c"), ts(10, 0), []byte("vc"), nil)
	MVCCPut(engine, []byte("d"), ts(10, 0), []byte("vd"), nil)
	MVCCPut(engine, []byte("e"), ts(10, 0), []byte("ve"), nil)

	// Scan [b, d) → b, c
	results, err := MVCCScan(engine, []byte("b"), []byte("d"), ts(20, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if string(results[0].Key) != "b" || string(results[0].Value) != "vb" {
		t.Fatalf("results[0] = %q:%q", results[0].Key, results[0].Value)
	}
	if string(results[1].Key) != "c" || string(results[1].Value) != "vc" {
		t.Fatalf("results[1] = %q:%q", results[1].Key, results[1].Value)
	}
}

func TestMVCCScan_RespectsTimestamp(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	MVCCPut(engine, []byte("a"), ts(10, 0), []byte("old"), nil)
	MVCCPut(engine, []byte("a"), ts(20, 0), []byte("new"), nil)
	MVCCPut(engine, []byte("b"), ts(15, 0), []byte("vb"), nil)

	// Scan at t=12 → a="old", b not yet written
	results, err := MVCCScan(engine, []byte("a"), []byte("z"), ts(12, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || string(results[0].Value) != "old" {
		t.Fatalf("expected [a=old], got %v", results)
	}

	// Scan at t=25 → a="new", b="vb"
	results, err = MVCCScan(engine, []byte("a"), []byte("z"), ts(25, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2, got %d", len(results))
	}
	if string(results[0].Value) != "new" {
		t.Fatalf("a: got %q, want %q", results[0].Value, "new")
	}
}

func TestMVCCScan_SkipsDeletedKeys(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	MVCCPut(engine, []byte("a"), ts(10, 0), []byte("va"), nil)
	MVCCPut(engine, []byte("b"), ts(10, 0), []byte("vb"), nil)
	MVCCPut(engine, []byte("c"), ts(10, 0), []byte("vc"), nil)
	MVCCDelete(engine, []byte("b"), ts(20, 0), nil)

	// Scan at t=25 → a, c (b is deleted)
	results, err := MVCCScan(engine, []byte("a"), []byte("z"), ts(25, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2, got %d", len(results))
	}
	if string(results[0].Key) != "a" || string(results[1].Key) != "c" {
		t.Fatalf("got keys %q, %q", results[0].Key, results[1].Key)
	}

	// Scan at t=15 → a, b, c (before the delete)
	results, err = MVCCScan(engine, []byte("a"), []byte("z"), ts(15, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3, got %d", len(results))
	}
}

func TestMVCCScan_IntentBlocks(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}
	txn2 := TxnID{2}

	MVCCPut(engine, []byte("a"), ts(10, 0), []byte("va"), nil)
	MVCCPut(engine, []byte("b"), ts(10, 0), []byte("vb"), &txn1) // intent

	// Scan from T2 encounters T1's intent → error
	_, err := MVCCScan(engine, []byte("a"), []byte("z"), ts(20, 0), ReadOptions{Txn: &txn2})
	if !errors.Is(err, ErrWriteIntent) {
		t.Fatalf("expected ErrWriteIntent, got %v", err)
	}
}

func TestMVCCScan_NilEnd(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	MVCCPut(engine, []byte("a"), ts(10, 0), []byte("va"), nil)
	MVCCPut(engine, []byte("z"), ts(10, 0), []byte("vz"), nil)

	// nil end → scan to the end of the keyspace
	results, err := MVCCScan(engine, []byte("a"), nil, ts(20, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2, got %d", len(results))
	}
}

// ---------------------------------------------------------------------------
// MVCCGet tests (continued)
// ---------------------------------------------------------------------------

func TestMVCCGet_IntentInFutureSkipped(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}
	seedMeta(t, engine, []byte("A"), MVCCMetadata{
		Timestamp:    ts(30, 0),
		Txn:          &txn1,
		TxnTimestamp: ts(30, 0),
	})
	seedVersion(t, engine, []byte("A"), ts(30, 0), []byte("future-intent"))
	seedVersion(t, engine, []byte("A"), ts(10, 0), []byte("committed"))

	// Read at t=20 — intent is at t=30 which is after our timestamp.
	// Should skip the intent check and return the committed version.
	got, err := MVCCGet(engine, []byte("A"), ts(20, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "committed" {
		t.Fatalf("got %q, want %q", got, "committed")
	}
}
