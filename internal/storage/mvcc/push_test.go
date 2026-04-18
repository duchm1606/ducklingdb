package mvcc

import (
	"bytes"
	"errors"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// --- WriteTooOldError tests -------------------------------------------------

func TestMVCCPut_WriteTooOld_BelowCommitted(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)

	// Committed version at t=100.
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v100"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// A write at t=50 is too old — newer committed version exists.
	err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 50}, []byte("v50"), nil)
	if err == nil {
		t.Fatal("expected WriteTooOldError, got nil")
	}
	var too *WriteTooOldError
	if !errors.As(err, &too) {
		t.Fatalf("expected *WriteTooOldError, got %T: %v", err, err)
	}
	if too.ExistingTimestamp != (hlc.Timestamp{WallTime: 100}) {
		t.Fatalf("ExistingTimestamp = %v, want 100", too.ExistingTimestamp)
	}
	if too.RequestedTimestamp != (hlc.Timestamp{WallTime: 50}) {
		t.Fatalf("RequestedTimestamp = %v, want 50", too.RequestedTimestamp)
	}
	// Sentinel matching.
	if !errors.Is(err, ErrWriteTooOld) {
		t.Fatal("errors.Is(err, ErrWriteTooOld) should match")
	}
}

func TestMVCCPut_WriteTooOld_EqualToCommitted(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Writing at exactly the same timestamp is also too old — a new version
	// below-or-equal the newest committed one breaks serializability.
	err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v'"), nil)
	if !errors.Is(err, ErrWriteTooOld) {
		t.Fatalf("expected ErrWriteTooOld, got %v", err)
	}
}

func TestMVCCPut_AboveCommitted_Succeeds(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("old"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 200}, []byte("new"), nil); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
}

func TestMVCCPut_OwnIntentAtSameTimestamp_Succeeds(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	txn := TxnID{1}

	// First write from this txn at t=100 creates an intent.
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v"), &txn); err != nil {
		t.Fatalf("first put: %v", err)
	}
	// Second write from the same txn at the same timestamp should succeed —
	// the own-intent path skips the too-old check.
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v'"), &txn); err != nil {
		t.Fatalf("second put from same txn: %v", err)
	}
}

func TestMVCCDelete_WriteTooOld(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	err := MVCCDelete(engine, []byte("k"), hlc.Timestamp{WallTime: 50}, nil)
	if !errors.Is(err, ErrWriteTooOld) {
		t.Fatalf("expected ErrWriteTooOld, got %v", err)
	}
}

// --- MVCCPushIntent tests ---------------------------------------------------

func TestMVCCPushIntent_PushesIntentForward(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	txn := TxnID{1}

	// Intent at t=100.
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v"), &txn); err != nil {
		t.Fatalf("seed intent: %v", err)
	}

	// Push to t=200.
	if err := MVCCPushIntent(engine, []byte("k"), txn, hlc.Timestamp{WallTime: 200}); err != nil {
		t.Fatalf("push: %v", err)
	}

	// Metadata reflects the push.
	metaVal, err := engine.Get(EncodeMeta([]byte("k")))
	if err != nil {
		t.Fatalf("read meta: %v", err)
	}
	meta, err := DecodeMetadata(metaVal)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if meta.TxnTimestamp != (hlc.Timestamp{WallTime: 200}) {
		t.Fatalf("TxnTimestamp = %v, want 200", meta.TxnTimestamp)
	}
	if meta.Timestamp != (hlc.Timestamp{WallTime: 200}) {
		t.Fatalf("Timestamp = %v, want 200", meta.Timestamp)
	}
	// Intent is still owned by the same txn.
	if !meta.HasIntent() || *meta.Txn != txn {
		t.Fatalf("intent owner changed: %+v", meta.Txn)
	}

	// The old entry at t=100 is gone.
	oldKey := Encode(MVCCKey{Key: []byte("k"), Timestamp: hlc.Timestamp{WallTime: 100}})
	if _, err := engine.Get(oldKey); !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("old entry should be deleted, got err=%v", err)
	}

	// The new entry at t=200 exists with the original value.
	newKey := Encode(MVCCKey{Key: []byte("k"), Timestamp: hlc.Timestamp{WallTime: 200}})
	raw, err := engine.Get(newKey)
	if err != nil {
		t.Fatalf("read new entry: %v", err)
	}
	val, isT := decodeMVCCValue(raw)
	if isT || !bytes.Equal(val, []byte("v")) {
		t.Fatalf("new entry value = %q tombstone=%v, want %q", val, isT, []byte("v"))
	}
}

func TestMVCCPushIntent_MakesIntentInvisibleToOlderReads(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	txn := TxnID{1}

	// Committed value at t=50, then intent at t=100.
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 50}, []byte("committed"), nil); err != nil {
		t.Fatalf("seed committed: %v", err)
	}
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("intent"), &txn); err != nil {
		t.Fatalf("seed intent: %v", err)
	}

	// A read at t=150 would block on the intent (intentTs=100 ≤ 150).
	_, err := MVCCGet(engine, []byte("k"), hlc.Timestamp{WallTime: 150}, ReadOptions{})
	if !errors.Is(err, ErrWriteIntent) {
		t.Fatalf("pre-push read: expected ErrWriteIntent, got %v", err)
	}

	// Push the intent to t=200.
	if err := MVCCPushIntent(engine, []byte("k"), txn, hlc.Timestamp{WallTime: 200}); err != nil {
		t.Fatalf("push: %v", err)
	}

	// Now the same read sees the prior committed value — intent is "in the future."
	got, err := MVCCGet(engine, []byte("k"), hlc.Timestamp{WallTime: 150}, ReadOptions{})
	if err != nil {
		t.Fatalf("post-push read: %v", err)
	}
	if !bytes.Equal(got, []byte("committed")) {
		t.Fatalf("post-push read = %q, want committed", got)
	}
}

func TestMVCCPushIntent_BackwardPush_NoOp(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	txn := TxnID{1}
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v"), &txn); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Attempt to push backward — must be a no-op, not an error.
	if err := MVCCPushIntent(engine, []byte("k"), txn, hlc.Timestamp{WallTime: 50}); err != nil {
		t.Fatalf("push: %v", err)
	}
	// Metadata still at 100.
	metaVal, _ := engine.Get(EncodeMeta([]byte("k")))
	meta, _ := DecodeMetadata(metaVal)
	if meta.TxnTimestamp != (hlc.Timestamp{WallTime: 100}) {
		t.Fatalf("TxnTimestamp = %v, want 100 (push backward ignored)", meta.TxnTimestamp)
	}
}

func TestMVCCPushIntent_DifferentTxn_NoOp(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	txnA := TxnID{1}
	txnB := TxnID{2}

	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v"), &txnA); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Push attempted by txnB — not the owner. Must be a no-op.
	if err := MVCCPushIntent(engine, []byte("k"), txnB, hlc.Timestamp{WallTime: 200}); err != nil {
		t.Fatalf("push: %v", err)
	}
	metaVal, _ := engine.Get(EncodeMeta([]byte("k")))
	meta, _ := DecodeMetadata(metaVal)
	if meta.TxnTimestamp != (hlc.Timestamp{WallTime: 100}) {
		t.Fatalf("TxnTimestamp = %v, want 100 (push by non-owner ignored)", meta.TxnTimestamp)
	}
}

func TestMVCCPushIntent_NoIntent_NoOp(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	txn := TxnID{1}
	// Committed version, no intent.
	if err := MVCCPut(engine, []byte("k"), hlc.Timestamp{WallTime: 100}, []byte("v"), nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := MVCCPushIntent(engine, []byte("k"), txn, hlc.Timestamp{WallTime: 200}); err != nil {
		t.Fatalf("push: %v", err)
	}
	// Nothing changed.
	metaVal, _ := engine.Get(EncodeMeta([]byte("k")))
	meta, _ := DecodeMetadata(metaVal)
	if meta.HasIntent() {
		t.Fatal("push created an intent out of thin air")
	}
}
