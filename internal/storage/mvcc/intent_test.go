package mvcc

import (
	"errors"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

func TestResolveIntent_CommitSameTimestamp(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}

	// Write an intent at t=20.
	MVCCPut(engine, []byte("A"), ts(20, 0), []byte("val"), &txn1)

	// Commit at the same timestamp.
	if err := MVCCResolveWriteIntent(engine, []byte("A"), txn1, TxnCommitted, ts(20, 0)); err != nil {
		t.Fatal(err)
	}

	// Now any reader should see the value without WriteIntentError.
	got, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "val" {
		t.Fatalf("got %q, want %q", got, "val")
	}
}

func TestResolveIntent_CommitPushedTimestamp(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}

	// Write intent at t=20, then commit at t=30 (timestamp pushed).
	MVCCPut(engine, []byte("A"), ts(20, 0), []byte("val"), &txn1)

	if err := MVCCResolveWriteIntent(engine, []byte("A"), txn1, TxnCommitted, ts(30, 0)); err != nil {
		t.Fatal(err)
	}

	// At t=25, the value should NOT be visible (committed at t=30).
	_, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected not found at t=25, got %v", err)
	}

	// At t=35, the value IS visible.
	got, err := MVCCGet(engine, []byte("A"), ts(35, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "val" {
		t.Fatalf("got %q, want %q", got, "val")
	}
}

func TestResolveIntent_Abort_RevertsToPrevious(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}

	// Write a committed version, then an intent.
	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("committed"), nil)
	MVCCPut(engine, []byte("A"), ts(20, 0), []byte("intent"), &txn1)

	// Abort T1.
	if err := MVCCResolveWriteIntent(engine, []byte("A"), txn1, TxnAborted, ts(0, 0)); err != nil {
		t.Fatal(err)
	}

	// The intent should be gone. Reading at t=25 should return the old committed value.
	got, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "committed" {
		t.Fatalf("got %q, want %q", got, "committed")
	}
}

func TestResolveIntent_Abort_NoPreviousVersion(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}

	// Write only an intent (no prior committed version).
	MVCCPut(engine, []byte("A"), ts(20, 0), []byte("intent"), &txn1)

	// Abort T1.
	if err := MVCCResolveWriteIntent(engine, []byte("A"), txn1, TxnAborted, ts(0, 0)); err != nil {
		t.Fatal(err)
	}

	// Key should be completely absent.
	_, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestResolveIntent_Abort_PreviousWasTombstone(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}

	// Write a value, delete it, then write an intent.
	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("v1"), nil)
	MVCCDelete(engine, []byte("A"), ts(20, 0), nil)
	MVCCPut(engine, []byte("A"), ts(30, 0), []byte("intent"), &txn1)

	// Abort T1. Metadata should revert to the tombstone at t=20.
	if err := MVCCResolveWriteIntent(engine, []byte("A"), txn1, TxnAborted, ts(0, 0)); err != nil {
		t.Fatal(err)
	}

	// Key should be "not found" because the previous version is a tombstone.
	_, err := MVCCGet(engine, []byte("A"), ts(35, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected not found (tombstone), got %v", err)
	}

	// But reading before the delete should still work.
	got, err := MVCCGet(engine, []byte("A"), ts(15, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v1" {
		t.Fatalf("got %q, want %q", got, "v1")
	}
}

func TestResolveIntent_WrongTxnIsNoop(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	txn1 := TxnID{1}
	txn2 := TxnID{2}

	MVCCPut(engine, []byte("A"), ts(20, 0), []byte("intent"), &txn1)

	// Try to resolve with a different txn ID → should be a no-op.
	if err := MVCCResolveWriteIntent(engine, []byte("A"), txn2, TxnCommitted, ts(20, 0)); err != nil {
		t.Fatal(err)
	}

	// Intent should still be there — T2's read still blocks.
	_, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{Txn: &txn2})
	if !errors.Is(err, ErrWriteIntent) {
		t.Fatalf("expected ErrWriteIntent (intent should still be there), got %v", err)
	}
}

func TestResolveIntent_NoMetadata_Noop(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	// Resolve on a key that was never written → no error.
	txn1 := TxnID{1}
	if err := MVCCResolveWriteIntent(engine, []byte("ghost"), txn1, TxnAborted, ts(0, 0)); err != nil {
		t.Fatal(err)
	}
}
