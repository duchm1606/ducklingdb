package mvcc

import (
	"errors"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

// ---------------------------------------------------------------------------
// Inline value tests
// ---------------------------------------------------------------------------

func TestInlineValue_PutAndGet(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	// Write inline value (timestamp=0).
	if err := MVCCPut(engine, []byte("txn-record"), ts(0, 0), []byte("data"), nil); err != nil {
		t.Fatal(err)
	}

	// Read inline value (timestamp=0).
	got, err := MVCCGet(engine, []byte("txn-record"), ts(0, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "data" {
		t.Fatalf("got %q, want %q", got, "data")
	}
}

func TestInlineValue_Overwrite(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	MVCCPut(engine, []byte("key"), ts(0, 0), []byte("v1"), nil)
	MVCCPut(engine, []byte("key"), ts(0, 0), []byte("v2"), nil)

	got, err := MVCCGet(engine, []byte("key"), ts(0, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "v2" {
		t.Fatalf("got %q, want %q", got, "v2")
	}
}

func TestInlineValue_NotVisibleAtNonZeroTimestamp(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	MVCCPut(engine, []byte("key"), ts(0, 0), []byte("inline"), nil)

	// Reading at a non-zero timestamp should not find the inline value.
	_, err := MVCCGet(engine, []byte("key"), ts(10, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound for non-zero timestamp, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// GC tests
// ---------------------------------------------------------------------------

func TestGC_DropsOldVersions(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	// Write 5 versions.
	for i := int64(1); i <= 5; i++ {
		MVCCPut(engine, []byte("A"), ts(i*10, 0), []byte{byte(i)}, nil)
	}

	// GC with threshold=30 → keep versions at t=40, t=50, drop t=10, t=20, t=30.
	// But t=30 is kept because rule says "keep newest ≤ threshold" only applies
	// when it's the overall newest. Here t=50 is newest, so t=30 gets dropped.
	if err := MVCCGarbageCollect(engine, ts(30, 0)); err != nil {
		t.Fatal(err)
	}

	// Versions 40 and 50 should still be readable.
	got, err := MVCCGet(engine, []byte("A"), ts(50, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 5 {
		t.Fatalf("newest: got %d, want 5", got[0])
	}

	got, err = MVCCGet(engine, []byte("A"), ts(40, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 4 {
		t.Fatalf("t=40: got %d, want 4", got[0])
	}

	// Versions at t=10,20,30 should be gone.
	for _, wall := range []int64{10, 20, 30} {
		_, err := MVCCGet(engine, []byte("A"), ts(wall, 0), ReadOptions{})
		if !errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("Get(t=%d) should be not found after GC, got %v", wall, err)
		}
	}
}

func TestGC_KeepsNewestEvenIfOld(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	// Single version at t=10.
	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("only"), nil)

	// GC at threshold=100 → version is old but it's the newest (and only), keep it.
	if err := MVCCGarbageCollect(engine, ts(100, 0)); err != nil {
		t.Fatal(err)
	}

	got, err := MVCCGet(engine, []byte("A"), ts(100, 0), ReadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "only" {
		t.Fatalf("got %q, want %q", got, "only")
	}
}

func TestGC_TombstoneAtBottom_FullyRemoved(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	// Write then delete.
	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("val"), nil)
	MVCCDelete(engine, []byte("A"), ts(20, 0), nil)

	// GC at threshold=30 → tombstone is newest and ≤ threshold → fully remove.
	if err := MVCCGarbageCollect(engine, ts(30, 0)); err != nil {
		t.Fatal(err)
	}

	// Key should be completely gone — even metadata deleted.
	_, err := MVCCGet(engine, []byte("A"), ts(100, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected fully gone, got %v", err)
	}
}

func TestGC_TombstoneAboveThreshold_Preserved(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("val"), nil)
	MVCCDelete(engine, []byte("A"), ts(20, 0), nil)

	// GC at threshold=15 → tombstone at t=20 is above threshold → keep it.
	if err := MVCCGarbageCollect(engine, ts(15, 0)); err != nil {
		t.Fatal(err)
	}

	// At t=25, should still see "not found" (tombstone preserved).
	_, err := MVCCGet(engine, []byte("A"), ts(25, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected not found (tombstone), got %v", err)
	}

	// Old version at t=10 should be GC'd.
	_, err = MVCCGet(engine, []byte("A"), ts(10, 0), ReadOptions{})
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("expected old version GC'd, got %v", err)
	}
}

func TestGC_MultipleKeys(t *testing.T) {
	t.Parallel()
	engine := newTestEngine(t)

	// Two keys, each with old and new versions.
	MVCCPut(engine, []byte("A"), ts(10, 0), []byte("a-old"), nil)
	MVCCPut(engine, []byte("A"), ts(30, 0), []byte("a-new"), nil)
	MVCCPut(engine, []byte("B"), ts(10, 0), []byte("b-old"), nil)
	MVCCPut(engine, []byte("B"), ts(30, 0), []byte("b-new"), nil)

	if err := MVCCGarbageCollect(engine, ts(20, 0)); err != nil {
		t.Fatal(err)
	}

	// New versions still exist.
	for _, key := range []string{"A", "B"} {
		_, err := MVCCGet(engine, []byte(key), ts(30, 0), ReadOptions{})
		if err != nil {
			t.Fatalf("Get(%s, t=30) after GC: %v", key, err)
		}
	}

	// Old versions are gone.
	for _, key := range []string{"A", "B"} {
		_, err := MVCCGet(engine, []byte(key), ts(10, 0), ReadOptions{})
		if !errors.Is(err, storage.ErrKeyNotFound) {
			t.Fatalf("Get(%s, t=10) should be gone, got %v", key, err)
		}
	}
}
