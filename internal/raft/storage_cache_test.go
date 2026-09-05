package raft

import (
	"os"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
)

// TestLastIndexCacheMatchesScan pins that the cached LastIndex agrees with a
// full scan across every mutation that can change it.
//
// LastIndex used to rescan the whole raft-log key range on every call, and it
// is called several times per Ready cycle at 100Hz. Once iterators began
// snapshotting the memtable, each of those calls also copied it. Caching fixes
// that, but only if the invalidation is right — a stale cache would make Raft
// believe its log ends somewhere it does not.
func TestLastIndexCacheMatchesScan(t *testing.T) {
	dir, err := os.MkdirTemp("", "lastidx-cache-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	eng, err := lsm.OpenLSM(lsm.LSMOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	s := NewLSMLogStorage(eng)

	check := func(step string) {
		t.Helper()
		cached, err := s.LastIndex()
		if err != nil {
			t.Fatalf("%s: LastIndex: %v", step, err)
		}
		scanned, err := s.scanLastIndex()
		if err != nil {
			t.Fatalf("%s: scanLastIndex: %v", step, err)
		}
		// scanLastIndex does not consult the snapshot anchor, so it is a lower
		// bound; the cached value must never be below it.
		if cached < scanned {
			t.Fatalf("%s: cached LastIndex %d < scanned %d (stale cache)", step, cached, scanned)
		}
	}

	check("empty")

	if err := s.AppendEntries([]Entry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2}, {Term: 1, Index: 3},
	}); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.LastIndex(); got != 3 {
		t.Fatalf("after append: LastIndex want 3, got %d", got)
	}
	check("after append")

	// Appending more must raise it.
	if err := s.AppendEntries([]Entry{{Term: 1, Index: 4}}); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.LastIndex(); got != 4 {
		t.Fatalf("after second append: LastIndex want 4, got %d", got)
	}
	check("after second append")

	// A snapshot anchor beyond the log must be reflected.
	if err := s.SaveSnapshot(Snapshot{
		Metadata: SnapshotMetadata{Index: 10, Term: 2},
		Data:     []byte("snap"),
	}); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.LastIndex(); got != 10 {
		t.Fatalf("after snapshot: LastIndex want 10, got %d", got)
	}
	check("after snapshot")

	// Compaction removes log keys; the answer must not go stale.
	if err := s.Compact(10); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.LastIndex(); got != 10 {
		t.Fatalf("after compact: LastIndex want 10, got %d", got)
	}
	check("after compact")
}
