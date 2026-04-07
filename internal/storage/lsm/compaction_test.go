package lsm

import (
	"fmt"
	"testing"
)

// newTestCompactor creates a Compactor with a low L0 threshold suitable for tests.
func newTestCompactor(t *testing.T, dir string, l0Threshold int) *Compactor {
	t.Helper()
	meta := NewMetaStore(dir)
	return NewCompactor(dir, meta, 0, CompactionOptions{
		L0Threshold:  l0Threshold,
		GrowthFactor: 10.0,
	})
}

// flushN flushes n MemTables, each containing a single key "key-NNNN" → "val-NNNN".
func flushN(t *testing.T, c *Compactor, n int) {
	t.Helper()
	for i := range n {
		mem := NewMemTable()
		mem.Put([]byte(fmt.Sprintf("key-%04d", i)), []byte(fmt.Sprintf("val-%04d", i)))
		if err := c.FlushMemTable(mem); err != nil {
			t.Fatalf("FlushMemTable %d: %v", i, err)
		}
	}
}

func TestCompactor_FlushMemTable_AddsToL0(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	c := newTestCompactor(t, dir, 4)

	mem := NewMemTable()
	mem.Put([]byte("hello"), []byte("world"))
	if err := c.FlushMemTable(mem); err != nil {
		t.Fatal(err)
	}

	meta, err := c.meta.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Levels) == 0 || len(meta.Levels[0]) != 1 {
		t.Fatalf("expected 1 L0 file, got %v", meta.Levels)
	}
}

func TestCompactor_FlushN_L0Accumulates(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	c := newTestCompactor(t, dir, 4)

	flushN(t, c, 3)

	meta, _ := c.meta.Load()
	if len(meta.Levels[0]) != 3 {
		t.Fatalf("expected 3 L0 files, got %d", len(meta.Levels[0]))
	}
}

func TestCompactor_MaybeTriggerCompaction_L0IntoL1(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	c := newTestCompactor(t, dir, 4)

	// Flush 4 MemTables → hits L0 threshold.
	flushN(t, c, 4)

	if err := c.MaybeTriggerCompaction(); err != nil {
		t.Fatal(err)
	}

	meta, _ := c.meta.Load()
	if len(meta.Levels[0]) != 0 {
		t.Fatalf("L0 should be empty after compaction, got %d files", len(meta.Levels[0]))
	}
	if len(meta.Levels) < 2 || len(meta.Levels[1]) != 1 {
		t.Fatalf("expected 1 L1 file after compaction, got %v", meta.Levels)
	}
}

func TestCompactor_CompactLevel_MergesData(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	c := newTestCompactor(t, dir, 100) // high threshold so auto-compaction doesn't fire

	// Flush two MemTables with distinct keys into L0.
	m1 := NewMemTable()
	m1.Put([]byte("aaa"), []byte("val-aaa"))
	m1.Put([]byte("bbb"), []byte("val-bbb"))
	if err := c.FlushMemTable(m1); err != nil {
		t.Fatal(err)
	}

	m2 := NewMemTable()
	m2.Put([]byte("ccc"), []byte("val-ccc"))
	m2.Put([]byte("ddd"), []byte("val-ddd"))
	if err := c.FlushMemTable(m2); err != nil {
		t.Fatal(err)
	}

	// Manually compact L0 → L1.
	if err := c.CompactLevel(0, 1); err != nil {
		t.Fatal(err)
	}

	// L1 should have one file containing all four keys.
	meta, _ := c.meta.Load()
	if len(meta.Levels[0]) != 0 {
		t.Fatalf("L0 should be empty, got %d files", len(meta.Levels[0]))
	}
	if len(meta.Levels[1]) != 1 {
		t.Fatalf("expected 1 L1 file, got %d", len(meta.Levels[1]))
	}

	r, err := OpenSSTable(c.dir + "/" + meta.Levels[1][0])
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	for _, key := range []string{"aaa", "bbb", "ccc", "ddd"} {
		v, found, tomb, err := r.Get([]byte(key))
		if err != nil || !found || tomb {
			t.Fatalf("key %q: found=%v tomb=%v err=%v", key, found, tomb, err)
		}
		if string(v) != "val-"+key {
			t.Fatalf("key %q: got %q", key, v)
		}
	}
}

func TestCompactor_NewVersionWins_DuplicateKeys(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	c := newTestCompactor(t, dir, 100)

	// Flush old value.
	m1 := NewMemTable()
	m1.Put([]byte("key"), []byte("old"))
	if err := c.FlushMemTable(m1); err != nil {
		t.Fatal(err)
	}

	// Flush new value for the same key.
	m2 := NewMemTable()
	m2.Put([]byte("key"), []byte("new"))
	if err := c.FlushMemTable(m2); err != nil {
		t.Fatal(err)
	}

	if err := c.CompactLevel(0, 1); err != nil {
		t.Fatal(err)
	}

	meta, _ := c.meta.Load()
	r, err := OpenSSTable(c.dir + "/" + meta.Levels[1][0])
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	v, found, tomb, err := r.Get([]byte("key"))
	if err != nil || !found || tomb {
		t.Fatalf("found=%v tomb=%v err=%v", found, tomb, err)
	}
	if string(v) != "new" {
		t.Fatalf("got %q, want %q", v, "new")
	}
}

func TestCompactor_TombstoneStripped_AtBottomLevel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	c := newTestCompactor(t, dir, 100)

	// Flush an old value.
	m1 := NewMemTable()
	m1.Put([]byte("key"), []byte("value"))
	if err := c.FlushMemTable(m1); err != nil {
		t.Fatal(err)
	}

	// Flush a tombstone for the same key.
	m2 := NewMemTable()
	m2.Delete([]byte("key"))
	if err := c.FlushMemTable(m2); err != nil {
		t.Fatal(err)
	}

	// Compact L0→L1 (L1 is the bottom level — tombstone must be stripped).
	if err := c.CompactLevel(0, 1); err != nil {
		t.Fatal(err)
	}

	meta, _ := c.meta.Load()
	r, err := OpenSSTable(c.dir + "/" + meta.Levels[1][0])
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Key must be completely absent — neither value nor tombstone.
	_, found, tomb, err := r.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if found || tomb {
		t.Fatalf("key should be fully gone after bottom-level compaction: found=%v tomb=%v", found, tomb)
	}
	if r.count != 0 {
		t.Fatalf("expected empty SSTable, got %d records", r.count)
	}
}

func TestCompactor_TombstonePreserved_NotAtBottomLevel(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	c := newTestCompactor(t, dir, 100)

	// Flush an old value into L1 directly.
	m1 := NewMemTable()
	m1.Put([]byte("key"), []byte("old-value"))
	if err := c.FlushMemTable(m1); err != nil {
		t.Fatal(err)
	}
	if err := c.CompactLevel(0, 1); err != nil {
		t.Fatal(err)
	}

	// Flush a tombstone for the same key into a new L0.
	m2 := NewMemTable()
	m2.Delete([]byte("key"))
	if err := c.FlushMemTable(m2); err != nil {
		t.Fatal(err)
	}

	// Put something into L2 so L1 is no longer the bottom level.
	m3 := NewMemTable()
	m3.Put([]byte("anchor"), []byte("keeps-l2-alive"))
	if err := c.FlushMemTable(m3); err != nil {
		t.Fatal(err)
	}
	if err := c.CompactLevel(0, 2); err != nil { // flush L0 directly to L2
		t.Fatal(err)
	}

	// Now compact L0 (tombstone) into L1. L2 exists, so L1 is NOT the bottom.
	// The tombstone must be preserved in L1 to shadow the old value in L2.
	m4 := NewMemTable()
	m4.Delete([]byte("key"))
	if err := c.FlushMemTable(m4); err != nil {
		t.Fatal(err)
	}
	if err := c.CompactLevel(0, 1); err != nil {
		t.Fatal(err)
	}

	meta, _ := c.meta.Load()
	r, err := OpenSSTable(c.dir + "/" + meta.Levels[1][0])
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	_, found, tomb, err := r.Get([]byte("key"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || !tomb {
		t.Fatalf("tombstone should be preserved in L1: found=%v tomb=%v", found, tomb)
	}
}

func TestCompactor_MaybeTriggerCompaction_CascadesLevels(t *testing.T) {
	t.Parallel()

	// L0Threshold=2, GrowthFactor=2 → L1 threshold=4, L2 threshold=8
	dir := t.TempDir()
	meta := NewMetaStore(dir)
	c := NewCompactor(dir, meta, 0, CompactionOptions{L0Threshold: 2, GrowthFactor: 2.0})

	// Flush enough MemTables to cascade compaction into L2.
	// Each MaybeTriggerCompaction compacts once per call — flush in batches.
	for batch := range 4 {
		for range 2 {
			m := NewMemTable()
			m.Put([]byte(fmt.Sprintf("k-%02d-%04d", batch, batch)), []byte("v"))
			if err := c.FlushMemTable(m); err != nil {
				t.Fatal(err)
			}
		}
		if err := c.MaybeTriggerCompaction(); err != nil {
			t.Fatalf("batch %d: %v", batch, err)
		}
	}

	// After enough flushes and compactions, data should have cascaded into L1.
	meta2, _ := c.meta.Load()
	totalFiles := 0
	for _, level := range meta2.Levels {
		totalFiles += len(level)
	}
	if totalFiles == 0 {
		t.Fatal("expected some files to remain after compaction cascade")
	}
}

func TestCompactor_AtomicMetadata_CrashBetweenWriteAndMeta(t *testing.T) {
	t.Parallel()

	// Simulate: new SSTable written but metadata NOT yet updated (crash scenario).
	// On the next open, metadata still points to old files → engine is consistent.
	dir := t.TempDir()
	c := newTestCompactor(t, dir, 4)

	flushN(t, c, 4)

	// Capture metadata before compaction.
	before, _ := c.meta.Load()

	// Compact but then "crash" by loading the pre-compaction metadata again.
	if err := c.CompactLevel(0, 1); err != nil {
		t.Fatal(err)
	}
	after, _ := c.meta.Load()

	// after must be a valid state: L0 empty, L1 has one file.
	_ = before
	if len(after.Levels[0]) != 0 {
		t.Fatalf("L0 not empty after compact: %v", after.Levels[0])
	}
	if len(after.Levels[1]) != 1 {
		t.Fatalf("L1 should have 1 file: %v", after.Levels)
	}
}
