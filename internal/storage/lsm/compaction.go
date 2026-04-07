package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

const (
	defaultL0Threshold  = 4
	defaultGrowthFactor = 10.0
)

// CompactionOptions controls when compaction is triggered.
type CompactionOptions struct {
	// L0Threshold is the number of L0 SSTables that triggers an L0→L1 compaction.
	// Default: 4.
	L0Threshold int

	// GrowthFactor determines the target size ratio between adjacent levels.
	// Level n is compacted into level n+1 when len(Levels[n]) >= threshold(n),
	// where threshold(0)=L0Threshold and threshold(n)=threshold(n-1)*GrowthFactor.
	// Default: 10.0.
	GrowthFactor float64
}

// Compactor manages flushing MemTables and merging SSTables across levels.
//
// It works alongside the MetaStore: every change to the on-disk SSTable set
// is recorded atomically (new files written → metadata saved → old files
// deleted). A crash at any point leaves the engine in a state it can recover
// from on the next open.
//
// All operations are serialized by an internal mutex so they are safe to call
// from multiple goroutines, but for throughput the caller should schedule
// MaybeTriggerCompaction outside the hot write path.
type Compactor struct {
	dir    string
	meta   *MetaStore
	nextID atomic.Uint64
	opts   CompactionOptions
	mu     sync.Mutex
}

// NewCompactor creates a Compactor. nextID is the first SSTable ID to assign
// (typically derived from the highest existing file on disk).
func NewCompactor(dir string, meta *MetaStore, nextID uint64, opts CompactionOptions) *Compactor {
	if opts.L0Threshold <= 0 {
		opts.L0Threshold = defaultL0Threshold
	}
	if opts.GrowthFactor <= 0 {
		opts.GrowthFactor = defaultGrowthFactor
	}
	c := &Compactor{dir: dir, meta: meta, opts: opts}
	c.nextID.Store(nextID)
	return c
}

// NextID returns the current next SSTable ID without incrementing it.
// Useful for the LSM engine to stay in sync.
func (c *Compactor) NextID() uint64 {
	return c.nextID.Load()
}

// FlushMemTable writes mem to a new L0 SSTable and prepends it to Levels[0]
// in the metadata. The WAL is not touched here — the caller is responsible
// for truncating it after a successful flush.
func (c *Compactor) FlushMemTable(mem *MemTable) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	name := sstableFilename(c.allocID())
	path := filepath.Join(c.dir, name)

	if err := WriteSSTableFromIterator(path, mem.NewIterator()); err != nil {
		return fmt.Errorf("flush memtable: %w", err)
	}

	meta, err := c.meta.Load()
	if err != nil {
		return err
	}

	if len(meta.Levels) == 0 {
		meta.Levels = [][]string{{name}}
	} else {
		// Prepend so the newest L0 file is at index 0 — important for read order.
		meta.Levels[0] = append([]string{name}, meta.Levels[0]...)
	}

	return c.meta.Save(meta)
}

// MaybeTriggerCompaction checks every level from L0 upward and triggers
// compaction wherever the file count exceeds the level's threshold.
// It loops until no level needs compaction.
func (c *Compactor) MaybeTriggerCompaction() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for {
		meta, err := c.meta.Load()
		if err != nil {
			return err
		}

		level := c.firstOverThreshold(meta)
		if level < 0 {
			return nil // nothing to do
		}

		if err := c.compactLevelLocked(level, level+1); err != nil {
			return fmt.Errorf("compact level %d→%d: %w", level, level+1, err)
		}
		// loop: re-read metadata and check again — the compaction may have pushed
		// data into level+1, which might now also exceed its threshold.
	}
}

// CompactLevel merges all SSTables at fromLevel with those at toLevel,
// writes a new SSTable into toLevel, updates metadata atomically, then
// deletes the old files.
func (c *Compactor) CompactLevel(fromLevel, toLevel int) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.compactLevelLocked(fromLevel, toLevel)
}

// --- internal helpers --------------------------------------------------------

func (c *Compactor) allocID() uint64 {
	return c.nextID.Add(1) - 1
}

// firstOverThreshold returns the first level index whose file count exceeds
// its threshold, or -1 if every level is within bounds.
func (c *Compactor) firstOverThreshold(meta MetaData) int {
	for level := range meta.Levels {
		if len(meta.Levels[level]) >= c.levelThreshold(level) {
			return level
		}
	}
	return -1
}

// levelThreshold returns the file-count threshold for a given level.
// threshold(0) = L0Threshold; threshold(n) = threshold(n-1) * GrowthFactor.
func (c *Compactor) levelThreshold(level int) int {
	t := c.opts.L0Threshold
	for i := 0; i < level; i++ {
		t = int(float64(t) * c.opts.GrowthFactor)
	}
	return t
}

// compactLevelLocked performs the actual compaction without acquiring the mutex
// (caller must hold it).
//
// Algorithm:
//  1. Open all SSTables from fromLevel and toLevel.
//  2. Build a MergeIterator over them — newer sources (fromLevel) sort first
//     because iters[0..len(fromFiles)-1] have lower indices.
//  3. If toLevel is the deepest level, wrap with DeletedFilterIterator to strip
//     tombstones — no older data exists that they need to shadow.
//  4. Write a single new SSTable from the merged stream.
//  5. Update metadata: clear fromLevel, replace toLevel with the new file. Fsync.
//  6. Delete the old files (best-effort).
func (c *Compactor) compactLevelLocked(fromLevel, toLevel int) error {
	meta, err := c.meta.Load()
	if err != nil {
		return err
	}

	// Grow the levels slice if toLevel doesn't exist yet.
	for len(meta.Levels) <= toLevel {
		meta.Levels = append(meta.Levels, nil)
	}

	fromFiles := meta.Levels[fromLevel]
	toFiles := meta.Levels[toLevel]

	if len(fromFiles) == 0 {
		return nil
	}

	// Open readers for every file involved in this compaction.
	allFiles := append(append([]string{}, fromFiles...), toFiles...)
	readers := make([]*SSTableReader, 0, len(allFiles))
	for _, name := range allFiles {
		r, err := OpenSSTable(filepath.Join(c.dir, name))
		if err != nil {
			closeSSTableReaders(readers)
			return fmt.Errorf("open %q: %w", name, err)
		}
		readers = append(readers, r)
	}
	defer closeSSTableReaders(readers)

	// Build merge iterator. fromLevel files come first so their records win
	// over same-key records from toLevel (newer beats older).
	iters := make([]storage.Iterator, len(readers))
	for i, r := range readers {
		iters[i] = r.NewIterator()
	}
	var merged storage.Iterator = NewMergeIterator(iters)

	// Strip tombstones only at the bottom level — they are still needed above.
	if c.isBottomLevel(meta, toLevel) {
		merged = NewDeletedFilterIterator(merged)
	}

	// Write the compacted output. WriteSSTableFromIterator calls iter.Close().
	newName := sstableFilename(c.allocID())
	newPath := filepath.Join(c.dir, newName)
	if err := WriteSSTableFromIterator(newPath, merged); err != nil {
		return fmt.Errorf("write compacted sstable: %w", err)
	}

	// Atomic metadata update: new file is on disk, now make it official.
	meta.Levels[fromLevel] = nil
	meta.Levels[toLevel] = []string{newName}
	meta.Levels = trimTrailingEmptyLevels(meta.Levels)
	if err := c.meta.Save(meta); err != nil {
		return fmt.Errorf("save metadata: %w", err)
	}

	// Best-effort cleanup: orphaned files are harmless but waste space.
	for _, name := range allFiles {
		_ = os.Remove(filepath.Join(c.dir, name))
	}

	return nil
}

// isBottomLevel returns true when no level below toLevel contains any files.
func (c *Compactor) isBottomLevel(meta MetaData, toLevel int) bool {
	for i := toLevel + 1; i < len(meta.Levels); i++ {
		if len(meta.Levels[i]) > 0 {
			return false
		}
	}
	return true
}

// trimTrailingEmptyLevels removes empty levels from the end of the slice,
// keeping at least one entry if the slice is non-empty.
func trimTrailingEmptyLevels(levels [][]string) [][]string {
	for len(levels) > 1 && len(levels[len(levels)-1]) == 0 {
		levels = levels[:len(levels)-1]
	}
	return levels
}

func closeSSTableReaders(readers []*SSTableReader) {
	for _, r := range readers {
		_ = r.Close()
	}
}
