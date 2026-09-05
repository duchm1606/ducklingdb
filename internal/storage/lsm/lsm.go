package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

const (
	defaultMemTableThreshold = 4 << 20 // 4 MiB
	lsmWALName               = "wal"
	sstablePattern           = "sst-*.sst"
)

var _ storage.Engine = (*LSMEngine)(nil)

type LSMOptions struct {
	Dir               string
	MemTableThreshold int
	L0Threshold       int
	GrowthFactor      float64
}

// LSMEngine is the complete LSM storage engine.
//
// Write path: WAL append → MemTable insert → if threshold exceeded, freeze
// MemTable, flush to a new L0 SSTable via the Compactor, truncate WAL, then
// trigger compaction to merge levels as needed.
//
// Read path: active MemTable → L0 SSTables (newest first) → L1, L2, … with
// tombstone filtering applied at the iterator layer for scans.
//
// Crash safety: every metadata change goes through the dual-slot MetaStore.
// A crash at any point leaves the engine in a consistent state.
type LSMEngine struct {
	mu                sync.RWMutex
	dir               string
	wal               *WAL
	mem               *MemTable
	levels            [][]*SSTableReader // levels[i][j] = j-th SSTable at level i; L0[0] is newest
	memTableThreshold int
	meta              *MetaStore
	compactor         *Compactor
}

func OpenLSM(opts LSMOptions) (*LSMEngine, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("lsm: empty directory")
	}
	if opts.MemTableThreshold <= 0 {
		opts.MemTableThreshold = defaultMemTableThreshold
	}

	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}

	meta := NewMetaStore(opts.Dir)

	// Derive the next SSTable ID by scanning the directory for existing files.
	// This is intentionally directory-based, not metadata-based, so that
	// orphaned files from crashed compactions are never overwritten.
	nextID, err := scanDirForNextID(opts.Dir)
	if err != nil {
		return nil, err
	}

	compactor := NewCompactor(opts.Dir, meta, nextID, CompactionOptions{
		L0Threshold:  opts.L0Threshold,
		GrowthFactor: opts.GrowthFactor,
	})

	engine := &LSMEngine{
		dir:               opts.Dir,
		memTableThreshold: opts.MemTableThreshold,
		meta:              meta,
		compactor:         compactor,
	}

	// Load SSTables from metadata (not glob — metadata is the source of truth).
	if err := engine.reloadLevels(); err != nil {
		return nil, err
	}

	wal, err := OpenWAL(filepath.Join(opts.Dir, lsmWALName))
	if err != nil {
		engine.closeAllReaders()
		return nil, fmt.Errorf("open WAL: %w", err)
	}
	engine.wal = wal

	mem := NewMemTable()
	if err := replayWALIntoMemTable(wal, mem); err != nil {
		_ = wal.Close()
		engine.closeAllReaders()
		return nil, err
	}
	engine.mem = mem

	return engine, nil
}

func (e *LSMEngine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Append(&Entry{Key: key, Value: value, Op: OpPut}); err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	e.mem.Put(key, value)
	return e.maybeFlushLocked()
}

func (e *LSMEngine) Get(key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	// Active MemTable first — always the freshest source.
	if value, found, tombstone := e.mem.Get(key); found {
		if tombstone {
			return nil, storage.ErrKeyNotFound
		}
		return value, nil
	}

	// L0 SSTables newest-first (levels[0][0] is the most recently flushed).
	// L0 files can overlap, so we must check every one.
	if len(e.levels) > 0 {
		for _, r := range e.levels[0] {
			value, found, tombstone, err := r.Get(key)
			if err != nil {
				return nil, err
			}
			if found {
				if tombstone {
					return nil, storage.ErrKeyNotFound
				}
				return value, nil
			}
		}
	}

	// L1, L2, … — non-overlapping within each level; linear scan is correct.
	for _, level := range e.levelSlice(1) {
		for _, r := range level {
			value, found, tombstone, err := r.Get(key)
			if err != nil {
				return nil, err
			}
			if found {
				if tombstone {
					return nil, storage.ErrKeyNotFound
				}
				return value, nil
			}
		}
	}

	return nil, storage.ErrKeyNotFound
}

func (e *LSMEngine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.wal.Append(&Entry{Key: key, Op: OpDelete}); err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	e.mem.Delete(key)
	return e.maybeFlushLocked()
}

func (e *LSMEngine) NewIterator() (storage.Iterator, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.newMergedIteratorLocked()
}

func (e *LSMEngine) NewBatch() storage.Batch {
	return &lsmBatch{engine: e}
}

func (e *LSMEngine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.flushActiveMemTableLocked(); err != nil {
		return err
	}

	var closeErr error
	e.closeAllReaders()
	if err := e.wal.Close(); err != nil {
		closeErr = err
	}
	return closeErr
}

// --- internal helpers --------------------------------------------------------

func (e *LSMEngine) maybeFlushLocked() error {
	if e.mem.Size() < e.memTableThreshold {
		return nil
	}
	return e.flushActiveMemTableLocked()
}

func (e *LSMEngine) flushActiveMemTableLocked() error {
	if e.mem.Len() == 0 {
		return nil
	}

	e.mem.Freeze()

	// Step 1: write SSTable + update metadata (crash-safe via MetaStore).
	if err := e.compactor.FlushMemTable(e.mem); err != nil {
		return fmt.Errorf("flush memtable: %w", err)
	}

	// Step 2: activate the new MemTable and clear the WAL.
	e.mem = NewMemTable()
	if err := e.wal.Truncate(); err != nil {
		return fmt.Errorf("truncate WAL: %w", err)
	}

	// Step 3: reload so Get sees the new L0 SSTable.
	if err := e.reloadLevels(); err != nil {
		return err
	}

	// Step 4: compact if any level is over threshold, then sync readers again.
	if err := e.compactor.MaybeTriggerCompaction(); err != nil {
		return fmt.Errorf("compaction: %w", err)
	}

	return e.reloadLevels()
}

// reloadLevels closes all existing SSTableReaders and reopens them from the
// current MetaStore snapshot.  Called after every flush or compaction cycle.
func (e *LSMEngine) reloadLevels() error {
	e.closeAllReaders()

	meta, err := e.meta.Load()
	if err != nil {
		return fmt.Errorf("load metadata: %w", err)
	}

	e.levels = make([][]*SSTableReader, len(meta.Levels))
	for i, files := range meta.Levels {
		e.levels[i] = make([]*SSTableReader, len(files))
		for j, name := range files {
			r, err := OpenSSTable(filepath.Join(e.dir, name))
			if err != nil {
				// Release everything opened so far and leave e.levels empty
				// rather than half-populated. The previous hand-unwind left nil
				// readers in e.levels (nil deref on the next Get or iterator)
				// and left the slice in place, so a later Close() released the
				// survivors a second time and drove their refcount negative.
				e.levels[i] = e.levels[i][:j]
				e.closeAllReaders()
				return fmt.Errorf("open %q: %w", name, err)
			}
			e.levels[i][j] = r
		}
	}
	return nil
}

func (e *LSMEngine) closeAllReaders() {
	for _, level := range e.levels {
		for _, r := range level {
			_ = r.Close()
		}
	}
	e.levels = nil
}

// newMergedIteratorLocked builds a merge iterator with priority order:
// MemTable (freshest) → L0 newest→oldest → L1 → L2 → …
// Lower index in the iterator slice = higher priority on key collision.
func (e *LSMEngine) newMergedIteratorLocked() (storage.Iterator, error) {
	iters := make([]storage.Iterator, 0, 1+e.totalSSTableCount())

	// Snapshot the active MemTable rather than binding it. The live skiplist
	// keeps being mutated by Put/Delete after this function returns, and the
	// iterator outlives the lock we are holding right now.
	iters = append(iters, e.mem.Snapshot().NewIterator())

	// Pin every SSTable reader we hand to the iterator, so a concurrent flush
	// or compaction (which calls reloadLevels → closeAllReaders) cannot close
	// the file handle while the iterator is still reading it.
	pinned := make([]*SSTableReader, 0, e.totalSSTableCount())
	pin := func(r *SSTableReader) {
		r.Ref()
		pinned = append(pinned, r)
		iters = append(iters, r.NewIterator())
	}

	// L0 newest-first.
	if len(e.levels) > 0 {
		for _, r := range e.levels[0] {
			pin(r)
		}
	}
	// L1 and below.
	for _, level := range e.levelSlice(1) {
		for _, r := range level {
			pin(r)
		}
	}

	return &pinnedIterator{
		Iterator: NewDeletedFilterIterator(NewMergeIterator(iters)),
		pinned:   pinned,
	}, nil
}

// pinnedIterator holds references on the SSTable readers its inner iterator was
// built over, releasing them when the iterator is closed.
//
// This is what makes the storage.Iterator observation contract hold: the
// iterator sees a stable set of files for its whole lifetime, even across a
// flush or compaction cycle.
type pinnedIterator struct {
	storage.Iterator
	pinned []*SSTableReader
}

func (p *pinnedIterator) Close() {
	p.Iterator.Close()
	for _, r := range p.pinned {
		_ = r.Unref()
	}
	p.pinned = nil
}

// levelSlice returns e.levels[from:] safely (returns nil if from >= len).
func (e *LSMEngine) levelSlice(from int) [][]*SSTableReader {
	if from >= len(e.levels) {
		return nil
	}
	return e.levels[from:]
}

func (e *LSMEngine) totalSSTableCount() int {
	n := 0
	for _, level := range e.levels {
		n += len(level)
	}
	return n
}

func sstableFilename(id uint64) string {
	return fmt.Sprintf("sst-%06d.sst", id)
}

// scanDirForNextID scans the directory for existing SSTable files and returns
// max(id)+1, ensuring new files never collide with any on-disk file (including
// orphans from crashed compactions that are absent from metadata).
func scanDirForNextID(dir string) (uint64, error) {
	paths, err := filepath.Glob(filepath.Join(dir, sstablePattern))
	if err != nil {
		return 0, fmt.Errorf("glob sstables: %w", err)
	}
	sort.Strings(paths)

	var nextID uint64
	for _, path := range paths {
		var id uint64
		if _, err := fmt.Sscanf(filepath.Base(path), "sst-%06d.sst", &id); err == nil {
			if id+1 > nextID {
				nextID = id + 1
			}
		}
	}
	return nextID, nil
}

// --- batch -------------------------------------------------------------------

type lsmBatch struct {
	engine  *LSMEngine
	entries []Entry
}

func (b *lsmBatch) Put(key, value []byte) {
	b.entries = append(b.entries, Entry{
		Key:   append([]byte{}, key...),
		Value: append([]byte{}, value...),
		Op:    OpPut,
	})
}

func (b *lsmBatch) Delete(key []byte) {
	b.entries = append(b.entries, Entry{
		Key: append([]byte{}, key...),
		Op:  OpDelete,
	})
}

func (b *lsmBatch) Commit() error {
	b.engine.mu.Lock()
	defer b.engine.mu.Unlock()

	if err := b.engine.wal.WriteAll(b.entries); err != nil {
		return fmt.Errorf("batch WAL write: %w", err)
	}

	applyEntriesToMemTable(b.engine.mem, b.entries)
	return b.engine.maybeFlushLocked()
}

func (b *lsmBatch) Reset() {
	b.entries = b.entries[:0]
}
