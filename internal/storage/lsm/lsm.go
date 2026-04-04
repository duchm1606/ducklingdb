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
}

// LSMEngine:
// This is the first integrated Phase B engine shape: active WAL + active
// MemTable + persisted SSTables. Fresh writes land in the WAL and MemTable,
// while older frozen MemTable contents are flushed into immutable SSTables.
// Reads merge the active MemTable with SSTables so the engine presents one
// logical keyspace even though data now spans memory and disk.
type LSMEngine struct {
	mu                sync.RWMutex
	dir               string
	wal               *WAL
	mem               *MemTable
	sstables          []*SSTableReader // newest first
	memTableThreshold int
	nextSSTableID     uint64
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

	sstables, nextID, err := loadSSTables(opts.Dir)
	if err != nil {
		return nil, err
	}

	wal, err := OpenWAL(filepath.Join(opts.Dir, lsmWALName))
	if err != nil {
		closeSSTables(sstables)
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	mem := NewMemTable()
	if err := replayWALIntoMemTable(wal, mem); err != nil {
		_ = wal.Close()
		closeSSTables(sstables)
		return nil, err
	}

	return &LSMEngine{
		dir:               opts.Dir,
		wal:               wal,
		mem:               mem,
		sstables:          sstables,
		memTableThreshold: opts.MemTableThreshold,
		nextSSTableID:     nextID,
	}, nil
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

	// Check the active MemTable first (newest data).
	value, found, tombstone := e.mem.Get(key)
	if found {
		if tombstone {
			return nil, storage.ErrKeyNotFound
		}
		return value, nil
	}

	// Search SSTables from newest to oldest.
	for i := len(e.sstables) - 1; i >= 0; i-- {
		value, found, tombstone, err := e.sstables[i].Get(key)
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
	for _, sstable := range e.sstables {
		if err := sstable.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	if err := e.wal.Close(); err != nil && closeErr == nil {
		closeErr = err
	}
	return closeErr
}

func (e *LSMEngine) newMergedIteratorLocked() (storage.Iterator, error) {
	iterators := make([]storage.Iterator, 0, 1+len(e.sstables))
	iterators = append(iterators, e.mem.NewIterator())
	for i := len(e.sstables) - 1; i >= 0; i-- {
		iterators = append(iterators, e.sstables[i].NewIterator())
	}
	return NewDeletedFilterIterator(NewMergeIterator(iterators)), nil
}

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
	path := filepath.Join(e.dir, sstableFilename(e.nextSSTableID))
	if err := WriteSSTableFromIterator(path, e.mem.NewIterator()); err != nil {
		return fmt.Errorf("flush memtable to sstable: %w", err)
	}

	sstable, err := OpenSSTable(path)
	if err != nil {
		return fmt.Errorf("open flushed sstable: %w", err)
	}

	e.sstables = append(e.sstables, sstable)
	e.nextSSTableID++
	e.mem = NewMemTable()
	if err := e.wal.Truncate(); err != nil {
		return fmt.Errorf("truncate WAL after flush: %w", err)
	}

	return nil
}

func loadSSTables(dir string) ([]*SSTableReader, uint64, error) {
	paths, err := filepath.Glob(filepath.Join(dir, sstablePattern))
	if err != nil {
		return nil, 0, fmt.Errorf("glob sstables: %w", err)
	}
	sort.Strings(paths)

	sstables := make([]*SSTableReader, 0, len(paths))
	var nextID uint64
	for _, path := range paths {
		sstable, err := OpenSSTable(path)
		if err != nil {
			closeSSTables(sstables)
			return nil, 0, fmt.Errorf("open sstable %q: %w", path, err)
		}
		sstables = append(sstables, sstable)

		var id uint64
		if _, err := fmt.Sscanf(filepath.Base(path), "sst-%06d.sst", &id); err == nil && id >= nextID {
			nextID = id + 1
		}
	}

	return sstables, nextID, nil
}

func closeSSTables(sstables []*SSTableReader) {
	for _, sstable := range sstables {
		_ = sstable.Close()
	}
}

func sstableFilename(id uint64) string {
	return fmt.Sprintf("sst-%06d.sst", id)
}

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
