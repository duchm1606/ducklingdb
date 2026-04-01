package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

const basicKVWALName = "wal"

var _ storage.Engine = (*BasicKV)(nil)

// BasicKV is the first durable key-value engine in DucklingDB.
// It combines a WAL for durability and a MemTable for fast reads.
type BasicKV struct {
	mu  sync.RWMutex
	dir string
	wal *WAL
	mem *MemTable
}

// OpenBasicKV opens or creates a BasicKV engine in dir.
// It replays the WAL into a fresh MemTable on startup.
func OpenBasicKV(dir string) (*BasicKV, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create dir: %w", err)
	}

	walPath := filepath.Join(dir, basicKVWALName)
	wal, err := OpenWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	mem := NewMemTable()
	if err := replayWALIntoMemTable(wal, mem); err != nil {
		_ = wal.Close()
		return nil, err
	}

	return &BasicKV{
		dir: dir,
		wal: wal,
		mem: mem,
	}, nil
}

func replayWALIntoMemTable(wal *WAL, mem *MemTable) error {
	entries, err := wal.ReadAll()
	if err != nil {
		return fmt.Errorf("read WAL: %w", err)
	}

	for _, entry := range entries {
		switch entry.Op {
		case OpPut:
			mem.Put(entry.Key, entry.Value)
		case OpDelete:
			mem.Delete(entry.Key)
		default:
			return fmt.Errorf("replay WAL: unknown op %d", entry.Op)
		}
	}

	return nil
}

// Put durably appends the write to the WAL, then applies it to the MemTable.
func (kv *BasicKV) Put(key, value []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err := kv.wal.Append(&Entry{Key: key, Value: value, Op: OpPut}); err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	kv.mem.Put(key, value)
	return nil
}

// Get returns the latest value for key from the MemTable.
func (kv *BasicKV) Get(key []byte) ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	value, found, tombstone := kv.mem.Get(key)
	if !found || tombstone {
		return nil, storage.ErrKeyNotFound
	}

	return value, nil
}

// Delete durably appends a tombstone to the WAL, then applies it to the MemTable.
func (kv *BasicKV) Delete(key []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err := kv.wal.Append(&Entry{Key: key, Op: OpDelete}); err != nil {
		return fmt.Errorf("WAL append: %w", err)
	}

	kv.mem.Delete(key)
	return nil
}

// NewIterator returns an iterator over live keys only.
// Tombstones are filtered out from the underlying MemTable iterator.
func (kv *BasicKV) NewIterator() (storage.Iterator, error) {
	kv.mu.RLock()
	inner := kv.mem.NewIterator()
	return &noTombstoneIterator{inner: inner, mu: &kv.mu}, nil
}

type noTombstoneIterator struct {
	inner *MemTableIterator
	mu    *sync.RWMutex
}

func (it *noTombstoneIterator) Seek(key []byte) bool {
	if !it.inner.Seek(key) {
		return false
	}

	for it.inner.Valid() && it.inner.IsTombstone() {
		it.inner.Next()
	}

	return it.inner.Valid()
}

func (it *noTombstoneIterator) Next() bool {
	for it.inner.Next() {
		if !it.inner.IsTombstone() {
			return true
		}
	}

	return false
}

func (it *noTombstoneIterator) Prev() bool {
	for it.inner.Prev() {
		if !it.inner.IsTombstone() {
			return true
		}
	}

	return false
}

func (it *noTombstoneIterator) Valid() bool {
	return it.inner.Valid()
}

func (it *noTombstoneIterator) Key() []byte {
	return it.inner.Key()
}

func (it *noTombstoneIterator) Value() []byte {
	return it.inner.Value()
}

func (it *noTombstoneIterator) IsTombstone() bool {
	return false
}

func (it *noTombstoneIterator) Close() {
	it.inner.Close()
	it.mu.RUnlock()
}

// NewBatch creates a write batch that amortizes the WAL fsync cost.
func (kv *BasicKV) NewBatch() storage.Batch {
	return &basicBatch{kv: kv}
}

type basicBatch struct {
	kv      *BasicKV
	entries []Entry
}

func (b *basicBatch) Put(key, value []byte) {
	b.entries = append(b.entries, Entry{
		Key:   append([]byte{}, key...),
		Value: append([]byte{}, value...),
		Op:    OpPut,
	})
}

func (b *basicBatch) Delete(key []byte) {
	b.entries = append(b.entries, Entry{
		Key: append([]byte{}, key...),
		Op:  OpDelete,
	})
}

func (b *basicBatch) Commit() error {
	b.kv.mu.Lock()
	defer b.kv.mu.Unlock()

	for _, entry := range b.entries {
		if _, err := b.kv.wal.fp.Write(entry.Encode()); err != nil {
			return fmt.Errorf("batch WAL write: %w", err)
		}
	}

	if err := b.kv.wal.fp.Sync(); err != nil {
		return fmt.Errorf("batch WAL sync: %w", err)
	}

	for _, entry := range b.entries {
		switch entry.Op {
		case OpPut:
			b.kv.mem.Put(entry.Key, entry.Value)
		case OpDelete:
			b.kv.mem.Delete(entry.Key)
		}
	}

	return nil
}

func (b *basicBatch) Reset() {
	b.entries = b.entries[:0]
}

// Close releases the WAL file handle.
func (kv *BasicKV) Close() error {
	return kv.wal.Close()
}
