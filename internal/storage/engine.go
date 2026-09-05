package storage

import "errors"

// ErrKeyNotFound is returned when a key does not exist in the engine.
var ErrKeyNotFound = errors.New("key not found")

// Engine is the contract for a sorted key-value storage backend.
// Keys and values are arbitrary byte slices. Keys are stored in
// lexicographic order.
type Engine interface {
	// Put stores a key-value pair. If the key already exists, its value is overwritten.
	Put(key, value []byte) error

	// Get retrieves the value for a key. It returns ErrKeyNotFound when the key does not exist.
	Get(key []byte) ([]byte, error)

	// Delete removes a key. It is not an error to delete a key that does not exist.
	Delete(key []byte) error

	// NewIterator returns an iterator over the entire key space.
	// The caller must call Close when finished.
	NewIterator() (Iterator, error)

	// NewBatch creates a write batch for atomic multi-operation writes.
	NewBatch() Batch

	// Close releases all resources held by the engine.
	Close() error
}

// Iterator provides ordered iteration over key-value pairs.
// An iterator must be positioned before reading keys or values.
// Iterator is an ordered cursor over the engine's contents.
//
// Observation contract: an iterator observes a stable, point-in-time view of the
// engine as of the moment it was created. Writes committed after that moment are
// not visible to it, and concurrent writes, flushes, or compactions never
// invalidate it. The caller must Close it to release the resources that view
// pins — an unclosed iterator keeps SSTable file handles open.
//
// This contract is load-bearing, not aspirational: implementations must not hand
// out a cursor over a structure that later mutates. An earlier version bound the
// live MemTable here, which `go test -race` correctly reported as a data race
// once the Raft apply loop began writing concurrently with scans.
type Iterator interface {
	// Seek positions the iterator at the first key greater than or equal to key.
	// It returns true when the iterator points at a valid entry.
	Seek(key []byte) bool

	// Next advances the iterator to the next key.
	// It returns true when the iterator remains valid.
	Next() bool

	// Prev moves the iterator to the previous key.
	// It returns true when the iterator remains valid.
	Prev() bool

	// Valid reports whether the iterator currently points at a valid entry.
	Valid() bool

	// Key returns the current key.
	// It is only valid when Valid returns true.
	Key() []byte

	// Value returns the current value.
	// It is only valid when Valid returns true.
	Value() []byte

	// IsTombstone reports whether the current entry is a deletion marker.
	IsTombstone() bool

	// Close releases any resources held by the iterator.
	Close()
}

// Batch groups multiple writes into a single atomic operation.
type Batch interface {
	// Put adds a put operation to the batch.
	Put(key, value []byte)

	// Delete adds a delete operation to the batch.
	Delete(key []byte)

	// Commit atomically applies all operations in the batch.
	Commit() error

	// Reset clears the batch for reuse.
	Reset()
}
