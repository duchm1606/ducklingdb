package lsm

import (
	"bytes"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

// MergeIterator:
// Reads in an LSM tree do not come from one place. A key may exist in the
// active MemTable, in older SSTables, or in several sources at once.
// The MergeIterator gives the storage engine one ordered view over many sorted
// sources, while preserving the most important LSM rule: newer sources win.
//
// This iterator does not hide tombstones. It surfaces the raw merged truth so
// higher layers can decide whether to filter deletions (user reads) or preserve
// them (compaction and internal maintenance).
type MergeIterator struct {
	iters  []storage.Iterator
	winner int
}

func NewMergeIterator(iters []storage.Iterator) *MergeIterator {
	merge := &MergeIterator{
		iters:  iters,
		winner: -1,
	}
	merge.pickWinner()
	return merge
}

func (it *MergeIterator) pickWinner() {
	winner := -1
	for idx, iter := range it.iters {
		if !iter.Valid() {
			continue
		}

		if winner == -1 {
			winner = idx
			continue
		}

		cmp := bytes.Compare(iter.Key(), it.iters[winner].Key())
		if cmp < 0 || (cmp == 0 && idx < winner) {
			winner = idx
		}
	}

	it.winner = winner
}

func (it *MergeIterator) Seek(key []byte) bool {
	for _, iter := range it.iters {
		iter.Seek(key)
	}

	it.pickWinner()
	return it.Valid()
}

func (it *MergeIterator) Next() bool {
	if !it.Valid() {
		return false
	}

	currentKey := append([]byte(nil), it.Key()...)
	for _, iter := range it.iters {
		if iter.Valid() && bytes.Equal(iter.Key(), currentKey) {
			iter.Next()
		}
	}

	it.pickWinner()
	return it.Valid()
}

func (it *MergeIterator) Prev() bool {
	if !it.Valid() {
		return false
	}

	currentKey := append([]byte(nil), it.Key()...)
	for _, iter := range it.iters {
		if iter.Valid() && bytes.Equal(iter.Key(), currentKey) {
			iter.Prev()
		}
	}

	it.pickWinner()
	return it.Valid()
}

func (it *MergeIterator) Valid() bool {
	return it.winner >= 0
}

func (it *MergeIterator) Key() []byte {
	return it.iters[it.winner].Key()
}

func (it *MergeIterator) Value() []byte {
	return it.iters[it.winner].Value()
}

func (it *MergeIterator) IsTombstone() bool {
	return it.iters[it.winner].IsTombstone()
}

func (it *MergeIterator) Close() {
	for _, iter := range it.iters {
		iter.Close()
	}
}

// DeletedFilterIterator:
// The raw merge iterator must surface tombstones because they are part of the
// storage truth and compaction needs to preserve them. User-facing reads,
// however, usually want the logical view where deleted keys simply disappear.
// This wrapper keeps those responsibilities separate by filtering tombstones
// from any underlying iterator without changing the raw merge behavior.
type DeletedFilterIterator struct {
	inner storage.Iterator
}

func NewDeletedFilterIterator(inner storage.Iterator) *DeletedFilterIterator {
	return &DeletedFilterIterator{inner: inner}
}

func (it *DeletedFilterIterator) Seek(key []byte) bool {
	if !it.inner.Seek(key) {
		return false
	}

	for it.inner.Valid() && it.inner.IsTombstone() {
		it.inner.Next()
	}

	return it.inner.Valid()
}

func (it *DeletedFilterIterator) Next() bool {
	for it.inner.Next() {
		if !it.inner.IsTombstone() {
			return true
		}
	}

	return false
}

func (it *DeletedFilterIterator) Prev() bool {
	for it.inner.Prev() {
		if !it.inner.IsTombstone() {
			return true
		}
	}

	return false
}

func (it *DeletedFilterIterator) Valid() bool {
	return it.inner.Valid()
}

func (it *DeletedFilterIterator) Key() []byte {
	return it.inner.Key()
}

func (it *DeletedFilterIterator) Value() []byte {
	return it.inner.Value()
}

func (it *DeletedFilterIterator) IsTombstone() bool {
	return false
}

func (it *DeletedFilterIterator) Close() {
	it.inner.Close()
}
