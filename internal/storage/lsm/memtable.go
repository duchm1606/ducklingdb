package lsm

import (
	"bytes"
	"math/rand"
	"sort"
)

const maxMemTableLevel = 16

type memLevel struct {
	forward *memNode
}

type memNode struct {
	key       []byte
	value     []byte
	tombstone bool
	backward  *memNode
	levels    []memLevel
}

type MemTable struct {
	head   *memNode
	tail   *memNode
	level  int
	length int
	size   int
	frozen bool
}

// MemTableFreeze:
// Flush integration depends on a simple but critical invariant: once a MemTable
// is handed off for SSTable creation, it must stop changing. Freeze marks the
// table immutable so the engine can safely iterate and flush a stable snapshot
// while newer writes move to a different active MemTable.
//
// A frozen MemTable is still readable. It simply stops accepting mutations.

// The head node should have maxMemTableLevel levels, even when the MemTable is empty. That gives you a fixed top anchor for future searches.
// The MemTable’s current level should start at 1, not 0. Even an empty skip list has a bottom level.
func NewMemTable() *MemTable {
	head := &memNode{
		levels: make([]memLevel, maxMemTableLevel),
	}
	return &MemTable{
		head:   head,
		level:  1,
		length: 0,
		size:   0,
	}
}

func newMemNode(level int, key, value []byte, tombstone bool) *memNode {
	return &memNode{
		key:       key,
		value:     value,
		tombstone: tombstone,
		levels:    make([]memLevel, level),
	}
}

func randomLevel() int {
	level := 1
	for level < maxMemTableLevel && rand.Intn(2) == 1 {
		level++
	}
	return level
}

// findPath finds the path to the node with the given key.
// It returns the update array and the node to insert the new node after.
//
// How the algorithm works:
//
// At each level:
//
// - move forward while the next key is still < target
//
// - stop when moving again would overshoot
//
// - record the current node as the predecessor for that level: update[level] = x
//
// After you finish level 0, the first candidate node is: `next := x.levels[0].forward`
//
// If next.key == key, you found the exact node. If not, update already tells you where a new node should be inserted.
func (m *MemTable) findPath(key []byte) ([]*memNode, *memNode) {
	update := make([]*memNode, maxMemTableLevel)
	x := m.head
	for level := m.level - 1; level >= 0; level-- {
		for x.levels[level].forward != nil && bytes.Compare(x.levels[level].forward.key, key) < 0 {
			x = x.levels[level].forward
		}
		update[level] = x
	}

	next := x.levels[0].forward
	if next != nil && bytes.Equal(next.key, key) {
		return update, next
	}
	return update, nil
}

// insertNode inserts a new node into the MemTable.
//
// How the algorithm works:
//
// If the new node’s height is greater than the current MemTable height, you extend the active height and set missing predecessors to `m.head`.
//
// Then for each level that the node participates in:
//
// - point the new node forward to what used to come next
//
// - point the predecessor forward to the new node
//
// At level 0, also maintain:
//
// - `backward`
//
// - `tail`
//
// Those matter later for iteration and reverse iteration.
func (m *MemTable) insertNode(node *memNode, update []*memNode) {
	nodeLevel := len(node.levels)
	// Extend the active height if needed
	if nodeLevel > m.level {
		for level := m.level; level < nodeLevel; level++ {
			update[level] = m.head
		}
		m.level = nodeLevel
	}
	// Wire forward pointer
	for level := 0; level < nodeLevel; level++ {
		// node.forward = predecessor.forward (at `level-th` level)
		node.levels[level].forward = update[level].levels[level].forward
		// predecessor.forward = node (at `level-th` level)
		update[level].levels[level].forward = node
	}
	// Wire backward pointer
	// backward only exists at level 0. If the predecessor is head (sentinel), we set backward = nil because there's no real previous node. Otherwise, backward points to the predecessor.
	if update[0] == m.head {
		node.backward = nil // node is the new first element
	} else {
		node.backward = update[0] // point back to level-0 predecessor
	}
	// Fix successor's backward pointer
	if node.levels[0].forward != nil {
		node.levels[0].forward.backward = node // old successor points back to us
	} else {
		m.tail = node // we're the new rightmost node.
	}
	// Edge case for first insertion
	if m.tail == nil {
		m.tail = node
	}
	m.length++
}

// Put inserts a new key-value pair into the MemTable.
//
// If the key already exists:
// - replace the value
// - clear tombstone
// - adjust `m.size`
//
// If the existing node was a tombstone, you need to count the key/value size transition carefully.
// For this phase, a simple and consistent rule is enough:
// - new live entry contributes `len(key) + len(value)`
// - tombstone contributes roughly `len(key)`
// That is why converting tombstone → live value needs extra size adjustment.
func (m *MemTable) Put(key, value []byte) {
	if m.frozen {
		panic("lsm: Put on frozen MemTable")
	}

	update, found := m.findPath(key)
	if found != nil {
		oldValueLen := len(found.value)
		wasTombstone := found.tombstone
		found.value = value
		found.tombstone = false
		m.size += len(value) - oldValueLen
		if wasTombstone {
			m.size += len(key)
		}
		return
	}
	node := newMemNode(randomLevel(), key, value, false)
	m.insertNode(node, update)
	m.size += len(key) + len(value)
}

// Get retrieves the value for a key from the MemTable.
//
// If the key does not exist, it returns (nil, false, false).
// If the key exists and is a tombstone, it returns (nil, true, true).
// If the key exists and is a live value, it returns (value, true, false).
func (m *MemTable) Get(key []byte) ([]byte, bool, bool) {
	_, found := m.findPath(key)
	if found == nil {
		return nil, false, false
	}
	if found.tombstone {
		return nil, true, true
	}
	return found.value, true, false
}

// Delete mark a key as deleted.
//
// - if the key exists, turn it into a tombstone
// - if the key does not exist, insert a tombstone anyway
func (m *MemTable) Delete(key []byte) {
	if m.frozen {
		panic("lsm: Delete on frozen MemTable")
	}

	update, found := m.findPath(key)
	if found != nil {
		if found.tombstone {
			return
		}

		m.size -= len(found.value)
		found.value = nil
		found.tombstone = true
		return
	}

	node := newMemNode(randomLevel(), key, nil, true)
	m.insertNode(node, update)
	m.size += len(key)
}

// applyEntriesToMemTable replays a slice of entries into a MemTable.
func applyEntriesToMemTable(mem *MemTable, entries []Entry) {
	for _, entry := range entries {
		switch entry.Op {
		case OpPut:
			mem.Put(entry.Key, entry.Value)
		case OpDelete:
			mem.Delete(entry.Key)
		}
	}
}

func (m *MemTable) Freeze() {
	m.frozen = true
}

func (m *MemTable) IsFrozen() bool {
	return m.frozen
}

func (m *MemTable) Size() int {
	return m.size
}
func (m *MemTable) Len() int {
	return m.length
}

// MemTableIterator:
// The iterator is simple because all the hard work was done during insertion — the level-0 forward and backward pointers form a sorted doubly-linked list.
type MemTableIterator struct {
	curr *memNode
	mem  *MemTable
}

func (m *MemTable) NewIterator() *MemTableIterator {
	return &MemTableIterator{mem: m}
}

func (m *MemTable) seekNode(key []byte) *memNode {
	x := m.head
	for level := m.level - 1; level >= 0; level-- {
		for x.levels[level].forward != nil && bytes.Compare(x.levels[level].forward.key, key) < 0 {
			x = x.levels[level].forward
		}
	}
	return x.levels[0].forward
}

func (it *MemTableIterator) Seek(key []byte) bool {
	it.curr = it.mem.seekNode(key)
	return it.Valid()
}

func (it *MemTableIterator) Next() bool {
	if it.curr != nil {
		it.curr = it.curr.levels[0].forward
	}
	return it.Valid()
}

func (it *MemTableIterator) Prev() bool {
	if it.curr != nil {
		it.curr = it.curr.backward
	}
	return it.Valid()
}

func (it *MemTableIterator) Valid() bool {
	return it.curr != nil
}

func (it *MemTableIterator) Key() []byte {
	return it.curr.key
}

func (it *MemTableIterator) Value() []byte {
	return it.curr.value
}

func (it *MemTableIterator) IsTombstone() bool {
	return it.curr.tombstone
}

func (it *MemTableIterator) Close() {}

// memEntry is one immutable key/value/tombstone triple captured from a MemTable.
type memEntry struct {
	key       []byte
	value     []byte
	tombstone bool
}

// MemTableSnapshot is an immutable, point-in-time copy of a MemTable's contents.
//
// Why this exists: MemTableIterator walks the *live* skiplist. The engine binds
// the active MemTable when an iterator is created and then keeps serving Put and
// Delete, which relink the very nodes the iterator is walking. That is a genuine
// data race — `go test -race` reported it as MemTable.insertNode racing
// MemTable.seekNode — and it also lets a scan observe writes that landed after
// the scan began.
//
// Copying the entries once, while the engine lock is held, gives the iterator a
// stable view for its whole lifetime. That is the observation contract
// storage.Iterator now documents.
//
// Cost: one O(n) copy per iterator, bounded by the MemTable flush threshold
// (4 MiB by default). Sharing the key/value slices is safe because MemTable.Put
// *reassigns* node.value rather than writing through it, so a captured slice is
// never mutated underneath us.
type MemTableSnapshot struct {
	entries []memEntry
}

// Snapshot copies the MemTable's current contents in sorted order.
//
// The caller must hold whatever lock protects the MemTable against concurrent
// mutation; the returned snapshot then needs no locking at all.
func (m *MemTable) Snapshot() *MemTableSnapshot {
	entries := make([]memEntry, 0, m.length)
	for x := m.head.levels[0].forward; x != nil; x = x.levels[0].forward {
		entries = append(entries, memEntry{
			key:       x.key,
			value:     x.value,
			tombstone: x.tombstone,
		})
	}
	return &MemTableSnapshot{entries: entries}
}

// MemTableSnapshotIterator iterates an immutable MemTableSnapshot.
// Level-0 order in the skiplist is already sorted, so the copied slice is too.
type MemTableSnapshotIterator struct {
	snap *MemTableSnapshot
	idx  int
}

func (s *MemTableSnapshot) NewIterator() *MemTableSnapshotIterator {
	return &MemTableSnapshotIterator{snap: s, idx: -1}
}

func (it *MemTableSnapshotIterator) Seek(key []byte) bool {
	it.idx = sort.Search(len(it.snap.entries), func(i int) bool {
		return bytes.Compare(it.snap.entries[i].key, key) >= 0
	})
	return it.Valid()
}

func (it *MemTableSnapshotIterator) Next() bool {
	if it.idx < len(it.snap.entries) {
		it.idx++
	}
	return it.Valid()
}

func (it *MemTableSnapshotIterator) Prev() bool {
	if it.idx >= 0 {
		it.idx--
	}
	return it.Valid()
}

func (it *MemTableSnapshotIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.snap.entries)
}

func (it *MemTableSnapshotIterator) Key() []byte { return it.snap.entries[it.idx].key }

func (it *MemTableSnapshotIterator) Value() []byte { return it.snap.entries[it.idx].value }

func (it *MemTableSnapshotIterator) IsTombstone() bool { return it.snap.entries[it.idx].tombstone }

func (it *MemTableSnapshotIterator) Close() {}
