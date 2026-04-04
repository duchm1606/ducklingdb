package lsm

import (
	"bytes"
	"math/rand"
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
	if nodeLevel > m.level {
		for level := m.level; level < nodeLevel; level++ {
			update[level] = m.head
		}
		m.level = nodeLevel
	}
	for level := 0; level < nodeLevel; level++ {
		node.levels[level].forward = update[level].levels[level].forward
		update[level].levels[level].forward = node
	}
	if update[0] == m.head {
		node.backward = nil
	} else {
		node.backward = update[0]
	}
	if node.levels[0].forward != nil {
		node.levels[0].forward.backward = node
	} else {
		m.tail = node
	}
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
