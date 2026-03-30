package lsm

import (
	"bytes"
	"testing"
)

func TestMemTablePutAndGet(t *testing.T) {
	t.Parallel()

	m := NewMemTable()
	m.Put([]byte("alice"), []byte("wonderland"))
	m.Put([]byte("bob"), []byte("builder"))

	value, found, tombstone := m.Get([]byte("alice"))
	if !found {
		t.Fatal("Get(alice) found = false, want true")
	}
	if tombstone {
		t.Fatal("Get(alice) tombstone = true, want false")
	}
	if !bytes.Equal(value, []byte("wonderland")) {
		t.Fatalf("Get(alice) value = %q, want %q", value, []byte("wonderland"))
	}

	value, found, tombstone = m.Get([]byte("bob"))
	if !found {
		t.Fatal("Get(bob) found = false, want true")
	}
	if tombstone {
		t.Fatal("Get(bob) tombstone = true, want false")
	}
	if !bytes.Equal(value, []byte("builder")) {
		t.Fatalf("Get(bob) value = %q, want %q", value, []byte("builder"))
	}
}

func TestMemTableOverwrite(t *testing.T) {
	t.Parallel()

	m := NewMemTable()
	m.Put([]byte("key"), []byte("v1"))
	m.Put([]byte("key"), []byte("v2"))

	value, found, tombstone := m.Get([]byte("key"))
	if !found {
		t.Fatal("Get(key) found = false, want true")
	}
	if tombstone {
		t.Fatal("Get(key) tombstone = true, want false")
	}
	if !bytes.Equal(value, []byte("v2")) {
		t.Fatalf("Get(key) value = %q, want %q", value, []byte("v2"))
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
}

func TestMemTableDelete(t *testing.T) {
	t.Parallel()

	m := NewMemTable()
	m.Put([]byte("key"), []byte("value"))
	m.Delete([]byte("key"))

	value, found, tombstone := m.Get([]byte("key"))
	if value != nil {
		t.Fatalf("Get(key) value = %q, want nil", value)
	}
	if !found {
		t.Fatal("Get(key) found = false, want true")
	}
	if !tombstone {
		t.Fatal("Get(key) tombstone = false, want true")
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
}

func TestMemTableDeleteNonExistent(t *testing.T) {
	t.Parallel()

	m := NewMemTable()
	m.Delete([]byte("ghost"))

	value, found, tombstone := m.Get([]byte("ghost"))
	if value != nil {
		t.Fatalf("Get(ghost) value = %q, want nil", value)
	}
	if !found {
		t.Fatal("Get(ghost) found = false, want true")
	}
	if !tombstone {
		t.Fatal("Get(ghost) tombstone = false, want true")
	}
	if got := m.Len(); got != 1 {
		t.Fatalf("Len() = %d, want 1", got)
	}
}

func TestMemTableIteratorSorted(t *testing.T) {
	t.Parallel()

	m := NewMemTable()
	m.Put([]byte("delta"), []byte("4"))
	m.Put([]byte("alpha"), []byte("1"))
	m.Put([]byte("charlie"), []byte("3"))
	m.Put([]byte("bravo"), []byte("2"))

	it := m.NewIterator()
	defer it.Close()

	if !it.Seek([]byte("")) {
		t.Fatal("Seek(\"\") = false, want true")
	}

	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}

	want := []string{"alpha", "bravo", "charlie", "delta"}
	if len(keys) != len(want) {
		t.Fatalf("len(keys) = %d, want %d", len(keys), len(want))
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys[%d] = %q, want %q", i, keys[i], want[i])
		}
	}
}

func TestMemTableIteratorSeek(t *testing.T) {
	t.Parallel()

	m := NewMemTable()
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("c"), []byte("3"))
	m.Put([]byte("e"), []byte("5"))
	m.Put([]byte("g"), []byte("7"))

	it := m.NewIterator()
	defer it.Close()

	if !it.Seek([]byte("d")) {
		t.Fatal("Seek(d) = false, want true")
	}
	if got := string(it.Key()); got != "e" {
		t.Fatalf("Seek(d) key = %q, want %q", got, "e")
	}

	if !it.Seek([]byte("c")) {
		t.Fatal("Seek(c) = false, want true")
	}
	if got := string(it.Key()); got != "c" {
		t.Fatalf("Seek(c) key = %q, want %q", got, "c")
	}

	if it.Seek([]byte("z")) {
		t.Fatal("Seek(z) = true, want false")
	}
	if it.Valid() {
		t.Fatal("Valid() = true after Seek(z), want false")
	}
}

func TestMemTableIteratorPrev(t *testing.T) {
	t.Parallel()

	m := NewMemTable()
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Put([]byte("c"), []byte("3"))

	it := m.NewIterator()
	defer it.Close()

	if !it.Seek([]byte("c")) {
		t.Fatal("Seek(c) = false, want true")
	}
	if !it.Prev() {
		t.Fatal("Prev() = false, want true")
	}
	if got := string(it.Key()); got != "b" {
		t.Fatalf("Prev() key = %q, want %q", got, "b")
	}
}

func TestMemTableIteratorIncludesTombstones(t *testing.T) {
	t.Parallel()

	m := NewMemTable()
	m.Put([]byte("a"), []byte("1"))
	m.Put([]byte("b"), []byte("2"))
	m.Put([]byte("c"), []byte("3"))
	m.Delete([]byte("b"))

	it := m.NewIterator()
	defer it.Close()

	if !it.Seek([]byte("")) {
		t.Fatal("Seek(\"\") = false, want true")
	}

	if got := string(it.Key()); got != "a" {
		t.Fatalf("first key = %q, want %q", got, "a")
	}
	if !it.Next() {
		t.Fatal("Next() after a = false, want true")
	}
	if got := string(it.Key()); got != "b" {
		t.Fatalf("second key = %q, want %q", got, "b")
	}
	if !it.IsTombstone() {
		t.Fatal("IsTombstone() on b = false, want true")
	}
	if !it.Next() {
		t.Fatal("Next() after tombstone = false, want true")
	}
	if got := string(it.Key()); got != "c" {
		t.Fatalf("third key = %q, want %q", got, "c")
	}
}

func TestMemTableSize(t *testing.T) {
	t.Parallel()

	m := NewMemTable()
	m.Put([]byte("a"), []byte("1"))
	if got, want := m.Size(), 2; got != want {
		t.Fatalf("Size() after first Put = %d, want %d", got, want)
	}

	m.Put([]byte("bb"), []byte("22"))
	if got, want := m.Size(), 6; got != want {
		t.Fatalf("Size() after second Put = %d, want %d", got, want)
	}

	m.Put([]byte("a"), []byte("333"))
	if got, want := m.Size(), 8; got != want {
		t.Fatalf("Size() after overwrite = %d, want %d", got, want)
	}

	m.Delete([]byte("a"))
	if got, want := m.Size(), 5; got != want {
		t.Fatalf("Size() after Delete(a) = %d, want %d", got, want)
	}

	m.Delete([]byte("ghost"))
	if got, want := m.Size(), 10; got != want {
		t.Fatalf("Size() after Delete(ghost) = %d, want %d", got, want)
	}
}
