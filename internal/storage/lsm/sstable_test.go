package lsm

import (
	"bytes"
	"fmt"
	"path/filepath"
	"testing"
)

func TestSSTableWriteAndRead(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "table.sst")
	entries := make([]sstableTestEntry, 1000)
	for i := range entries {
		entries[i] = sstableTestEntry{
			key:   []byte(fmt.Sprintf("key-%04d", i)),
			value: []byte(fmt.Sprintf("value-%04d", i)),
		}
	}

	writeTestSSTable(t, path, entries)

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable() error = %v", err)
	}
	defer reader.Close()

	for _, entry := range entries {
		value, found, err := reader.Get(entry.key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", entry.key, err)
		}
		if !found {
			t.Fatalf("Get(%q) found = false, want true", entry.key)
		}
		if !bytes.Equal(value, entry.value) {
			t.Fatalf("Get(%q) value = %q, want %q", entry.key, value, entry.value)
		}
	}
}

func TestSSTableGet_NotFound(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "table.sst")
	writeTestSSTable(t, path, []sstableTestEntry{
		{key: []byte("a"), value: []byte("1")},
		{key: []byte("c"), value: []byte("3")},
		{key: []byte("e"), value: []byte("5")},
	})

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable() error = %v", err)
	}
	defer reader.Close()

	for _, key := range [][]byte{[]byte("b"), []byte("d"), []byte("z")} {
		value, found, err := reader.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if found {
			t.Fatalf("Get(%q) found = true, want false", key)
		}
		if value != nil {
			t.Fatalf("Get(%q) value = %q, want nil", key, value)
		}
	}
}

func TestSSTableGet_Tombstone(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "table.sst")
	writeTestSSTable(t, path, []sstableTestEntry{
		{key: []byte("a"), value: []byte("1")},
		{key: []byte("b"), tombstone: true},
		{key: []byte("c"), value: []byte("3")},
	})

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable() error = %v", err)
	}
	defer reader.Close()

	value, found, err := reader.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Get(b) error = %v", err)
	}
	if found {
		t.Fatal("Get(b) found = true, want false")
	}
	if value != nil {
		t.Fatalf("Get(b) value = %q, want nil", value)
	}
}

func TestSSTableEmpty(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "table.sst")
	writeTestSSTable(t, path, nil)

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable() error = %v", err)
	}
	defer reader.Close()

	value, found, err := reader.Get([]byte("anything"))
	if err != nil {
		t.Fatalf("Get(anything) error = %v", err)
	}
	if found {
		t.Fatal("Get(anything) found = true, want false")
	}
	if value != nil {
		t.Fatalf("Get(anything) value = %q, want nil", value)
	}

	it := reader.NewIterator()
	defer it.Close()
	if it.Valid() {
		t.Fatal("iterator Valid() = true on empty SSTable, want false")
	}
}

func TestSSTableIterator_FullScan(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "table.sst")
	entries := make([]sstableTestEntry, 100)
	for i := range entries {
		entries[i] = sstableTestEntry{
			key:   []byte(fmt.Sprintf("key-%03d", i)),
			value: []byte(fmt.Sprintf("value-%03d", i)),
		}
	}
	writeTestSSTable(t, path, entries)

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable() error = %v", err)
	}
	defer reader.Close()

	it := reader.NewIterator()
	defer it.Close()

	if !it.Next() {
		t.Fatal("Next() from initial position = false, want true")
	}

	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}

	if len(keys) != len(entries) {
		t.Fatalf("len(keys) = %d, want %d", len(keys), len(entries))
	}
	for i, entry := range entries {
		if keys[i] != string(entry.key) {
			t.Fatalf("keys[%d] = %q, want %q", i, keys[i], entry.key)
		}
	}
}

func TestSSTableIterator_Seek(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "table.sst")
	writeTestSSTable(t, path, []sstableTestEntry{
		{key: []byte("a"), value: []byte("1")},
		{key: []byte("c"), value: []byte("3")},
		{key: []byte("e"), value: []byte("5")},
		{key: []byte("g"), value: []byte("7")},
	})

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable() error = %v", err)
	}
	defer reader.Close()

	it := reader.NewIterator()
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

func TestSSTableIterator_Prev(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "table.sst")
	writeTestSSTable(t, path, []sstableTestEntry{
		{key: []byte("a"), value: []byte("1")},
		{key: []byte("b"), value: []byte("2")},
		{key: []byte("c"), value: []byte("3")},
	})

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable() error = %v", err)
	}
	defer reader.Close()

	it := reader.NewIterator()
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

func TestSSTableIterator_IncludesTombstones(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "table.sst")
	writeTestSSTable(t, path, []sstableTestEntry{
		{key: []byte("a"), value: []byte("1")},
		{key: []byte("b"), tombstone: true},
		{key: []byte("c"), value: []byte("3")},
	})

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable() error = %v", err)
	}
	defer reader.Close()

	it := reader.NewIterator()
	defer it.Close()

	if !it.Next() {
		t.Fatal("Next() from initial position = false, want true")
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
		t.Fatal("Next() after b = false, want true")
	}
	if got := string(it.Key()); got != "c" {
		t.Fatalf("third key = %q, want %q", got, "c")
	}
}

func TestWriteSSTableFromIterator_MemTableRoundTrip(t *testing.T) {
	t.Parallel()

	mem := NewMemTable()
	mem.Put([]byte("a"), []byte("1"))
	mem.Put([]byte("b"), []byte("2"))
	mem.Put([]byte("c"), []byte("3"))
	mem.Delete([]byte("b"))
	mem.Freeze()

	path := filepath.Join(t.TempDir(), "flushed.sst")
	if err := WriteSSTableFromIterator(path, mem.NewIterator()); err != nil {
		t.Fatalf("WriteSSTableFromIterator() error = %v", err)
	}

	reader, err := OpenSSTable(path)
	if err != nil {
		t.Fatalf("OpenSSTable() error = %v", err)
	}
	defer reader.Close()

	value, found, err := reader.Get([]byte("a"))
	if err != nil {
		t.Fatalf("Get(a) error = %v", err)
	}
	if !found {
		t.Fatal("Get(a) found = false, want true")
	}
	if !bytes.Equal(value, []byte("1")) {
		t.Fatalf("Get(a) value = %q, want %q", value, []byte("1"))
	}

	value, found, err = reader.Get([]byte("b"))
	if err != nil {
		t.Fatalf("Get(b) error = %v", err)
	}
	if found {
		t.Fatal("Get(b) found = true, want false")
	}
	if value != nil {
		t.Fatalf("Get(b) value = %q, want nil", value)
	}

	value, found, err = reader.Get([]byte("c"))
	if err != nil {
		t.Fatalf("Get(c) error = %v", err)
	}
	if !found {
		t.Fatal("Get(c) found = false, want true")
	}
	if !bytes.Equal(value, []byte("3")) {
		t.Fatalf("Get(c) value = %q, want %q", value, []byte("3"))
	}

	it := reader.NewIterator()
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
		t.Fatal("Next() after b = false, want true")
	}
	if got := string(it.Key()); got != "c" {
		t.Fatalf("third key = %q, want %q", got, "c")
	}
}

type sstableTestEntry struct {
	key       []byte
	value     []byte
	tombstone bool
}

func writeTestSSTable(t *testing.T, path string, entries []sstableTestEntry) {
	t.Helper()

	writer, err := NewSSTableWriter(path)
	if err != nil {
		t.Fatalf("NewSSTableWriter() error = %v", err)
	}

	for _, entry := range entries {
		if err := writer.Add(entry.key, entry.value, entry.tombstone); err != nil {
			_ = writer.Close()
			t.Fatalf("Add(%q) error = %v", entry.key, err)
		}
	}

	if err := writer.Finish(); err != nil {
		t.Fatalf("Finish() error = %v", err)
	}
}
