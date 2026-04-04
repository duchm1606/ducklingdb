package lsm

import (
	"bytes"
	"errors"
	"path/filepath"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

func TestLSMEngine_ImplementsEngine(t *testing.T) {
	t.Parallel()

	var _ storage.Engine = (*LSMEngine)(nil)
}

func TestLSMEngine_BasicPutGet(t *testing.T) {
	t.Parallel()

	engine, err := OpenLSM(LSMOptions{Dir: t.TempDir(), MemTableThreshold: 1 << 20})
	if err != nil {
		t.Fatalf("OpenLSM() error = %v", err)
	}
	defer engine.Close()

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(255 - i)}
		if err := engine.Put(key, value); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		want := []byte{byte(255 - i)}
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestLSMEngine_MemTableFlush(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	engine, err := OpenLSM(LSMOptions{Dir: dir, MemTableThreshold: 32})
	if err != nil {
		t.Fatalf("OpenLSM() error = %v", err)
	}
	defer engine.Close()

	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		value := []byte("value")
		if err := engine.Put(key, value); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	files, err := filepath.Glob(filepath.Join(dir, sstablePattern))
	if err != nil {
		t.Fatalf("Glob() error = %v", err)
	}
	if len(files) == 0 {
		t.Fatal("expected at least one SSTable file after flush")
	}

	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if !bytes.Equal(got, []byte("value")) {
			t.Fatalf("Get(%q) = %q, want %q", key, got, []byte("value"))
		}
	}
}

func TestLSMEngine_Persistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	engine, err := OpenLSM(LSMOptions{Dir: dir, MemTableThreshold: 32})
	if err != nil {
		t.Fatalf("OpenLSM() error = %v", err)
	}

	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(i + 1)}
		if err := engine.Put(key, value); err != nil {
			_ = engine.Close()
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	engine, err = OpenLSM(LSMOptions{Dir: dir, MemTableThreshold: 32})
	if err != nil {
		t.Fatalf("OpenLSM() reopen error = %v", err)
	}
	defer engine.Close()

	for i := 0; i < 50; i++ {
		key := []byte{byte(i)}
		want := []byte{byte(i + 1)}
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) after reopen error = %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) after reopen = %q, want %q", key, got, want)
		}
	}
}

func TestLSMEngine_DeleteAndReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	engine, err := OpenLSM(LSMOptions{Dir: dir, MemTableThreshold: 32})
	if err != nil {
		t.Fatalf("OpenLSM() error = %v", err)
	}

	if err := engine.Put([]byte("a"), []byte("1")); err != nil {
		_ = engine.Close()
		t.Fatalf("Put(a) error = %v", err)
	}
	if err := engine.Delete([]byte("a")); err != nil {
		_ = engine.Close()
		t.Fatalf("Delete(a) error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	engine, err = OpenLSM(LSMOptions{Dir: dir, MemTableThreshold: 32})
	if err != nil {
		t.Fatalf("OpenLSM() reopen error = %v", err)
	}
	defer engine.Close()

	_, err = engine.Get([]byte("a"))
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get(a) error = %v, want %v", err, storage.ErrKeyNotFound)
	}
}

func TestLSMEngine_IteratorMergedView(t *testing.T) {
	t.Parallel()

	engine, err := OpenLSM(LSMOptions{Dir: t.TempDir(), MemTableThreshold: 8})
	if err != nil {
		t.Fatalf("OpenLSM() error = %v", err)
	}
	defer engine.Close()

	for _, key := range [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d")} {
		if err := engine.Put(key, []byte("v")); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	// With the small threshold above, the first four writes are flushed into an SSTable.
	// The following operations create newer in-memory state that must override disk state.
	if err := engine.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete(b) error = %v", err)
	}
	if err := engine.Put([]byte("e"), []byte("v")); err != nil {
		t.Fatalf("Put(e) error = %v", err)
	}

	it, err := engine.NewIterator()
	if err != nil {
		t.Fatalf("NewIterator() error = %v", err)
	}
	defer it.Close()

	if !it.Seek([]byte("")) {
		t.Fatal("Seek(\"\") = false, want true")
	}

	var keys []string
	for it.Valid() {
		keys = append(keys, string(it.Key()))
		it.Next()
	}

	want := []string{"a", "c", "d", "e"}
	if len(keys) != len(want) {
		t.Fatalf("len(keys) = %d, want %d", len(keys), len(want))
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys[%d] = %q, want %q", i, keys[i], want[i])
		}
	}
}
