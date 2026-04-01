package lsm

import (
	"bytes"
	"errors"
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

func TestBasicKVPutAndGet(t *testing.T) {
	t.Parallel()

	kv, err := OpenBasicKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenBasicKV() error = %v", err)
	}
	defer kv.Close()

	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		value := []byte{byte('0' + i)}
		if err := kv.Put(key, value); err != nil {
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		want := []byte{byte('0' + i)}
		got, err := kv.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) = %q, want %q", key, got, want)
		}
	}
}

func TestBasicKVDelete(t *testing.T) {
	t.Parallel()

	kv, err := OpenBasicKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenBasicKV() error = %v", err)
	}
	defer kv.Close()

	if err := kv.Put([]byte("k1"), []byte("v1")); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if err := kv.Delete([]byte("k1")); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	_, err = kv.Get([]byte("k1"))
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get() error = %v, want %v", err, storage.ErrKeyNotFound)
	}
}

func TestBasicKVPersistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	kv, err := OpenBasicKV(dir)
	if err != nil {
		t.Fatalf("OpenBasicKV() error = %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		value := []byte{byte(255 - i)}
		if err := kv.Put(key, value); err != nil {
			_ = kv.Close()
			t.Fatalf("Put(%q) error = %v", key, err)
		}
	}

	if err := kv.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	kv, err = OpenBasicKV(dir)
	if err != nil {
		t.Fatalf("OpenBasicKV() reopen error = %v", err)
	}
	defer kv.Close()

	for i := 0; i < 100; i++ {
		key := []byte{byte(i)}
		want := []byte{byte(255 - i)}
		got, err := kv.Get(key)
		if err != nil {
			t.Fatalf("Get(%q) after reopen error = %v", key, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("Get(%q) after reopen = %q, want %q", key, got, want)
		}
	}
}

func TestBasicKVDeletePersistence(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	kv, err := OpenBasicKV(dir)
	if err != nil {
		t.Fatalf("OpenBasicKV() error = %v", err)
	}

	if err := kv.Put([]byte("k1"), []byte("v1")); err != nil {
		_ = kv.Close()
		t.Fatalf("Put() error = %v", err)
	}
	if err := kv.Delete([]byte("k1")); err != nil {
		_ = kv.Close()
		t.Fatalf("Delete() error = %v", err)
	}

	if err := kv.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	kv, err = OpenBasicKV(dir)
	if err != nil {
		t.Fatalf("OpenBasicKV() reopen error = %v", err)
	}
	defer kv.Close()

	_, err = kv.Get([]byte("k1"))
	if !errors.Is(err, storage.ErrKeyNotFound) {
		t.Fatalf("Get() after reopen error = %v, want %v", err, storage.ErrKeyNotFound)
	}
}

func TestBasicKVIteratorSkipsTombstones(t *testing.T) {
	t.Parallel()

	kv, err := OpenBasicKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenBasicKV() error = %v", err)
	}
	defer kv.Close()

	if err := kv.Put([]byte("c"), []byte("3")); err != nil {
		t.Fatalf("Put(c) error = %v", err)
	}
	if err := kv.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatalf("Put(a) error = %v", err)
	}
	if err := kv.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatalf("Put(b) error = %v", err)
	}
	if err := kv.Delete([]byte("b")); err != nil {
		t.Fatalf("Delete(b) error = %v", err)
	}

	it, err := kv.NewIterator()
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

	want := []string{"a", "c"}
	if len(keys) != len(want) {
		t.Fatalf("len(keys) = %d, want %d", len(keys), len(want))
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys[%d] = %q, want %q", i, keys[i], want[i])
		}
	}
}

func TestBasicKVBatchCommit(t *testing.T) {
	t.Parallel()

	kv, err := OpenBasicKV(t.TempDir())
	if err != nil {
		t.Fatalf("OpenBasicKV() error = %v", err)
	}
	defer kv.Close()

	batch := kv.NewBatch()
	batch.Put([]byte("a"), []byte("1"))
	batch.Put([]byte("b"), []byte("2"))
	batch.Put([]byte("c"), []byte("3"))
	batch.Put([]byte("d"), []byte("4"))
	batch.Put([]byte("e"), []byte("5"))

	if err := batch.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}

	for _, tc := range []struct {
		key  []byte
		want []byte
	}{
		{key: []byte("a"), want: []byte("1")},
		{key: []byte("b"), want: []byte("2")},
		{key: []byte("c"), want: []byte("3")},
		{key: []byte("d"), want: []byte("4")},
		{key: []byte("e"), want: []byte("5")},
	} {
		got, err := kv.Get(tc.key)
		if err != nil {
			t.Fatalf("Get(%q) error = %v", tc.key, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("Get(%q) = %q, want %q", tc.key, got, tc.want)
		}
	}
}
