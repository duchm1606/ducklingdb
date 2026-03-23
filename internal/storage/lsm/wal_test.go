package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestWALAppendAndReadAll(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wal")
	entries := makeTestEntries(100)

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	for _, entry := range entries {
		entry := entry
		if err := wal.Append(&entry); err != nil {
			_ = wal.Close()
			t.Fatalf("Append() error = %v", err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	wal, err = OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() reopen error = %v", err)
	}
	defer wal.Close()

	decoded, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	assertEntriesEqual(t, entries, decoded)
}

func TestWALCrashRecoveryTruncatedEntry(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wal")
	entries := makeTestEntries(10)

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	for _, entry := range entries {
		entry := entry
		if err := wal.Append(&entry); err != nil {
			_ = wal.Close()
			t.Fatalf("Append() error = %v", err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat() error = %v", err)
	}

	if err := os.Truncate(path, info.Size()-5); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}

	wal, err = OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() reopen error = %v", err)
	}
	defer wal.Close()

	decoded, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	assertEntriesEqual(t, entries[:9], decoded)
}

func TestWALCrashRecoveryCorruptedEntry(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wal")
	entries := makeTestEntries(10)

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}

	for _, entry := range entries {
		entry := entry
		if err := wal.Append(&entry); err != nil {
			_ = wal.Close()
			t.Fatalf("Append() error = %v", err)
		}
	}

	if err := wal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	rawFile, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("OpenFile() error = %v", err)
	}

	corruptOffset := entryOffset(entries, 4) + int64(entryHeaderSize) + 2
	byteAtOffset := make([]byte, 1)
	if _, err := rawFile.ReadAt(byteAtOffset, corruptOffset); err != nil {
		_ = rawFile.Close()
		t.Fatalf("ReadAt() error = %v", err)
	}

	byteAtOffset[0] ^= 0xFF
	if _, err := rawFile.WriteAt(byteAtOffset, corruptOffset); err != nil {
		_ = rawFile.Close()
		t.Fatalf("WriteAt() error = %v", err)
	}

	if err := rawFile.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	wal, err = OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() reopen error = %v", err)
	}
	defer wal.Close()

	decoded, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	assertEntriesEqual(t, entries[:4], decoded)
}

func TestWALEmpty(t *testing.T) {
	t.Parallel()

	wal, err := OpenWAL(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer wal.Close()

	decoded, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(decoded) != 0 {
		t.Fatalf("len(ReadAll()) = %d, want 0", len(decoded))
	}
}

func TestWALTruncate(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wal")
	entries := makeTestEntries(10)

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer wal.Close()

	for _, entry := range entries {
		entry := entry
		if err := wal.Append(&entry); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	if err := wal.Truncate(); err != nil {
		t.Fatalf("Truncate() error = %v", err)
	}

	decoded, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if len(decoded) != 0 {
		t.Fatalf("len(ReadAll()) = %d, want 0", len(decoded))
	}
}

func TestWALAppendAfterReadAll(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "wal")
	firstEntries := makeTestEntries(5)
	secondEntries := makeTestEntries(3)

	wal, err := OpenWAL(path)
	if err != nil {
		t.Fatalf("OpenWAL() error = %v", err)
	}
	defer wal.Close()

	for _, entry := range firstEntries {
		entry := entry
		if err := wal.Append(&entry); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	decoded, err := wal.ReadAll()
	if err != nil {
		t.Fatalf("first ReadAll() error = %v", err)
	}
	assertEntriesEqual(t, firstEntries, decoded)

	for _, entry := range secondEntries {
		entry := entry
		if err := wal.Append(&entry); err != nil {
			t.Fatalf("Append() error = %v", err)
		}
	}

	decoded, err = wal.ReadAll()
	if err != nil {
		t.Fatalf("second ReadAll() error = %v", err)
	}

	assertEntriesEqual(t, append(firstEntries, secondEntries...), decoded)
}

func makeTestEntries(count int) []Entry {
	entries := make([]Entry, count)
	for index := range count {
		entries[index] = Entry{
			Key:   []byte(fmt.Sprintf("key-%03d", index)),
			Value: []byte(fmt.Sprintf("value-%03d", index)),
			Op:    OpPut,
		}
	}

	return entries
}

func entryOffset(entries []Entry, index int) int64 {
	var offset int64
	for _, entry := range entries[:index] {
		offset += int64(len(entry.Encode()))
	}

	return offset
}

func assertEntriesEqual(t *testing.T, want, got []Entry) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("len(entries) = %d, want %d", len(got), len(want))
	}

	for index := range want {
		if want[index].Op != got[index].Op {
			t.Fatalf("entries[%d].Op = %d, want %d", index, got[index].Op, want[index].Op)
		}

		if !bytes.Equal(want[index].Key, got[index].Key) {
			t.Fatalf("entries[%d].Key = %q, want %q", index, got[index].Key, want[index].Key)
		}

		if !bytes.Equal(want[index].Value, got[index].Value) {
			t.Fatalf("entries[%d].Value = %q, want %q", index, got[index].Value, want[index].Value)
		}
	}
}
