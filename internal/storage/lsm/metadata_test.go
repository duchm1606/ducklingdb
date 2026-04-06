package lsm

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestMetaStore_SaveLoad_RoundTrip(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ms := NewMetaStore(dir)

	meta := MetaData{
		Levels: [][]string{
			{"sst-000000.sst", "sst-000001.sst"},
			{"sst-000002.sst"},
		},
	}

	if err := ms.Save(meta); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := ms.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if len(loaded.Levels) != 2 {
		t.Fatalf("got %d levels, want 2", len(loaded.Levels))
	}
	if loaded.Levels[0][0] != "sst-000000.sst" {
		t.Fatalf("got %q, want sst-000000.sst", loaded.Levels[0][0])
	}
	if loaded.Version == 0 {
		t.Fatal("loaded version should be > 0")
	}
}

func TestMetaStore_Load_FreshDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ms := NewMetaStore(dir)

	meta, err := ms.Load()
	if err != nil {
		t.Fatalf("Load on fresh dir: %v", err)
	}
	if meta.Version != 0 || len(meta.Levels) != 0 {
		t.Fatalf("expected empty MetaData, got %+v", meta)
	}
}

func TestMetaStore_VersionMonotonicallyIncreases(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ms := NewMetaStore(dir)

	var lastVersion uint64
	for i := 0; i < 5; i++ {
		if err := ms.Save(MetaData{}); err != nil {
			t.Fatalf("Save %d: %v", i, err)
		}
		loaded, err := ms.Load()
		if err != nil {
			t.Fatalf("Load %d: %v", i, err)
		}
		if loaded.Version <= lastVersion {
			t.Fatalf("version did not increase: got %d, previous was %d", loaded.Version, lastVersion)
		}
		lastVersion = loaded.Version
	}
}

func TestMetaStore_CorruptSlot0_LoadsSlot1(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ms := NewMetaStore(dir)

	// Write twice so both slots are populated.
	if err := ms.Save(MetaData{Levels: [][]string{{"sst-000000.sst"}}}); err != nil {
		t.Fatal(err)
	}
	if err := ms.Save(MetaData{Levels: [][]string{{"sst-000001.sst"}}}); err != nil {
		t.Fatal(err)
	}

	// Corrupt meta0 by overwriting its checksum bytes.
	slot0Path := filepath.Join(dir, metaSlot0)
	data, err := os.ReadFile(slot0Path)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(data[len(data)-4:], 0xdeadbeef) // wrong checksum
	if err := os.WriteFile(slot0Path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := ms.Load()
	if err != nil {
		t.Fatalf("Load with corrupted slot0: %v", err)
	}
	// slot1 holds the second save, which has one level with sst-000001.sst
	if len(loaded.Levels) == 0 || loaded.Levels[0][0] != "sst-000001.sst" {
		t.Fatalf("expected sst-000001.sst from slot1, got %+v", loaded)
	}
}

func TestMetaStore_CorruptSlot1_LoadsSlot0(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ms := NewMetaStore(dir)

	if err := ms.Save(MetaData{Levels: [][]string{{"sst-000000.sst"}}}); err != nil {
		t.Fatal(err)
	}
	if err := ms.Save(MetaData{Levels: [][]string{{"sst-000001.sst"}}}); err != nil {
		t.Fatal(err)
	}

	slot1Path := filepath.Join(dir, metaSlot1)
	data, err := os.ReadFile(slot1Path)
	if err != nil {
		t.Fatal(err)
	}
	binary.LittleEndian.PutUint32(data[len(data)-4:], 0xdeadbeef)
	if err := os.WriteFile(slot1Path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	loaded, err := ms.Load()
	if err != nil {
		t.Fatalf("Load with corrupted slot1: %v", err)
	}
	if len(loaded.Levels) == 0 || loaded.Levels[0][0] != "sst-000000.sst" {
		t.Fatalf("expected sst-000000.sst from slot0, got %+v", loaded)
	}
}

func TestMetaStore_AlwaysOneValidSlot(t *testing.T) {
	t.Parallel()

	// Simulate rapid successive saves and verify that Load always succeeds,
	// even if we pretend a crash happened after the most recent write.
	dir := t.TempDir()
	ms := NewMetaStore(dir)

	for i := 0; i < 10; i++ {
		if err := ms.Save(MetaData{Levels: [][]string{{sstableFilename(uint64(i))}}}); err != nil {
			t.Fatalf("Save %d: %v", i, err)
		}
		// After every write, at least one slot must be loadable.
		if _, err := ms.Load(); err != nil {
			t.Fatalf("Load after save %d: %v", i, err)
		}
	}
}

func TestMetaStore_EmptyLevels(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ms := NewMetaStore(dir)

	if err := ms.Save(MetaData{}); err != nil {
		t.Fatal(err)
	}
	loaded, err := ms.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Version == 0 {
		t.Fatal("version should be > 0 after save")
	}
	if len(loaded.Levels) != 0 {
		t.Fatalf("expected empty levels, got %v", loaded.Levels)
	}
}
