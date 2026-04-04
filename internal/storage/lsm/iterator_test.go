package lsm

import (
	"testing"

	"github.com/duchm1606/ducklingdb/internal/storage"
)

func TestMergeIterator_SingleSource(t *testing.T) {
	t.Parallel()

	merge := NewMergeIterator([]storage.Iterator{
		newMergeTestSource([]mergeTestEntry{{key: "a", value: "1"}, {key: "c", value: "3"}, {key: "e", value: "5"}}),
	})
	defer merge.Close()

	if !merge.Seek([]byte("")) {
		t.Fatal("Seek(\"\") = false, want true")
	}

	assertMergedKeysValues(t, merge,
		[]string{"a", "c", "e"},
		[]string{"1", "3", "5"},
		[]bool{false, false, false},
	)
}

func TestMergeIterator_TwoSourcesNoOverlap(t *testing.T) {
	t.Parallel()

	merge := NewMergeIterator([]storage.Iterator{
		newMergeTestSource([]mergeTestEntry{{key: "a", value: "1"}, {key: "c", value: "3"}, {key: "e", value: "5"}}),
		newMergeTestSource([]mergeTestEntry{{key: "b", value: "2"}, {key: "d", value: "4"}, {key: "f", value: "6"}}),
	})
	defer merge.Close()

	if !merge.Seek([]byte("")) {
		t.Fatal("Seek(\"\") = false, want true")
	}

	assertMergedKeysValues(t, merge,
		[]string{"a", "b", "c", "d", "e", "f"},
		[]string{"1", "2", "3", "4", "5", "6"},
		[]bool{false, false, false, false, false, false},
	)
}

func TestMergeIterator_DuplicateKeyNewerWins(t *testing.T) {
	t.Parallel()

	merge := NewMergeIterator([]storage.Iterator{
		newMergeTestSource([]mergeTestEntry{{key: "a", value: "new"}, {key: "c", value: "3"}}),
		newMergeTestSource([]mergeTestEntry{{key: "a", value: "old"}, {key: "b", value: "2"}}),
	})
	defer merge.Close()

	if !merge.Seek([]byte("")) {
		t.Fatal("Seek(\"\") = false, want true")
	}

	assertMergedKeysValues(t, merge,
		[]string{"a", "b", "c"},
		[]string{"new", "2", "3"},
		[]bool{false, false, false},
	)
}

func TestMergeIterator_TombstoneHidesOlderValue(t *testing.T) {
	t.Parallel()

	merge := NewMergeIterator([]storage.Iterator{
		newMergeTestSource([]mergeTestEntry{{key: "a", tombstone: true}}),
		newMergeTestSource([]mergeTestEntry{{key: "a", value: "old"}, {key: "b", value: "2"}}),
	})
	defer merge.Close()

	if !merge.Seek([]byte("")) {
		t.Fatal("Seek(\"\") = false, want true")
	}

	if got := string(merge.Key()); got != "a" {
		t.Fatalf("first key = %q, want %q", got, "a")
	}
	if !merge.IsTombstone() {
		t.Fatal("first entry tombstone = false, want true")
	}
	if !merge.Next() {
		t.Fatal("Next() after tombstone = false, want true")
	}
	if got := string(merge.Key()); got != "b" {
		t.Fatalf("second key = %q, want %q", got, "b")
	}
}

func TestMergeIterator_ThreeSources(t *testing.T) {
	t.Parallel()

	merge := NewMergeIterator([]storage.Iterator{
		newMergeTestSource([]mergeTestEntry{{key: "b", value: "2"}}),
		newMergeTestSource([]mergeTestEntry{{key: "a", value: "1"}, {key: "c", value: "3"}}),
		newMergeTestSource([]mergeTestEntry{{key: "a", value: "0"}, {key: "d", value: "4"}}),
	})
	defer merge.Close()

	if !merge.Seek([]byte("")) {
		t.Fatal("Seek(\"\") = false, want true")
	}

	assertMergedKeysValues(t, merge,
		[]string{"a", "b", "c", "d"},
		[]string{"1", "2", "3", "4"},
		[]bool{false, false, false, false},
	)
}

func TestMergeIterator_Seek(t *testing.T) {
	t.Parallel()

	merge := NewMergeIterator([]storage.Iterator{
		newMergeTestSource([]mergeTestEntry{{key: "b", value: "2"}, {key: "d", value: "4"}}),
		newMergeTestSource([]mergeTestEntry{{key: "a", value: "1"}, {key: "c", value: "3"}, {key: "e", value: "5"}}),
	})
	defer merge.Close()

	if !merge.Seek([]byte("c")) {
		t.Fatal("Seek(c) = false, want true")
	}
	if got := string(merge.Key()); got != "c" {
		t.Fatalf("Seek(c) key = %q, want %q", got, "c")
	}

	if !merge.Seek([]byte("d")) {
		t.Fatal("Seek(d) = false, want true")
	}
	if got := string(merge.Key()); got != "d" {
		t.Fatalf("Seek(d) key = %q, want %q", got, "d")
	}

	if merge.Seek([]byte("z")) {
		t.Fatal("Seek(z) = true, want false")
	}
	if merge.Valid() {
		t.Fatal("Valid() = true after Seek(z), want false")
	}
}

func TestDeletedFilterIterator(t *testing.T) {
	t.Parallel()

	filter := NewDeletedFilterIterator(NewMergeIterator([]storage.Iterator{
		newMergeTestSource([]mergeTestEntry{{key: "b", tombstone: true}, {key: "d", tombstone: true}}),
		newMergeTestSource([]mergeTestEntry{{key: "a", value: "1"}, {key: "b", value: "2"}, {key: "c", value: "3"}, {key: "d", value: "4"}, {key: "e", value: "5"}}),
	}))
	defer filter.Close()

	if !filter.Seek([]byte("")) {
		t.Fatal("Seek(\"\") = false, want true")
	}

	assertMergedKeysValues(t, filter,
		[]string{"a", "c", "e"},
		[]string{"1", "3", "5"},
		[]bool{false, false, false},
	)
}

type mergeTestEntry struct {
	key       string
	value     string
	tombstone bool
}

func newMergeTestSource(entries []mergeTestEntry) storage.Iterator {
	mem := NewMemTable()
	for _, entry := range entries {
		if entry.tombstone {
			mem.Delete([]byte(entry.key))
			continue
		}
		mem.Put([]byte(entry.key), []byte(entry.value))
	}
	return mem.NewIterator()
}

func assertMergedKeysValues(t *testing.T, iter storage.Iterator, wantKeys, wantValues []string, wantTombstones []bool) {
	t.Helper()

	var keys []string
	var values []string
	var tombstones []bool

	for iter.Valid() {
		keys = append(keys, string(iter.Key()))
		values = append(values, string(iter.Value()))
		tombstones = append(tombstones, iter.IsTombstone())
		iter.Next()
	}

	if len(keys) != len(wantKeys) {
		t.Fatalf("len(keys) = %d, want %d", len(keys), len(wantKeys))
	}
	for i := range wantKeys {
		if keys[i] != wantKeys[i] {
			t.Fatalf("keys[%d] = %q, want %q", i, keys[i], wantKeys[i])
		}
		if values[i] != wantValues[i] {
			t.Fatalf("values[%d] = %q, want %q", i, values[i], wantValues[i])
		}
		if tombstones[i] != wantTombstones[i] {
			t.Fatalf("tombstones[%d] = %v, want %v", i, tombstones[i], wantTombstones[i])
		}
	}
}
