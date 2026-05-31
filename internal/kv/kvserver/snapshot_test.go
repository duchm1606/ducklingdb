package kvserver

import (
	"bytes"
	"os"
	"testing"
	"time"

	"github.com/duchm1606/ducklingdb/internal/raft"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

func newTestEngine(t *testing.T) *lsm.LSMEngine {
	t.Helper()
	dir, err := os.MkdirTemp("", "snapshot-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	eng, err := lsm.OpenLSM(lsm.LSMOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })
	return eng
}

func TestSerializeEngineStateExcludesRaftKeys(t *testing.T) {
	eng := newTestEngine(t)
	// Write a mix of user keys and \x00raft/ keys.
	if err := eng.Put([]byte("user-a"), []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put([]byte("user-b"), []byte("beta")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put([]byte("\x00raft/hardstate"), []byte("internal")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Put([]byte("\x00raft/log/something"), []byte("logentry")); err != nil {
		t.Fatal(err)
	}

	data, err := serializeEngineState(eng)
	if err != nil {
		t.Fatal(err)
	}

	// Apply to a fresh engine and verify only user keys arrived.
	dst := newTestEngine(t)
	if err := applyEngineSnapshot(dst, data); err != nil {
		t.Fatal(err)
	}

	// User keys present.
	for k, want := range map[string]string{"user-a": "alpha", "user-b": "beta"} {
		got, err := dst.Get([]byte(k))
		if err != nil {
			t.Errorf("user key %q: %v", k, err)
			continue
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Errorf("user key %q: got %q want %q", k, got, want)
		}
	}
	// Raft keys absent.
	for _, k := range []string{"\x00raft/hardstate", "\x00raft/log/something"} {
		_, err := dst.Get([]byte(k))
		if err == nil {
			t.Errorf("raft key %q should not be in snapshot", k)
		}
	}
}

func TestClearStateMachineLeavesRaftKeys(t *testing.T) {
	eng := newTestEngine(t)
	eng.Put([]byte("user-a"), []byte("alpha"))
	eng.Put([]byte("user-b"), []byte("beta"))
	eng.Put([]byte("\x00raft/hardstate"), []byte("internal"))

	if err := clearStateMachine(eng); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"user-a", "user-b"} {
		if _, err := eng.Get([]byte(k)); err == nil {
			t.Errorf("user key %q should have been cleared", k)
		}
	}
	got, err := eng.Get([]byte("\x00raft/hardstate"))
	if err != nil {
		t.Fatalf("raft key should remain: %v", err)
	}
	if !bytes.Equal(got, []byte("internal")) {
		t.Fatalf("raft key value changed: %q", got)
	}
}

func TestApplyEngineSnapshotOverwritesExisting(t *testing.T) {
	src := newTestEngine(t)
	src.Put([]byte("k1"), []byte("from-snapshot"))

	data, err := serializeEngineState(src)
	if err != nil {
		t.Fatal(err)
	}

	dst := newTestEngine(t)
	// Pre-populate dst with a different value at k1 and an unrelated key.
	dst.Put([]byte("k1"), []byte("old-value"))
	dst.Put([]byte("k2"), []byte("orphan"))

	// applyEngineSnapshot overwrites k1 but does not delete k2.
	// (Production code wraps this with clearStateMachine first.)
	if err := applyEngineSnapshot(dst, data); err != nil {
		t.Fatal(err)
	}
	got, _ := dst.Get([]byte("k1"))
	if !bytes.Equal(got, []byte("from-snapshot")) {
		t.Errorf("k1: got %q want %q", got, "from-snapshot")
	}
	// k2 still there (this confirms apply does NOT wipe — caller is responsible).
	got, _ = dst.Get([]byte("k2"))
	if !bytes.Equal(got, []byte("orphan")) {
		t.Errorf("k2 should still be present pre-clearStateMachine: got %q", got)
	}
}

func TestReplicaInstallSnapshot(t *testing.T) {
	// End-to-end: build a snapshot on engine A, install it onto a fresh
	// Replica's engine, verify the Replica's state matches and the Raft
	// log is compacted appropriately.
	src := newTestEngine(t)
	src.Put([]byte("account:42"), []byte("balance=100"))
	src.Put([]byte("account:43"), []byte("balance=200"))

	snapData, err := serializeEngineState(src)
	if err != nil {
		t.Fatal(err)
	}
	snap := raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: 50, Term: 3},
		Data:     snapData,
	}

	// Set up a fresh Replica.
	dst := newTestEngine(t)
	clock := hlc.NewClock(hlc.SystemWallClock(), 500*time.Millisecond)
	storage := raft.NewLSMLogStorage(dst)
	bh := NewBatchHandler(dst, clock)
	rn := raft.NewRawNode(1, []uint64{1, 2}, storage)
	r := NewReplica(1, rn, storage, bh, func([]raft.Message) {})

	// Seed dst with a stale key that should be wiped on snapshot install.
	dst.Put([]byte("stale-key"), []byte("should-be-gone"))
	// Also seed a log entry so we can verify compaction.
	if err := storage.AppendEntries([]raft.Entry{{Term: 1, Index: 1, Data: []byte("e1")}}); err != nil {
		t.Fatal(err)
	}

	if err := r.installSnapshot(snap); err != nil {
		t.Fatalf("installSnapshot: %v", err)
	}

	// Verify the engine now has the snapshot's user keys.
	for k, want := range map[string]string{
		"account:42": "balance=100",
		"account:43": "balance=200",
	} {
		got, err := dst.Get([]byte(k))
		if err != nil {
			t.Errorf("post-install key %q: %v", k, err)
			continue
		}
		if !bytes.Equal(got, []byte(want)) {
			t.Errorf("post-install key %q: got %q want %q", k, got, want)
		}
	}
	// Stale key should be gone.
	if _, err := dst.Get([]byte("stale-key")); err == nil {
		t.Error("stale key should have been wiped by installSnapshot")
	}
	// Snapshot record durable on this replica.
	loaded, err := storage.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Metadata.Index != 50 || loaded.Metadata.Term != 3 {
		t.Errorf("persisted snapshot metadata mismatch: %+v", loaded.Metadata)
	}
	// Pre-existing log entry should be compacted.
	first, _ := storage.FirstIndex()
	if first != 51 {
		t.Errorf("FirstIndex after snapshot install want 51, got %d", first)
	}
	// Applied index should match the snapshot.
	applied, _ := storage.LoadApplied()
	if applied != 50 {
		t.Errorf("applied after install want 50, got %d", applied)
	}
}

func TestCreateSnapshotPersistsAndCompacts(t *testing.T) {
	eng := newTestEngine(t)
	clock := hlc.NewClock(hlc.SystemWallClock(), 500*time.Millisecond)
	storage := raft.NewLSMLogStorage(eng)
	bh := NewBatchHandler(eng, clock)
	rn := raft.NewRawNode(1, []uint64{1}, storage)
	r := NewReplica(1, rn, storage, bh, func([]raft.Message) {})

	// Populate the log and the engine with some state.
	entries := []raft.Entry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
		{Term: 1, Index: 3, Data: []byte("c")},
	}
	if err := storage.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	storage.SaveHardState(raft.HardState{Term: 1, Commit: 3})
	eng.Put([]byte("user-k"), []byte("user-v"))

	meta, err := r.CreateSnapshot(3)
	if err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}
	if meta.Index != 3 {
		t.Errorf("snapshot index want 3, got %d", meta.Index)
	}

	// Snapshot is durable.
	got, err := storage.Snapshot()
	if err != nil || got.IsEmpty() {
		t.Fatalf("snapshot not persisted: snap=%+v err=%v", got, err)
	}
	// Verify snapshot's data contains the user key.
	dst := newTestEngine(t)
	if err := applyEngineSnapshot(dst, got.Data); err != nil {
		t.Fatal(err)
	}
	v, err := dst.Get([]byte("user-k"))
	if err != nil {
		t.Fatalf("snapshot did not capture user-k: %v", err)
	}
	if !bytes.Equal(v, []byte("user-v")) {
		t.Errorf("user-k: got %q want %q", v, "user-v")
	}
	// Log entries 1..3 should be compacted; FirstIndex == 4.
	first, _ := storage.FirstIndex()
	if first != 4 {
		t.Errorf("FirstIndex after snapshot want 4, got %d", first)
	}
}

// compile-time check: clearStateMachine is correctly typed
var _ = func(eng storage.Engine) { _ = clearStateMachine(eng) }
