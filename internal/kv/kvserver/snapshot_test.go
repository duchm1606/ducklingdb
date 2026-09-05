package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/raft"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
	"google.golang.org/protobuf/proto"
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

// TestCreateSnapshotRoutesThroughReadyLoop proves the routing that block 2
// exists to establish: the exported CreateSnapshot hands its work to the run()
// goroutine and gets the result back, rather than touching the RawNode, log
// storage and engine from the caller's goroutine.
//
// TestCreateSnapshotPersistsAndCompacts covers the snapshot *mechanism* by
// calling the loop body directly on a replica that was never started, so it
// deliberately does not exercise this path. Without this test, the serialization
// itself would have no coverage — mechanism proven, routing unproven.
//
// Run under -race to be meaningful: the point is that a snapshot taken while the
// loop is actively applying entries produces no data race.
func TestCreateSnapshotRoutesThroughReadyLoop(t *testing.T) {
	tr := &testTransport{replicas: make(map[uint64]*Replica)}
	r := newTestReplica(t, 1, []uint64{1}, tr)
	r.Start()
	t.Cleanup(r.Stop)

	// Single-node group elects itself once the election timeout fires.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && r.Lead() != r.id {
		time.Sleep(10 * time.Millisecond)
	}
	if r.Lead() != r.id {
		t.Fatal("replica did not become leader")
	}

	// Keep the Ready loop busy applying entries for the whole call, so the
	// snapshot genuinely overlaps with mutation of the state it reads.
	stop := make(chan struct{})
	done := make(chan struct{})

	// Record, per key, the applied watermark observed *before* that key was
	// proposed. A key's own log entry is necessarily at an index strictly
	// greater than that watermark, which is what lets the assertion below
	// survive the log compaction CreateSnapshot performs.
	var mu sync.Mutex
	appliedBefore := make(map[string]uint64)

	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			key := fmt.Sprintf("bg-%04d", i)
			before := r.AppliedIndex()

			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			req := &pb.BatchRequest{
				Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: time.Now().UnixNano()}},
				Requests: []*pb.RequestUnion{{
					Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
						Key:   []byte(key),
						Value: &pb.Value{RawBytes: []byte("v")},
					}},
				}},
			}
			data, _ := proto.Marshal(req)
			err := r.Propose(ctx, data)
			cancel()
			if err == nil {
				mu.Lock()
				appliedBefore[key] = before
				mu.Unlock()
			}
		}
	}()

	// Give the background writer time to get entries applied.
	time.Sleep(200 * time.Millisecond)

	meta, err := r.CreateSnapshot()

	close(stop)
	<-done

	if err != nil {
		t.Fatalf("CreateSnapshot via Ready loop: %v", err)
	}
	if meta.Index == 0 {
		t.Fatal("snapshot anchored at index 0; nothing was applied")
	}

	snap, err := r.storage.Snapshot()
	if err != nil {
		t.Fatalf("read back snapshot: %v", err)
	}
	if snap.IsEmpty() {
		t.Fatal("snapshot was not persisted")
	}

	// The real property: the snapshot's data must correspond to its index.
	//
	// Asserting `meta.Index == <the index we passed in>` was a tautology — the
	// implementation copied the argument straight into the metadata, so no
	// implementation could fail it.
	//
	// Instead: for each key in the snapshot, its log entry sits at an index
	// strictly greater than the applied watermark observed just before it was
	// proposed. So if that watermark is already >= the snapshot's index, the
	// key's entry is *after* the snapshot's anchor and must not appear in the
	// snapshot's data. That is exactly what happens when the index is sampled
	// off-loop and the loop applies more entries before serializing.
	mu.Lock()
	recorded := make(map[string]uint64, len(appliedBefore))
	for k, v := range appliedBefore {
		recorded[k] = v
	}
	mu.Unlock()

	for _, k := range keysInSnapshot(t, snap.Data) {
		before, ok := recorded[k]
		if !ok {
			continue // proposal did not report success; no sound bound
		}
		if before >= meta.Index {
			t.Fatalf("snapshot labelled index %d contains key %q, whose log entry is at an "+
				"index > %d: snapshot data is ahead of its own index",
				meta.Index, k, before)
		}
	}
}

// keysInSnapshot applies the snapshot blob to a scratch engine and returns the
// user keys it contains.
func keysInSnapshot(t *testing.T, data []byte) []string {
	t.Helper()
	scratch := newTestEngine(t)
	if err := applyEngineSnapshot(scratch, data); err != nil {
		t.Fatalf("apply snapshot to scratch engine: %v", err)
	}
	iter, err := scratch.NewIterator()
	if err != nil {
		t.Fatalf("scratch iterator: %v", err)
	}
	defer iter.Close()

	seen := make(map[string]bool)
	var keys []string
	for ok := iter.Seek(nil); ok; ok = iter.Next() {
		k := iter.Key()
		if len(k) > 0 && k[0] == 0x00 {
			continue // raft-internal keys are excluded from snapshots
		}
		decoded, _, err := mvcc.Decode(k)
		if err != nil {
			continue
		}
		if s := string(decoded.Key); !seen[s] {
			seen[s] = true
			keys = append(keys, s)
		}
	}
	return keys
}

// TestCreateSnapshotStoppedReplicaDoesNotHang proves the request path fails
// fast instead of blocking forever when there is no run() goroutine to serve it.
func TestCreateSnapshotStoppedReplicaDoesNotHang(t *testing.T) {
	tr := &testTransport{replicas: make(map[uint64]*Replica)}
	r := newTestReplica(t, 1, []uint64{1}, tr)
	r.Start()
	r.Stop()

	errC := make(chan error, 1)
	go func() {
		_, err := r.CreateSnapshot()
		errC <- err
	}()

	select {
	case err := <-errC:
		if err == nil {
			t.Fatal("expected an error from a stopped replica, got nil")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("CreateSnapshot hung on a stopped replica")
	}
}

func TestCreateSnapshotPersistsAndCompacts(t *testing.T) {
	eng := newTestEngine(t)
	clock := hlc.NewClock(hlc.SystemWallClock(), 500*time.Millisecond)
	storage := raft.NewLSMLogStorage(eng)
	bh := NewBatchHandler(eng, clock)

	// Populate the log and the durable applied watermark *before* constructing
	// the RawNode, which restores its applied index from storage. The snapshot
	// index is no longer a parameter — the loop reads Applied() itself — so the
	// precondition has to be established here rather than asserted by passing a
	// number in.
	entries := []raft.Entry{
		{Term: 1, Index: 1, Data: []byte("a")},
		{Term: 1, Index: 2, Data: []byte("b")},
		{Term: 1, Index: 3, Data: []byte("c")},
	}
	if err := storage.AppendEntries(entries); err != nil {
		t.Fatal(err)
	}
	storage.SaveHardState(raft.HardState{Term: 1, Commit: 3})
	if err := storage.SaveApplied(3); err != nil {
		t.Fatal(err)
	}
	eng.Put([]byte("user-k"), []byte("user-v"))

	rn := raft.NewRawNode(1, []uint64{1}, storage)
	r := NewReplica(1, rn, storage, bh, func([]raft.Message) {})
	if got := rn.Applied(); got != 3 {
		t.Fatalf("precondition: applied want 3, got %d", got)
	}

	// This replica is never Start()ed, so there is no run() goroutine to serve
	// the public CreateSnapshot request. Call the loop body directly: this test
	// covers snapshot mechanics, not the concurrency routing.
	meta, err := r.createSnapshotOnLoop()
	if err != nil {
		t.Fatalf("createSnapshotOnLoop: %v", err)
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
