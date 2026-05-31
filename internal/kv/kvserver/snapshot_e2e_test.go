package kvserver

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/raft"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
	"google.golang.org/protobuf/proto"
)

// gatedTransport delivers Raft messages between replicas but can drop messages
// destined for a "partitioned" peer. Used to simulate a follower that misses
// log replication so the leader has to fall back to MsgSnap on recovery.
type gatedTransport struct {
	mu         sync.RWMutex
	replicas   map[uint64]*Replica
	partitioned map[uint64]bool
}

func newGatedTransport() *gatedTransport {
	return &gatedTransport{
		replicas:    make(map[uint64]*Replica),
		partitioned: make(map[uint64]bool),
	}
}

func (tr *gatedTransport) send(msgs []raft.Message) {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	for _, m := range msgs {
		if tr.partitioned[m.From] || tr.partitioned[m.To] {
			continue
		}
		if r, ok := tr.replicas[m.To]; ok {
			r.Step(m)
		}
	}
}

func (tr *gatedTransport) partition(id uint64) {
	tr.mu.Lock()
	tr.partitioned[id] = true
	tr.mu.Unlock()
}

func (tr *gatedTransport) heal(id uint64) {
	tr.mu.Lock()
	delete(tr.partitioned, id)
	tr.mu.Unlock()
}

// newReplicaWithDir creates a Replica on a specific directory so tests can
// "restart" a node by re-opening the same engine.
func newReplicaWithDir(t *testing.T, id uint64, peers []uint64,
	dir string, tr *gatedTransport) *Replica {
	t.Helper()
	eng, err := lsm.OpenLSM(lsm.LSMOptions{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { eng.Close() })

	clock := hlc.NewClock(hlc.SystemWallClock(), 500*time.Millisecond)
	storage := raft.NewLSMLogStorage(eng)
	rn := raft.NewRawNode(id, peers, storage)
	bh := NewBatchHandler(eng, clock)
	r := NewReplica(id, rn, storage, bh, tr.send)
	tr.mu.Lock()
	tr.replicas[id] = r
	tr.mu.Unlock()
	return r
}

func waitForLeader(t *testing.T, tr *gatedTransport, timeout time.Duration) *Replica {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		tr.mu.RLock()
		for _, r := range tr.replicas {
			if r.Lead() != 0 && r.Lead() == r.id {
				tr.mu.RUnlock()
				return r
			}
		}
		tr.mu.RUnlock()
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func proposePut(t *testing.T, r *Replica, key, value []byte) {
	t.Helper()
	req := &pb.BatchRequest{
		Header: &pb.Header{Timestamp: &pb.Timestamp{WallTime: time.Now().UnixNano()}},
		Requests: []*pb.RequestUnion{{
			Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   key,
				Value: &pb.Value{RawBytes: value},
			}},
		}},
	}
	data, _ := proto.Marshal(req)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Propose(ctx, data); err != nil {
		t.Fatalf("propose failed: %v", err)
	}
}

// TestSnapshotE2ELaggingFollowerRecovers exercises the full snapshot path:
// a follower is partitioned, the leader proposes writes and snapshots the log
// past the entries the follower missed, then the partition heals — the leader
// must ship a snapshot rather than the (compacted) entries.
func TestSnapshotE2ELaggingFollowerRecovers(t *testing.T) {
	tr := newGatedTransport()
	peers := []uint64{1, 2, 3}

	dirs := make(map[uint64]string)
	for _, id := range peers {
		d, _ := os.MkdirTemp("", "snap-e2e-*")
		t.Cleanup(func() { os.RemoveAll(d) })
		dirs[id] = d
	}

	r1 := newReplicaWithDir(t, 1, peers, dirs[1], tr)
	r2 := newReplicaWithDir(t, 2, peers, dirs[2], tr)
	r3 := newReplicaWithDir(t, 3, peers, dirs[3], tr)

	r1.Start()
	r2.Start()
	r3.Start()
	t.Cleanup(func() {
		r1.Stop()
		r2.Stop()
		r3.Stop()
	})

	leader := waitForLeader(t, tr, 3*time.Second)
	var follower *Replica
	for _, r := range []*Replica{r1, r2, r3} {
		if r.id != leader.id {
			follower = r
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower")
	}
	t.Logf("leader=%d follower=%d (lagging)", leader.id, follower.id)

	// 1. Replicate one write that all nodes see.
	proposePut(t, leader, []byte("baseline"), []byte("v0"))
	time.Sleep(200 * time.Millisecond)

	// 2. Partition the follower; leader continues to write.
	tr.partition(follower.id)
	for i := 0; i < 10; i++ {
		proposePut(t, leader, []byte("postpartition-"+string(rune('a'+i))), []byte("v"))
	}
	// Give the leader time to commit on the remaining quorum.
	time.Sleep(300 * time.Millisecond)

	// 3. Leader snapshots and compacts.
	appliedIdx := leader.rn.Applied()
	t.Logf("leader applied=%d, creating snapshot", appliedIdx)
	if _, err := leader.CreateSnapshot(appliedIdx); err != nil {
		t.Fatalf("CreateSnapshot: %v", err)
	}

	// 4. Heal the partition. Leader's sendAppend should detect the follower
	// is below firstIndex and ship MsgSnap instead.
	tr.heal(follower.id)

	// 5. Wait for the follower to catch up via snapshot.
	deadline := time.Now().Add(5 * time.Second)
	caughtUp := false
	for time.Now().Before(deadline) {
		eng := follower.batch.Engine()
		// Check for the most recent write (postpartition-j).
		kvs, err := mvcc.MVCCScan(eng,
			[]byte("postpartition-j"),
			[]byte("postpartition-j\x00"),
			hlc.Timestamp{WallTime: time.Now().UnixNano()},
			mvcc.ReadOptions{})
		if err == nil && len(kvs) > 0 && bytes.Equal(kvs[0].Value, []byte("v")) {
			caughtUp = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !caughtUp {
		t.Fatal("follower did not catch up via snapshot within 5s")
	}

	// Also verify baseline is visible (was committed before partition).
	kvs, err := mvcc.MVCCScan(follower.batch.Engine(),
		[]byte("baseline"),
		[]byte("baseline\x00"),
		hlc.Timestamp{WallTime: time.Now().UnixNano()},
		mvcc.ReadOptions{})
	if err != nil {
		t.Fatalf("baseline scan: %v", err)
	}
	if len(kvs) == 0 || !bytes.Equal(kvs[0].Value, []byte("v0")) {
		t.Errorf("baseline missing or wrong on follower after snapshot: %+v", kvs)
	}

	// And the follower's snapshot record should be persisted.
	snap, err := follower.storage.Snapshot()
	if err != nil {
		t.Fatalf("follower snapshot read: %v", err)
	}
	if snap.IsEmpty() {
		t.Fatal("follower should have a persisted snapshot after recovery")
	}
	if snap.Metadata.Index < appliedIdx {
		t.Errorf("follower snapshot index %d < leader's snapshot index %d",
			snap.Metadata.Index, appliedIdx)
	}
}
