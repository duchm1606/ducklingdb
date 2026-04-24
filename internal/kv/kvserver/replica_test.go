package kvserver

import (
	"context"
	"os"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/raft"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/storage/mvcc"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
	"google.golang.org/protobuf/proto"
)

// testTransport delivers Raft messages in-process between replicas.
type testTransport struct {
	replicas map[uint64]*Replica
}

func (tr *testTransport) send(msgs []raft.Message) {
	for _, m := range msgs {
		if r, ok := tr.replicas[m.To]; ok {
			r.Step(m)
		}
	}
}

func newTestReplica(t *testing.T, id uint64, peers []uint64, tr *testTransport) *Replica {
	t.Helper()
	dir, _ := os.MkdirTemp("", "replica-test-*")
	t.Cleanup(func() { os.RemoveAll(dir) })

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
	tr.replicas[id] = r
	return r
}

func TestReplicaProposalApplied(t *testing.T) {
	peers := []uint64{1, 2, 3}
	tr := &testTransport{replicas: make(map[uint64]*Replica)}

	r1 := newTestReplica(t, 1, peers, tr)
	r2 := newTestReplica(t, 2, peers, tr)
	r3 := newTestReplica(t, 3, peers, tr)
	_ = r2
	_ = r3

	r1.Start()
	r2.Start()
	r3.Start()
	defer r1.Stop()
	defer r2.Stop()
	defer r3.Stop()

	// Wait for a leader to emerge
	var leaderReplica *Replica
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, r := range tr.replicas {
			if r.Lead() != 0 && r.Lead() == r.id {
				leaderReplica = r
			}
		}
		if leaderReplica != nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if leaderReplica == nil {
		t.Fatal("no leader elected within 3s")
	}

	// Propose a Put via BatchRequest.
	// Use a non-zero timestamp so the write is stored as a versioned MVCC
	// entry (rather than an inline entry), which MVCCScan can retrieve.
	writeTS := &pb.Timestamp{WallTime: time.Now().UnixNano()}
	req := &pb.BatchRequest{
		Header: &pb.Header{Timestamp: writeTS},
		Requests: []*pb.RequestUnion{{
			Value: &pb.RequestUnion_Put{Put: &pb.PutRequest{
				Key:   []byte("testkey"),
				Value: &pb.Value{RawBytes: []byte("testval")},
			}},
		}},
	}
	data, _ := proto.Marshal(req)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := leaderReplica.Propose(ctx, data); err != nil {
		t.Fatalf("propose failed: %v", err)
	}

	// Wait for all replicas to apply the committed entry
	time.Sleep(300 * time.Millisecond)

	// Verify all replicas have the key via MVCC scan
	for id, r := range tr.replicas {
		kvs, err := mvcc.MVCCScan(
			r.batch.Engine(),
			[]byte("testkey"),
			[]byte("testkey\x00"),
			hlc.Timestamp{WallTime: time.Now().UnixNano()},
			mvcc.ReadOptions{},
		)
		if err != nil {
			t.Errorf("replica %d: MVCCScan failed: %v", id, err)
			continue
		}
		if len(kvs) == 0 {
			t.Errorf("replica %d: key not found after replication", id)
			continue
		}
		if string(kvs[0].Value) != "testval" {
			t.Errorf("replica %d: got %q, want %q", id, string(kvs[0].Value), "testval")
		}
	}
}
