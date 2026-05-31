package kvserver

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/raft"
	"google.golang.org/protobuf/proto"
)

const tickInterval = 10 * time.Millisecond

type proposalMsg struct {
	data   []byte
	doneCh chan error
}

// Replica owns a RawNode and runs the Raft Ready loop.
// It persists log entries, sends messages to peers, and applies
// committed entries to the MVCC engine via BatchHandler.
type Replica struct {
	id      uint64
	rn      *raft.RawNode
	storage *raft.LSMLogStorage
	batch   *BatchHandler

	propc  chan proposalMsg
	stepc  chan raft.Message
	stopc  chan struct{}
	ticker *time.Ticker

	mu      sync.Mutex
	pending map[uint64]chan error // logIndex → done channel

	// pendingProps holds proposals waiting for their log index assignment.
	pendingProps []proposalMsg

	// sendFn delivers outbound Raft messages.
	// In production: sendFn sends via gRPC. In tests: delivers in-process.
	sendFn func([]raft.Message)

	// leadID caches the current leader ID from SoftState, updated only inside
	// the run() goroutine. Reads from other goroutines use atomic loads to
	// avoid data races with the concurrent write in handleReady().
	leadID atomic.Uint64

	// stopOnce ensures Stop() is idempotent and never double-closes stopc.
	stopOnce sync.Once
}

// NewReplica constructs a Replica. sendFn delivers outbound messages.
func NewReplica(id uint64, rn *raft.RawNode, storage *raft.LSMLogStorage,
	batch *BatchHandler, sendFn func([]raft.Message)) *Replica {
	return &Replica{
		id:      id,
		rn:      rn,
		storage: storage,
		batch:   batch,
		propc:   make(chan proposalMsg, 16),
		stepc:   make(chan raft.Message, 64),
		stopc:   make(chan struct{}),
		pending: make(map[uint64]chan error),
		sendFn:  sendFn,
	}
}

// Start begins the Ready loop goroutine.
func (r *Replica) Start() {
	r.ticker = time.NewTicker(tickInterval)
	go r.run()
}

// Stop shuts down the Ready loop. Safe to call multiple times.
func (r *Replica) Stop() {
	r.stopOnce.Do(func() {
		close(r.stopc)
		r.ticker.Stop()
	})
}

// Lead returns the current leader ID as seen by this replica.
// Safe to call from any goroutine.
func (r *Replica) Lead() uint64 { return r.leadID.Load() }

// Propose submits a command and blocks until it is committed and applied.
// Returns an error if the replica is not the leader or the context expires.
func (r *Replica) Propose(ctx context.Context, data []byte) error {
	if r.leadID.Load() != r.id {
		return errors.New("not the leader")
	}
	doneCh := make(chan error, 1)
	select {
	case r.propc <- proposalMsg{data: data, doneCh: doneCh}:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopc:
		return errors.New("replica stopped")
	}
	select {
	case err := <-doneCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopc:
		return errors.New("replica stopped")
	}
}

// Step delivers an inbound Raft message from a peer.
func (r *Replica) Step(msg raft.Message) {
	select {
	case r.stepc <- msg:
	default:
		log.Printf("replica %d: stepc full, dropping message type=%d", r.id, msg.Type)
	}
}

func (r *Replica) run() {
	for {
		select {
		case <-r.stopc:
			return
		case <-r.ticker.C:
			r.rn.Tick()
		case prop := <-r.propc:
			r.pendingProps = append(r.pendingProps, prop)
			r.rn.Propose(prop.data)
		case msg := <-r.stepc:
			r.rn.Step(msg)
		}
		r.handleReady()
	}
}

func (r *Replica) handleReady() {
	if !r.rn.HasReady() {
		return
	}
	rd := r.rn.Ready()

	// Cache the leader ID atomically so Propose() and external callers can
	// read it from any goroutine without racing against this write.
	if rd.SoftState != nil {
		r.leadID.Store(rd.SoftState.Lead)
	}

	// 1. Persist HardState
	if !raft.IsEmptyHardState(rd.HardState) {
		if err := r.storage.SaveHardState(rd.HardState); err != nil {
			log.Printf("replica %d: save HardState: %v", r.id, err)
		}
	}

	// 2. Install snapshot if one is pending.
	// Order matters: snapshot before entries — the snapshot represents state
	// up through some index, and any unstable entries above it are layered on
	// top by Raft's normal append path.
	if !rd.Snapshot.IsEmpty() {
		if err := r.installSnapshot(rd.Snapshot); err != nil {
			log.Printf("replica %d: install snapshot at index %d: %v",
				r.id, rd.Snapshot.Metadata.Index, err)
		} else {
			log.Printf("replica %d: installed snapshot at index %d term %d (%d bytes)",
				r.id, rd.Snapshot.Metadata.Index, rd.Snapshot.Metadata.Term,
				len(rd.Snapshot.Data))
		}
	}

	// 3. Persist Entries and assign pending proposal done channels
	if len(rd.Entries) > 0 {
		if err := r.storage.AppendEntries(rd.Entries); err != nil {
			log.Printf("replica %d: append entries: %v", r.id, err)
		}
		// Match persisted entries to pending proposals (FIFO order).
		for _, e := range rd.Entries {
			if len(r.pendingProps) > 0 && e.Data != nil {
				prop := r.pendingProps[0]
				r.pendingProps = r.pendingProps[1:]
				r.mu.Lock()
				r.pending[e.Index] = prop.doneCh
				r.mu.Unlock()
			}
		}
	}

	// 4. Send messages to peers
	if len(rd.Messages) > 0 {
		r.sendFn(rd.Messages)
	}

	// 5. Apply committed entries
	for _, entry := range rd.CommittedEntries {
		r.applyEntry(entry)
	}

	// 5b. Durably record how far we have applied so that a crash between
	// SaveHardState (which advances Commit) and here does not cause those
	// entries to be silently skipped on restart.
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		if err := r.storage.SaveApplied(last.Index); err != nil {
			log.Printf("replica %d: save applied index %d: %v", r.id, last.Index, err)
		}
	}

	// 6. Advance
	r.rn.Advance(rd)
}

// installSnapshot wipes the state machine's user-data, applies the snapshot's
// payload, persists the snapshot record, advances applied, and compacts the
// log up to the snapshot's anchor index.
//
// The order is deliberately conservative:
//  1. Wipe + apply: the state machine reaches the snapshot's view.
//  2. SaveApplied(snap.Index): if we crash here, the engine has the snapshot's
//     contents and the applied index agrees, so on restart we skip re-applying
//     entries below the snapshot.
//  3. SaveSnapshot: makes the snapshot durable on this replica so future
//     restarts know about it (and Compact in step 4 is safe).
//  4. Compact: reclaim log entries ≤ snap.Index. After this, FirstIndex moves
//     forward.
//
// A crash between any two steps leaves the replica recoverable: SaveApplied
// after SaveSnapshot would be equivalent semantically, but doing SaveApplied
// first means a crash between (2) and (3) leaves the engine consistent with
// the new applied index even if the snapshot record itself is missing — the
// next leader will simply re-send the snapshot.
func (r *Replica) installSnapshot(snap raft.Snapshot) error {
	eng := r.batch.Engine()

	if err := clearStateMachine(eng); err != nil {
		return fmt.Errorf("wipe: %w", err)
	}
	if err := applyEngineSnapshot(eng, snap.Data); err != nil {
		return fmt.Errorf("apply: %w", err)
	}
	if err := r.storage.SaveApplied(snap.Metadata.Index); err != nil {
		return fmt.Errorf("save applied: %w", err)
	}
	if err := r.storage.SaveSnapshot(snap); err != nil {
		return fmt.Errorf("save snapshot: %w", err)
	}
	if err := r.storage.Compact(snap.Metadata.Index); err != nil {
		return fmt.Errorf("compact: %w", err)
	}
	return nil
}

// CreateSnapshot captures the current state of the engine as a Raft snapshot
// at the given applied index. The caller is responsible for ensuring
// appliedIndex matches what the state machine has applied (typically the
// RawNode's applied watermark). Returns the snapshot's metadata.
//
// Concurrency: this method takes the engine's natural read view; concurrent
// writes during snapshot creation are tolerated (the snapshot reflects the
// engine state at some point during the serialization), but for crash
// recovery to align cleanly the caller should quiesce writes or accept a
// snapshot whose data may be slightly newer than appliedIndex would suggest.
// DucklingDB's M4 single-goroutine Ready loop keeps the snapshot atomic with
// respect to applies because CreateSnapshot runs on the same goroutine.
func (r *Replica) CreateSnapshot(appliedIndex uint64) (raft.SnapshotMetadata, error) {
	term, err := r.rn.LogTerm(appliedIndex)
	if err != nil {
		return raft.SnapshotMetadata{}, fmt.Errorf("snapshot: term for index %d: %w",
			appliedIndex, err)
	}
	data, err := serializeEngineState(r.batch.Engine())
	if err != nil {
		return raft.SnapshotMetadata{}, err
	}
	snap := raft.Snapshot{
		Metadata: raft.SnapshotMetadata{Index: appliedIndex, Term: term},
		Data:     data,
	}
	if err := r.storage.SaveSnapshot(snap); err != nil {
		return raft.SnapshotMetadata{}, fmt.Errorf("snapshot: save: %w", err)
	}
	if err := r.storage.Compact(appliedIndex); err != nil {
		return raft.SnapshotMetadata{}, fmt.Errorf("snapshot: compact: %w", err)
	}
	return snap.Metadata, nil
}

func (r *Replica) applyEntry(entry raft.Entry) {
	var applyErr error
	if len(entry.Data) > 0 {
		var req pb.BatchRequest
		if err := proto.Unmarshal(entry.Data, &req); err != nil {
			log.Printf("replica %d: unmarshal entry %d: %v", r.id, entry.Index, err)
			applyErr = err
		} else {
			if _, err := r.batch.Batch(context.Background(), &req); err != nil {
				log.Printf("replica %d: apply entry %d: %v", r.id, entry.Index, err)
				applyErr = err
			}
		}
	}
	// Signal pending proposal if one exists for this index.
	r.mu.Lock()
	if ch, ok := r.pending[entry.Index]; ok {
		ch <- applyErr
		delete(r.pending, entry.Index)
	}
	r.mu.Unlock()
}
