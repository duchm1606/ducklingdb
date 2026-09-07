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

// snapshotThreshold is how many applied entries may accumulate beyond the log's
// start before the Ready loop captures a snapshot and truncates. Small enough
// that a busy replica does not carry an unbounded log; large enough that steady
// single writes do not snapshot on every entry.
const snapshotThreshold uint64 = 8

type proposalMsg struct {
	data   []byte
	doneCh chan error
}

// snapshotRequest asks the Ready loop to create a snapshot on the caller's
// behalf. Snapshotting reads the RawNode, the log storage and the engine — all
// of which run() mutates — so it must execute on the run() goroutine rather
// than the caller's.
//
// The request carries no index: the loop picks the applied watermark itself.
// Letting the caller supply one made the snapshot's label and its contents
// disagree — see CreateSnapshot.
type snapshotRequest struct {
	respC chan snapshotResponse
}

type snapshotResponse struct {
	meta raft.SnapshotMetadata
	err  error
}

// appliedQuery asks the Ready loop for its applied watermark. Served on the
// loop so the answer is a real read of rn.Applied() rather than a mirror that
// can lag it.
type appliedQuery struct {
	respC chan uint64
}

// ErrNotLeader is returned by Propose when this replica is not the leader. The
// server layer maps it to a typed pb.NotLeaderError (with the leader's address)
// so both the Batch and ExecSQL entry points surface the same redirect shape.
var ErrNotLeader = errors.New("not the leader")

// RequestError is a deterministic, per-request failure produced while applying
// a command — a write-intent conflict, an unknown request type, and so on.
//
// Every replica applies the same committed entry and reaches the same outcome,
// so this is a client-level error, not divergence. It is reported to the
// proposer and deliberately not counted by ApplyErrorCount.
type RequestError struct{ Message string }

func (e *RequestError) Error() string { return e.Message }

// Replica owns a RawNode and runs the Raft Ready loop.
// It persists log entries, sends messages to peers, and applies
// committed entries to the MVCC engine via BatchHandler.
type Replica struct {
	id      uint64
	rn      *raft.RawNode
	storage *raft.LSMLogStorage
	batch   *BatchHandler

	propc    chan proposalMsg
	stepc    chan raft.Message
	snapc    chan snapshotRequest
	appliedc chan appliedQuery
	stopc    chan struct{}
	ticker   *time.Ticker

	mu      sync.Mutex
	pending map[uint64]chan error // logIndex → done channel

	// pendingProps holds proposals waiting for their log index assignment.
	pendingProps []proposalMsg

	// lastSnapIndex is the index of the most recent snapshot this replica has
	// created or installed. Tracked in-process (rather than read from storage
	// every cycle, which would deserialize the whole snapshot) so the Ready
	// loop can decide truncation cheaply. Touched only on the run() goroutine.
	lastSnapIndex uint64

	// sendFn delivers outbound Raft messages.
	// In production: sendFn sends via gRPC. In tests: delivers in-process.
	sendFn func([]raft.Message)

	// leadID caches the current leader ID from SoftState, updated only inside
	// the run() goroutine. Reads from other goroutines use atomic loads to
	// avoid data races with the concurrent write in handleReady().
	leadID atomic.Uint64

	// applyErrs counts entries that failed to apply because of an apply-machinery
	// failure. Any non-zero value means this replica may have diverged from its
	// peers. Deterministic per-request failures are excluded — see RequestError.
	applyErrs atomic.Uint64

	// stopOnce ensures Stop() is idempotent and never double-closes stopc.
	stopOnce sync.Once
}

// NewReplica constructs a Replica. sendFn delivers outbound messages.
func NewReplica(id uint64, rn *raft.RawNode, storage *raft.LSMLogStorage,
	batch *BatchHandler, sendFn func([]raft.Message)) *Replica {
	return &Replica{
		id:       id,
		rn:       rn,
		storage:  storage,
		batch:    batch,
		propc:    make(chan proposalMsg, 16),
		stepc:    make(chan raft.Message, 64),
		snapc:    make(chan snapshotRequest),
		appliedc: make(chan appliedQuery),
		stopc:    make(chan struct{}),
		ticker:   time.NewTicker(tickInterval),
		pending:  make(map[uint64]chan error),
		sendFn:   sendFn,
	}
}

// Start begins the Ready loop goroutine.
//
// The ticker is created in NewReplica, not here: Stop() reads r.ticker, and a
// Replica can now be started from one goroutine (the peer-resolution retry) and
// stopped from another (Node.Stop), which raced on that field. Creating it at
// construction also means Stop() is safe on a Replica that was never started.
func (r *Replica) Start() {
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
		return ErrNotLeader
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
		case req := <-r.snapc:
			meta, err := r.createSnapshotOnLoop()
			req.respC <- snapshotResponse{meta: meta, err: err}
		case q := <-r.appliedc:
			q.respC <- r.rn.Applied()
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

	// 7. Bound the log: snapshot past the threshold and truncate up to the safe
	// point. Runs after Advance so it reads the post-Advance applied watermark.
	r.maybeSnapshotAndCompact()
}

// maybeSnapshotAndCompact bounds Raft log growth. Once more than
// snapshotThreshold applied entries have accumulated beyond the log's start it
// captures a snapshot at the applied watermark; then, every cycle, it truncates
// the log up to CompactableIndex — which may stop short of the snapshot when a
// lagging follower still needs earlier entries. Re-running the truncation each
// cycle means the log shrinks as soon as that follower is caught up (via the
// MsgSnap that sendAppend ships it).
func (r *Replica) maybeSnapshotAndCompact() {
	applied := r.rn.Applied()
	first, err := r.storage.FirstIndex()
	if err != nil {
		return
	}
	// Capture a fresh snapshot once enough entries have piled up beyond the log
	// start, and only if we do not already have one at this watermark.
	if applied > r.lastSnapIndex && applied+1 >= first+snapshotThreshold {
		if _, err := r.createSnapshotOnLoop(); err != nil {
			log.Printf("replica %d: auto-snapshot at applied %d: %v", r.id, applied, err)
		}
	}
	// Advance truncation toward the safe point. A snapshot may have been
	// created earlier, and a lagging follower may have just acked one.
	if r.lastSnapIndex == 0 {
		return
	}
	compactTo := r.rn.CompactableIndex(r.lastSnapIndex)
	if compactTo+1 > first {
		if err := r.storage.Compact(compactTo); err != nil {
			log.Printf("replica %d: compact to %d: %v", r.id, compactTo, err)
		}
	}
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
	r.lastSnapIndex = snap.Metadata.Index
	return nil
}

// CreateSnapshot captures the current state of the engine as a Raft snapshot
// and returns the snapshot's metadata.
//
// Concurrency: the work runs on the run() goroutine, not the caller's. It reads
// the RawNode, the log storage and the engine, all of which the Ready loop
// mutates; doing it inline on the caller's goroutine is a data race, which the
// race detector reported as RawNode.Applied racing RaftLog.appliedTo.
//
// The applied index is chosen *on the loop*, which is what makes the snapshot's
// label agree with its contents. An earlier version took the index as a
// parameter, so callers read the watermark on their own goroutine and passed it
// in; the loop could then apply further entries before serializing, producing a
// snapshot labelled index N whose data reflected N+k. On install the receiver
// set applied = N and re-applied N+1…N+k over state that already contained
// them. That was masked only by MVCC apply happening to be idempotent.
func (r *Replica) CreateSnapshot() (raft.SnapshotMetadata, error) {
	respC := make(chan snapshotResponse, 1)
	select {
	case r.snapc <- snapshotRequest{respC: respC}:
	case <-r.stopc:
		return raft.SnapshotMetadata{}, errors.New("replica stopped")
	}
	select {
	case resp := <-respC:
		return resp.meta, resp.err
	case <-r.stopc:
		return raft.SnapshotMetadata{}, errors.New("replica stopped")
	}
}

// AppliedIndex returns the RawNode's applied watermark, read on the run()
// goroutine. Safe from any goroutine; reading r.rn.Applied() directly is not.
//
// This is a request on the loop rather than an atomic mirror updated at the end
// of handleReady: a mirror reports 0 on a restarted-but-idle replica whose
// durable applied index is non-zero, and lags mid-cycle.
func (r *Replica) AppliedIndex() uint64 {
	respC := make(chan uint64, 1)
	select {
	case r.appliedc <- appliedQuery{respC: respC}:
	case <-r.stopc:
		return 0
	}
	select {
	case v := <-respC:
		return v
	case <-r.stopc:
		return 0
	}
}

// ApplyErrorCount returns how many committed entries failed to apply on this
// replica because of an apply-machinery failure (a malformed entry, an engine
// error). Non-zero means this replica may have diverged from its peers.
//
// Deterministic per-request failures are not counted here: see RequestError.
func (r *Replica) ApplyErrorCount() uint64 { return r.applyErrs.Load() }

// createSnapshotOnLoop performs the snapshot. It must only be called from
// run(), and it reads the applied watermark itself so the snapshot's index and
// its data are taken at the same instant.
func (r *Replica) createSnapshotOnLoop() (raft.SnapshotMetadata, error) {
	appliedIndex := r.rn.Applied()
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
	r.lastSnapIndex = appliedIndex
	// Truncate only up to the safe point, not blindly to appliedIndex. If a
	// follower still needs entries at or below appliedIndex and has not yet
	// received a snapshot, CompactableIndex caps truncation at its match so it
	// retains a log fallback — see RawNode.CompactableIndex. On a single node
	// or a follower this equals appliedIndex, so behaviour there is unchanged.
	compactTo := r.rn.CompactableIndex(appliedIndex)
	if err := r.storage.Compact(compactTo); err != nil {
		return raft.SnapshotMetadata{}, fmt.Errorf("snapshot: compact: %w", err)
	}
	return snap.Metadata, nil
}

// applyEntry applies one committed entry to the state machine.
//
// An apply failure here is a divergence risk, not a client-level error: every
// replica applies the same committed entry, so a deterministic failure leaves
// this replica's state machine behind the others with nothing to notice it. On
// a follower there is no waiting proposer to receive the error at all, which is
// why the failure is also logged loudly rather than only returned.
func (r *Replica) applyEntry(entry raft.Entry) {
	var applyErr error
	if len(entry.Data) > 0 {
		var req pb.BatchRequest
		if err := proto.Unmarshal(entry.Data, &req); err != nil {
			// Apply-machinery failure: this replica cannot apply an entry its
			// peers will apply. That is divergence, so it is counted.
			r.applyErrs.Add(1)
			log.Printf("replica %d: APPLY FAILED (state machine may have diverged): unmarshal entry %d: %v",
				r.id, entry.Index, err)
			applyErr = err
		} else {
			// BatchHandler.Batch reports per-request failures in resp.Error and
			// returns a nil error, so checking only the error return misses
			// every request-level apply failure.
			resp, err := r.batch.Batch(context.Background(), &req)
			switch {
			case err != nil:
				// Engine/machinery failure — divergence.
				r.applyErrs.Add(1)
				log.Printf("replica %d: APPLY FAILED (state machine may have diverged): entry %d: %v",
					r.id, entry.Index, err)
				applyErr = err
			case resp != nil && resp.Error != nil:
				// Deterministic request-level failure. Every replica reaches the
				// same outcome, so this is a client error, not divergence: report
				// it to the proposer but do not count or log it as divergence.
				applyErr = &RequestError{Message: resp.Error.Message}
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
