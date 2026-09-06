package raft

// StateType is the role of a RawNode.
type StateType uint8

const (
	StateFollower  StateType = iota
	StateCandidate StateType = iota
	StateLeader    StateType = iota
	// StatePreCandidate is a node running a PreVote election: it is probing
	// whether it *could* win at term+1 without having adopted that term. It
	// never appears on the wire (SoftState is internal to the Ready loop), so
	// its ordinal is free to change.
	StatePreCandidate StateType = iota
)

// MessageType identifies a Raft message.
type MessageType uint8

const (
	MsgHup           MessageType = iota // internal: trigger election
	MsgBeat                             // internal: trigger heartbeat
	MsgVote                             // RequestVote RPC
	MsgVoteResp                         // RequestVote response
	MsgApp                              // AppendEntries RPC
	MsgAppResp                          // AppendEntries response
	MsgHeartbeat                        // leader heartbeat
	MsgHeartbeatResp                    // heartbeat response
	MsgProp                             // internal: client proposal
	MsgSnap                             // InstallSnapshot RPC
	// MsgPreVote and MsgPreVoteResp MUST stay at the end of this block. The
	// gRPC transport ships MessageType as a raw uint32 (raft_convert.go casts
	// both directions), so these ordinals are the wire contract — inserting a
	// value earlier would renumber MsgSnap et al. and break replicas that
	// disagree on the numbering.
	MsgPreVote     // PreVote RPC: probe at term+1 without adopting the term
	MsgPreVoteResp // PreVote response
)

// SnapshotMetadata describes a snapshot's position in the log.
// Index is the last log entry covered by the snapshot; Term is that entry's term.
// After installing a snapshot at (Index, Term), the receiver behaves as if it
// had committed and applied entries 1..Index, with the snapshot's Data as the
// resulting state machine state.
type SnapshotMetadata struct {
	Index uint64
	Term  uint64
}

// Snapshot bundles a state-machine snapshot with its metadata.
// Data is the serialized state-machine bytes; opaque to Raft.
type Snapshot struct {
	Metadata SnapshotMetadata
	Data     []byte
}

// IsEmpty reports whether s carries no information (zero-value snapshot).
func (s Snapshot) IsEmpty() bool {
	return s.Metadata.Index == 0
}

// Entry is one item in the Raft log.
type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte // nil = no-op (leader election entry)
}

// Message is a Raft protocol message exchanged between nodes.
type Message struct {
	Type       MessageType
	To         uint64
	From       uint64
	Term       uint64
	LogTerm    uint64 // term of prevLogIndex entry (MsgApp)
	Index      uint64 // prevLogIndex (MsgApp) or matchIndex (MsgAppResp)
	Entries    []Entry
	Commit     uint64 // leaderCommit
	Reject     bool
	RejectHint uint64   // follower's last log index (fast backtrack)
	Snapshot   Snapshot // populated for MsgSnap
}

// HardState is durable Raft state that must be persisted before responding to RPCs.
type HardState struct {
	Term     uint64
	VotedFor uint64
	Commit   uint64
}

// SoftState is volatile state; no persistence needed.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready bundles all pending I/O work produced by one Tick or Step.
// The caller must:
//  1. Persist HardState (if non-empty)
//  2. If Snapshot is non-empty, apply it to the state machine, persist via
//     SaveSnapshot, and compact the log up to Snapshot.Metadata.Index.
//  3. Persist Entries (before sending Messages)
//  4. Send Messages to peers
//  5. Apply CommittedEntries to the state machine
//  6. Call Advance(rd)
//
// Snapshot and CommittedEntries are mutually exclusive in a single Ready —
// a snapshot install supersedes any pending committed entries below its index.
type Ready struct {
	SoftState        *SoftState // non-nil only on state/lead change
	HardState        HardState
	Snapshot         Snapshot // non-empty when the caller must install a snapshot
	Entries          []Entry  // unstable entries to persist
	Messages         []Message
	CommittedEntries []Entry
}

// IsEmptyHardState returns true when hs carries no information.
func IsEmptyHardState(hs HardState) bool {
	return hs.Term == 0 && hs.VotedFor == 0 && hs.Commit == 0
}
