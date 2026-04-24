package raft

// StateType is the role of a RawNode.
type StateType uint8

const (
	StateFollower  StateType = iota
	StateCandidate StateType = iota
	StateLeader    StateType = iota
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
)

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
	RejectHint uint64 // follower's last log index (fast backtrack)
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
//  2. Persist Entries (before sending Messages)
//  3. Send Messages to peers
//  4. Apply CommittedEntries to the state machine
//  5. Call Advance(rd)
type Ready struct {
	SoftState        *SoftState // non-nil only on state/lead change
	HardState        HardState
	Entries          []Entry // unstable entries to persist
	Messages         []Message
	CommittedEntries []Entry
}

// IsEmptyHardState returns true when hs carries no information.
func IsEmptyHardState(hs HardState) bool {
	return hs.Term == 0 && hs.VotedFor == 0 && hs.Commit == 0
}
