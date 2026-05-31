package raft

import (
	"math/rand"
)

const (
	electionTimeoutMin = 10
	electionTimeoutMax = 20
	heartbeatTimeout   = 3
)

// none is used for "no vote / no leader".
const none uint64 = 0

// RawNode is a pure Raft state machine. No goroutines, no I/O, no timers.
// Drive it by calling Tick() every interval and Step(msg) on incoming messages.
// Collect pending work by calling Ready() after each Tick/Step.
type RawNode struct {
	id    uint64
	state StateType
	term  uint64

	// votedFor is the candidate this node voted for in current term.
	votedFor uint64
	// votes tracks votes received when we are a candidate.
	votes map[uint64]bool

	leadID uint64
	peers  []uint64

	// Per-leader state (valid only when state == StateLeader).
	nextIndex  map[uint64]uint64 // peer → next log index to send
	matchIndex map[uint64]uint64 // peer → highest log index known replicated

	log *RaftLog

	electionElapsed  int
	heartbeatElapsed int
	electionTimeout  int // randomized [min, max)

	msgs     []Message  // outbound; drained by Ready()
	prevSoft *SoftState // detect state changes for SoftState in Ready
	prevHard HardState  // detect changes for HardState in Ready
}

// NewRawNode constructs a RawNode. peers must include the node's own id.
func NewRawNode(id uint64, peers []uint64, storage LogStorage) *RawNode {
	rn := &RawNode{
		id:              id,
		state:           StateFollower,
		peers:           peers,
		log:             newRaftLog(storage),
		electionTimeout: electionTimeoutMin + rand.Intn(electionTimeoutMax-electionTimeoutMin),
	}
	hs, _ := storage.InitialState()
	if !IsEmptyHardState(hs) {
		rn.term = hs.Term
		rn.votedFor = hs.VotedFor
		rn.log.commitTo(hs.Commit)
		// Use the durably saved applied index, not committed. If a crash happened
		// between persisting the commit and applying entries to the state machine,
		// applied < committed and nextEntries() will surface the missing entries
		// for re-application on this startup.
		persisted, _ := storage.LoadApplied()
		if persisted > hs.Commit {
			persisted = hs.Commit // defensive: applied can never exceed committed
		}
		rn.log.appliedTo(persisted)
	}
	rn.prevHard = rn.hardState()
	rn.prevSoft = rn.softState()
	return rn
}

func (rn *RawNode) hardState() HardState {
	return HardState{Term: rn.term, VotedFor: rn.votedFor, Commit: rn.log.committed}
}

func (rn *RawNode) softState() *SoftState {
	return &SoftState{Lead: rn.leadID, RaftState: rn.state}
}

// quorum returns the minimum number of votes needed to win an election.
func (rn *RawNode) quorum() int { return len(rn.peers)/2 + 1 }

// send appends msg to the outbound queue (msg.From is always set to rn.id).
func (rn *RawNode) send(m Message) {
	m.From = rn.id
	rn.msgs = append(rn.msgs, m)
}

// bcastRequestVote sends MsgVote to all peers.
func (rn *RawNode) bcastRequestVote() {
	for _, id := range rn.peers {
		if id == rn.id {
			continue
		}
		rn.send(Message{
			Type:    MsgVote,
			To:      id,
			Term:    rn.term,
			Index:   rn.log.lastIndex(),
			LogTerm: rn.log.lastTerm(),
		})
	}
}

// bcastHeartbeat sends a heartbeat MsgHeartbeat to all peers.
func (rn *RawNode) bcastHeartbeat() {
	for _, id := range rn.peers {
		if id == rn.id {
			continue
		}
		rn.send(Message{
			Type:   MsgHeartbeat,
			To:     id,
			Term:   rn.term,
			Commit: rn.log.committed,
		})
	}
}

// bcastAppend sends MsgApp to all peers (log replication).
func (rn *RawNode) bcastAppend() {
	for _, id := range rn.peers {
		if id == rn.id {
			continue
		}
		rn.sendAppend(id)
	}
}

// sendAppend sends a MsgApp to one peer with entries from nextIndex[peer].
// When the follower is so far behind that we no longer have the entries (they
// were compacted by a snapshot), MsgSnap is sent instead.
func (rn *RawNode) sendAppend(to uint64) {
	next := rn.nextIndex[to]
	firstIdx := rn.log.firstIndex()
	if next < firstIdx {
		// Follower is below our log's start — we no longer have the entries
		// they'd need. Ship our most recent snapshot instead.
		snap, err := rn.log.storage.Snapshot()
		if err != nil || snap.IsEmpty() {
			// No snapshot available; nothing we can do this round. The
			// follower will keep failing until a snapshot is created.
			return
		}
		rn.send(Message{
			Type:     MsgSnap,
			To:       to,
			Term:     rn.term,
			Snapshot: snap,
		})
		// Optimistically advance nextIndex past the snapshot. If the follower
		// reports its actual last index via MsgAppResp, that wins.
		rn.nextIndex[to] = snap.Metadata.Index + 1
		return
	}
	prevIndex := next - 1
	prevTerm, err := rn.log.term(prevIndex)
	if err != nil {
		prevTerm = 0
	}
	entries := rn.log.entries(next, rn.log.lastIndex()+1)
	rn.send(Message{
		Type:    MsgApp,
		To:      to,
		Term:    rn.term,
		Index:   prevIndex,
		LogTerm: prevTerm,
		Entries: entries,
		Commit:  rn.log.committed,
	})
}

// becomeFollower transitions to Follower state.
func (rn *RawNode) becomeFollower(term, lead uint64) {
	rn.state = StateFollower
	rn.term = term
	rn.leadID = lead
	rn.votedFor = none
	rn.votes = nil
	rn.nextIndex = nil
	rn.matchIndex = nil
	rn.electionElapsed = 0
	rn.electionTimeout = electionTimeoutMin + rand.Intn(electionTimeoutMax-electionTimeoutMin)
}

// becomeCandidate transitions to Candidate state and votes for self.
func (rn *RawNode) becomeCandidate() {
	rn.state = StateCandidate
	rn.term++
	rn.votedFor = rn.id
	rn.leadID = none
	rn.votes = map[uint64]bool{rn.id: true}
	rn.electionElapsed = 0
	rn.electionTimeout = electionTimeoutMin + rand.Intn(electionTimeoutMax-electionTimeoutMin)
}

// becomeLeader transitions to Leader, initializes per-peer tracking, and
// appends a no-op entry to force commit of prior-term entries.
func (rn *RawNode) becomeLeader() {
	rn.state = StateLeader
	rn.leadID = rn.id
	rn.heartbeatElapsed = 0

	lastIdx := rn.log.lastIndex()
	rn.nextIndex = make(map[uint64]uint64, len(rn.peers))
	rn.matchIndex = make(map[uint64]uint64, len(rn.peers))
	for _, id := range rn.peers {
		rn.nextIndex[id] = lastIdx + 1
		rn.matchIndex[id] = 0
	}
	rn.matchIndex[rn.id] = lastIdx

	// Append no-op entry to commit previous-term entries.
	rn.log.append(Entry{Term: rn.term, Index: lastIdx + 1})
	rn.matchIndex[rn.id] = lastIdx + 1
	rn.maybeAdvanceCommit()
	rn.bcastAppend()
}

// Tick advances the logical clock by one tick.
func (rn *RawNode) Tick() {
	switch rn.state {
	case StateFollower, StateCandidate:
		rn.electionElapsed++
		if rn.electionElapsed >= rn.electionTimeout {
			rn.electionElapsed = 0
			rn.Step(Message{Type: MsgHup, From: rn.id, To: rn.id})
		}
	case StateLeader:
		rn.heartbeatElapsed++
		if rn.heartbeatElapsed >= heartbeatTimeout {
			rn.heartbeatElapsed = 0
			rn.Step(Message{Type: MsgBeat, From: rn.id, To: rn.id})
		}
	}
}

// Step processes a message and updates state.
func (rn *RawNode) Step(m Message) error {
	// Term check: step down on higher term.
	if m.Term > rn.term {
		lead := m.From
		if m.Type == MsgVote {
			lead = none
		}
		rn.becomeFollower(m.Term, lead)
	}

	switch m.Type {
	case MsgHup:
		rn.becomeCandidate()
		// Single-node fast-path.
		if len(rn.peers) == 1 {
			rn.becomeLeader()
			return nil
		}
		rn.bcastRequestVote()

	case MsgBeat:
		if rn.state == StateLeader {
			rn.bcastHeartbeat()
		}

	case MsgVote:
		canGrant := rn.votedFor == none || rn.votedFor == m.From
		logOK := rn.log.isUpToDate(m.Index, m.LogTerm)
		if m.Term >= rn.term && canGrant && logOK {
			rn.votedFor = m.From
			rn.leadID = m.From // optimistic: remember who we voted for as likely leader
			rn.electionElapsed = 0
			rn.send(Message{Type: MsgVoteResp, To: m.From, Term: rn.term})
		} else {
			rn.send(Message{Type: MsgVoteResp, To: m.From, Term: rn.term, Reject: true})
		}

	case MsgVoteResp:
		if rn.state != StateCandidate {
			return nil
		}
		rn.votes[m.From] = !m.Reject
		granted := 0
		for _, v := range rn.votes {
			if v {
				granted++
			}
		}
		if granted >= rn.quorum() {
			rn.becomeLeader()
		}

	case MsgProp:
		if rn.state != StateLeader {
			return nil // ignore proposals on non-leaders
		}
		lastIdx := rn.log.lastIndex()
		entries := make([]Entry, len(m.Entries))
		for i, e := range m.Entries {
			entries[i] = Entry{Term: rn.term, Index: lastIdx + uint64(i) + 1, Data: e.Data}
		}
		rn.log.append(entries...)
		rn.matchIndex[rn.id] = rn.log.lastIndex()
		rn.bcastAppend()
		rn.maybeAdvanceCommit()

	case MsgApp:
		if m.Term < rn.term {
			rn.send(Message{Type: MsgAppResp, To: m.From, Term: rn.term, Reject: true,
				RejectHint: rn.log.lastIndex()})
			return nil
		}
		// A valid AppendEntries from a leader establishes authority: any Candidate
		// must step down (Raft §5.2 — candidate reverts when it sees a leader with
		// term >= its own).
		rn.becomeFollower(m.Term, m.From)
		// Check prevLog consistency
		if m.Index > 0 {
			t, err := rn.log.term(m.Index)
			if err != nil || t != m.LogTerm {
				rn.send(Message{Type: MsgAppResp, To: m.From, Term: rn.term,
					Reject: true, RejectHint: rn.log.lastIndex()})
				return nil
			}
		}
		// Append entries, truncating conflicts
		if len(m.Entries) > 0 {
			rn.log.append(m.Entries...)
		}
		// Advance commit with upper-bound guard
		if m.Commit > rn.log.committed {
			rn.log.commitTo(min64(m.Commit, rn.log.lastIndex()))
		}
		rn.send(Message{Type: MsgAppResp, To: m.From, Term: rn.term,
			Index: rn.log.lastIndex()})

	case MsgAppResp:
		if rn.state != StateLeader {
			return nil
		}
		if m.Reject {
			// Back up nextIndex using the hint
			if m.RejectHint < rn.nextIndex[m.From] {
				rn.nextIndex[m.From] = m.RejectHint + 1
			} else {
				if rn.nextIndex[m.From] > 1 {
					rn.nextIndex[m.From]--
				}
			}
			rn.sendAppend(m.From)
		} else {
			if m.Index > rn.matchIndex[m.From] {
				rn.matchIndex[m.From] = m.Index
				rn.nextIndex[m.From] = m.Index + 1
			}
			rn.maybeAdvanceCommit()
		}

	case MsgHeartbeat:
		if m.Term >= rn.term {
			rn.electionElapsed = 0
			rn.leadID = m.From
			rn.log.commitTo(min64(m.Commit, rn.log.lastIndex()))
		}
		rn.send(Message{Type: MsgHeartbeatResp, To: m.From, Term: rn.term})

	case MsgHeartbeatResp:
		if rn.state == StateLeader {
			// Send append if follower is behind.
			if rn.matchIndex[m.From] < rn.log.lastIndex() {
				rn.sendAppend(m.From)
			}
		}

	case MsgSnap:
		// A leader has shipped us a snapshot because our log is too far behind.
		// Step down (we're a follower now), restore the snapshot into the log,
		// and ack with our new last index so the leader can resume MsgApp from
		// snap.Index + 1.
		if m.Term < rn.term {
			rn.send(Message{Type: MsgAppResp, To: m.From, Term: rn.term, Reject: true,
				RejectHint: rn.log.lastIndex()})
			return nil
		}
		rn.becomeFollower(m.Term, m.From)
		if rn.log.restore(m.Snapshot) {
			rn.send(Message{Type: MsgAppResp, To: m.From, Term: rn.term,
				Index: rn.log.lastIndex()})
		} else {
			// We already have entries past this snapshot — tell the leader
			// where we are so it can ship MsgApp from there.
			rn.send(Message{Type: MsgAppResp, To: m.From, Term: rn.term,
				Index: rn.log.committed})
		}
	}
	return nil
}

// Propose proposes a new command to be appended to the log (leader only).
func (rn *RawNode) Propose(data []byte) error {
	return rn.Step(Message{
		Type:    MsgProp,
		From:    rn.id,
		To:      rn.id,
		Entries: []Entry{{Data: data}},
	})
}

// Lead returns the current known leader ID (0 = unknown).
func (rn *RawNode) Lead() uint64 { return rn.leadID }

// Applied returns the highest log index that has been applied to the state
// machine. Used by snapshot creation to know what to capture.
func (rn *RawNode) Applied() uint64 { return rn.log.applied }

// LogTerm returns the term of the entry at index. Used by snapshot creation
// to anchor a snapshot at (appliedIndex, term-of-that-entry).
func (rn *RawNode) LogTerm(index uint64) (uint64, error) { return rn.log.term(index) }

// HasReady returns true if there is pending work to process.
func (rn *RawNode) HasReady() bool {
	hs := rn.hardState()
	if hs != rn.prevHard {
		return true
	}
	if rn.log.hasPendingSnapshot() {
		return true
	}
	if len(rn.log.unstableEntries()) > 0 {
		return true
	}
	if len(rn.log.nextEntries()) > 0 {
		return true
	}
	if len(rn.msgs) > 0 {
		return true
	}
	ss := rn.softState()
	if ss.Lead != rn.prevSoft.Lead || ss.RaftState != rn.prevSoft.RaftState {
		return true
	}
	return false
}

// Ready collects all pending work. Must call Advance(rd) after processing.
func (rn *RawNode) Ready() Ready {
	rd := Ready{}

	ss := rn.softState()
	if ss.Lead != rn.prevSoft.Lead || ss.RaftState != rn.prevSoft.RaftState {
		rd.SoftState = ss
	}

	hs := rn.hardState()
	if hs != rn.prevHard {
		rd.HardState = hs
	}

	if rn.log.hasPendingSnapshot() {
		rd.Snapshot = rn.log.pendingSnapshot
	}
	rd.Entries = rn.log.unstableEntries()
	rd.Messages = rn.msgs
	rd.CommittedEntries = rn.log.nextEntries()

	return rd
}

// Advance signals that the caller has processed the Ready.
func (rn *RawNode) Advance(rd Ready) {
	if rd.SoftState != nil {
		rn.prevSoft = rd.SoftState
	}
	if !IsEmptyHardState(rd.HardState) {
		rn.prevHard = rd.HardState
	}
	if !rd.Snapshot.IsEmpty() {
		// Caller has applied the snapshot to the state machine and persisted
		// it via SaveSnapshot. Clear it so subsequent Ready calls don't
		// re-emit the same snapshot.
		rn.log.pendingSnapshot = Snapshot{}
	}
	if len(rd.Entries) > 0 {
		last := rd.Entries[len(rd.Entries)-1]
		rn.log.stableTo(last.Index, last.Term)
	}
	if len(rd.CommittedEntries) > 0 {
		last := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		rn.log.appliedTo(last.Index)
	}
	rn.msgs = nil
}

func min64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// maybeAdvanceCommit tries to advance the commit index based on matchIndex quorum.
// Only valid when state == StateLeader.
func (rn *RawNode) maybeAdvanceCommit() {
	// Find the highest index replicated on a quorum.
	last := rn.log.lastIndex()
	for n := last; n > rn.log.committed; n-- {
		t, err := rn.log.term(n)
		if err != nil {
			break
		}
		if t != rn.term {
			// Only commit entries from the current term (Raft §5.4.2).
			break
		}
		count := 0
		for _, idx := range rn.matchIndex {
			if idx >= n {
				count++
			}
		}
		if count >= rn.quorum() {
			rn.log.commitTo(n)
			break
		}
	}
}
