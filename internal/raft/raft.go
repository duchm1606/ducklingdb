package raft

import (
	"math/rand"
)

const (
	electionTimeoutMin = 10
	electionTimeoutMax = 20
	heartbeatTimeout   = 3
	// snapshotCatchupGap is how far a follower may fall behind the log before
	// the leader prefers shipping a snapshot over streaming entries. A large
	// gap means a snapshot both catches the follower up in one step and, more
	// importantly, unblocks log truncation: CompactableIndex refuses to discard
	// entries a lagging follower still needs, so the log cannot shrink until
	// that follower is advanced. See sendAppend.
	snapshotCatchupGap = 8
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
	// recentActive tracks which peers we have heard from since the last
	// CheckQuorum sweep. Valid only when state == StateLeader; nil otherwise.
	recentActive map[uint64]bool

	// preVote runs an extra pre-election round (probe at term+1 without
	// adopting it) before campaigning for real, so a partitioned node that
	// keeps timing out cannot inflate its term and disrupt a healthy leader on
	// heal (Raft §9.6). checkQuorum lets a leader step down when it can no
	// longer reach a quorum, and lets followers refuse a challenger while they
	// still hear from a leader ("disruptive server" lease). Both default on in
	// NewRawNode; tests toggle them to exercise the counterfactual.
	preVote     bool
	checkQuorum bool

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
		preVote:         true,
		checkQuorum:     true,
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

// bcastPreVote sends MsgPreVote to all peers. The probe carries term+1 — the
// term we *would* campaign at — but we do NOT adopt it: rn.term is unchanged
// until a pre-vote quorum promotes us to a real candidate. That is the whole
// point of PreVote: a probe must not move anyone's term.
func (rn *RawNode) bcastPreVote() {
	for _, id := range rn.peers {
		if id == rn.id {
			continue
		}
		rn.send(Message{
			Type:    MsgPreVote,
			To:      id,
			Term:    rn.term + 1,
			Index:   rn.log.lastIndex(),
			LogTerm: rn.log.lastTerm(),
		})
	}
}

// bcastHeartbeat sends a heartbeat MsgHeartbeat to all peers.
//
// The commit index is clamped per-peer to min(matchIndex[peer], committed).
// Telling a follower to commit past what we know it has *matching* is a safety
// violation: a follower holding uncommitted entries from an older term at
// higher indices would commit its own divergent entries — the receiver only
// clamps against its own lastIndex and performs no log-match check. This
// mirrors etcd/raft, which sends min(pr.Match, r.raftLog.committed).
func (rn *RawNode) bcastHeartbeat() {
	for _, id := range rn.peers {
		if id == rn.id {
			continue
		}
		commit := rn.log.committed
		if m := rn.matchIndex[id]; m < commit {
			commit = m
		}
		rn.send(Message{
			Type:   MsgHeartbeat,
			To:     id,
			Term:   rn.term,
			Commit: commit,
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

// sendAppend sends a MsgApp to one peer with entries from nextIndex[peer], or a
// MsgSnap when log replication cannot (or should not) serve the follower:
//
//   - Compacted: the entries it needs were truncated (next < firstIndex). We
//     have no choice but to ship a snapshot.
//   - Far behind: it lags the log by more than snapshotCatchupGap. The entries
//     may still exist, but a snapshot catches it up in one step and unblocks
//     truncation — CompactableIndex will not discard entries this follower
//     still needs, so shipping it a snapshot is what lets the log shrink.
//
// Reading the persisted snapshot is deferred behind these two cheap gaps so a
// caught-up follower never pays for a snapshot load on the hot append path.
func (rn *RawNode) sendAppend(to uint64) {
	next := rn.nextIndex[to]
	firstIdx := rn.log.firstIndex()
	lastIdx := rn.log.lastIndex()

	compacted := next < firstIdx
	farBehind := lastIdx > rn.matchIndex[to]+snapshotCatchupGap
	if compacted || farBehind {
		snap, err := rn.log.storage.Snapshot()
		haveSnap := err == nil && !snap.IsEmpty()
		// Ship the snapshot when it actually advances this follower. When the
		// log was compacted we must; when merely far behind we do so only if a
		// snapshot exists that is ahead of the follower's match.
		if haveSnap && (compacted || snap.Metadata.Index > rn.matchIndex[to]) {
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
		if compacted {
			// The entries this follower needs were compacted away and we have
			// no usable snapshot. CompactableIndex never truncates past a
			// follower's match without a covering snapshot, so this is
			// unreachable in normal operation; do nothing (the follower is
			// retried next round) rather than send a MsgApp we cannot satisfy.
			return
		}
		// Far behind but no snapshot yet: the log still holds the entries, so
		// fall through and stream them via MsgApp.
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
	rn.recentActive = nil
	rn.electionElapsed = 0
	rn.electionTimeout = electionTimeoutMin + rand.Intn(electionTimeoutMax-electionTimeoutMin)
}

// becomePreCandidate transitions to PreCandidate state to run a pre-election.
// Unlike becomeCandidate it deliberately does NOT increment the term and does
// NOT record a durable vote (votedFor is untouched): the pre-election asks
// "would a quorum vote for me at term+1?" without committing to that term. Only
// winning the pre-election (campaign) promotes us to a real candidate.
func (rn *RawNode) becomePreCandidate() {
	rn.state = StatePreCandidate
	rn.leadID = none
	rn.votes = map[uint64]bool{rn.id: true}
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
	rn.electionElapsed = 0
	// Start the CheckQuorum window: we count ourselves active, and learn about
	// peers as their MsgAppResp / MsgHeartbeatResp arrive.
	rn.recentActive = map[uint64]bool{rn.id: true}

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

// hup starts a fresh campaign in response to an election timeout. With preVote
// enabled it begins with a pre-election; without it, it campaigns directly.
func (rn *RawNode) hup() {
	if rn.state == StateLeader {
		return
	}
	rn.campaign(rn.preVote)
}

// campaign transitions to (pre)candidate, records the self-vote, and either
// wins immediately (a single-node cluster reaches quorum on its own vote) or
// broadcasts the vote requests. Winning a pre-election recurses into the real
// election.
func (rn *RawNode) campaign(pre bool) {
	if pre {
		rn.becomePreCandidate()
	} else {
		rn.becomeCandidate()
	}
	if rn.tallyVotes() >= rn.quorum() {
		if pre {
			rn.campaign(false)
		} else {
			rn.becomeLeader()
		}
		return
	}
	if pre {
		rn.bcastPreVote()
	} else {
		rn.bcastRequestVote()
	}
}

// tallyVotes counts the votes (or pre-votes) granted so far this election.
func (rn *RawNode) tallyVotes() int {
	granted := 0
	for _, v := range rn.votes {
		if v {
			granted++
		}
	}
	return granted
}

// canVote reports whether we may grant m (a MsgVote or MsgPreVote), ignoring
// log freshness (checked separately) and the CheckQuorum lease (checked before
// term adoption). A pre-vote for a strictly higher term is always grantable —
// it is hypothetical and binds nothing — which is what lets a follower that has
// already voted this term still help a legitimately-ahead peer pre-elect.
func (rn *RawNode) canVote(m Message) bool {
	switch {
	case rn.votedFor == m.From:
		return true // a repeat of a vote we already cast
	case rn.votedFor == none && rn.leadID == none:
		return true // have not voted and know of no leader this term
	case m.Type == MsgPreVote && m.Term > rn.term:
		return true // pre-vote for a future term: non-binding
	default:
		return false
	}
}

// voteRespType returns the response type paired with a vote request type.
func voteRespType(t MessageType) MessageType {
	if t == MsgPreVote {
		return MsgPreVoteResp
	}
	return MsgVoteResp
}

// maybeStepDownOnQuorumLoss implements the leader side of CheckQuorum: once per
// election timeout, if we have not heard from a quorum of peers we assume we
// may be partitioned from the majority and step down. Stepping down is what
// lets the lease held by connected followers (see Step's term handling) expire,
// so a node on the majority side can win an election. The activity set is then
// reset so the next window measures fresh contact.
func (rn *RawNode) maybeStepDownOnQuorumLoss() {
	active := 0
	for _, id := range rn.peers {
		if id == rn.id || rn.recentActive[id] {
			active++
		}
	}
	rn.recentActive = map[uint64]bool{rn.id: true}
	if active < rn.quorum() {
		rn.becomeFollower(rn.term, none)
	}
}

// Tick advances the logical clock by one tick.
func (rn *RawNode) Tick() {
	switch rn.state {
	case StateFollower, StateCandidate, StatePreCandidate:
		rn.electionElapsed++
		if rn.electionElapsed >= rn.electionTimeout {
			rn.electionElapsed = 0
			rn.Step(Message{Type: MsgHup, From: rn.id, To: rn.id})
		}
	case StateLeader:
		if rn.checkQuorum {
			rn.electionElapsed++
			if rn.electionElapsed >= rn.electionTimeout {
				rn.electionElapsed = 0
				rn.maybeStepDownOnQuorumLoss()
			}
		}
		// maybeStepDownOnQuorumLoss may have demoted us to follower; only beat
		// while we are still the leader.
		if rn.state != StateLeader {
			return
		}
		rn.heartbeatElapsed++
		if rn.heartbeatElapsed >= heartbeatTimeout {
			rn.heartbeatElapsed = 0
			rn.Step(Message{Type: MsgBeat, From: rn.id, To: rn.id})
		}
	}
}

// Step processes a message and updates state.
func (rn *RawNode) Step(m Message) error {
	// Term handling. A higher term normally forces us to step down, but two
	// cases must NOT move our term:
	//
	//   - A vote/pre-vote challenger while CheckQuorum's lease holds (we still
	//     hear from a leader — electionElapsed < electionTimeout). Reacting is
	//     the disruption PreVote+CheckQuorum exist to prevent, so we ignore it
	//     entirely: no term change, no step down, no response. The challenger
	//     rejoins via the leader's next append, or after our lease expires.
	//   - A PreVote probe (it carries a term the prober has not itself adopted)
	//     and a *granted* PreVote response (it echoes our own probe term). The
	//     higher term is adopted only when a pre-election is actually won.
	if m.Term > rn.term {
		if (m.Type == MsgVote || m.Type == MsgPreVote) &&
			rn.checkQuorum && rn.leadID != none && rn.electionElapsed < rn.electionTimeout {
			return nil
		}
		switch {
		case m.Type == MsgPreVote:
			// Do not adopt a prober's speculative term.
		case m.Type == MsgPreVoteResp && !m.Reject:
			// Granted pre-vote at our probe term; adopt on quorum win, not here.
		default:
			lead := m.From
			if m.Type == MsgVote {
				lead = none
			}
			rn.becomeFollower(m.Term, lead)
		}
	}

	switch m.Type {
	case MsgHup:
		rn.hup()

	case MsgBeat:
		if rn.state == StateLeader {
			rn.bcastHeartbeat()
		}

	case MsgVote, MsgPreVote:
		// Grant when the request is not stale, we may vote for this peer, and
		// its log is at least as up-to-date as ours. A grant echoes the
		// request's term (for a pre-vote that is our probe term, one above
		// ours); a reject carries our own term so the requester learns it.
		canGrant := m.Term >= rn.term && rn.canVote(m) && rn.log.isUpToDate(m.Index, m.LogTerm)
		if canGrant {
			rn.send(Message{Type: voteRespType(m.Type), To: m.From, Term: m.Term})
			if m.Type == MsgVote {
				// Only a real vote is durable and resets the election timer. A
				// pre-vote is hypothetical: recording votedFor or resetting the
				// timer would let a persistent prober suppress our own campaign.
				// leadID is deliberately NOT set — a candidate we vote for may
				// still lose; leadID is learned from a leader's MsgApp /
				// MsgHeartbeat (see TestVoteGrantDoesNotSetLeader).
				rn.votedFor = m.From
				rn.electionElapsed = 0
			}
		} else {
			rn.send(Message{Type: voteRespType(m.Type), To: m.From, Term: rn.term, Reject: true})
		}

	case MsgVoteResp:
		if rn.state != StateCandidate {
			return nil
		}
		rn.votes[m.From] = !m.Reject
		if rn.tallyVotes() >= rn.quorum() {
			rn.becomeLeader()
		}

	case MsgPreVoteResp:
		if rn.state != StatePreCandidate {
			return nil
		}
		rn.votes[m.From] = !m.Reject
		if rn.tallyVotes() >= rn.quorum() {
			// The pre-election is won; now campaign for real (adopting term+1).
			rn.campaign(false)
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
		rn.recentActive[m.From] = true
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
			rn.recentActive[m.From] = true
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

// CompactableIndex returns the highest log index it is currently safe to
// truncate up to, given a snapshot exists at snapIndex.
//
// This is the truncation constraint. A follower (or a non-leader in general)
// may compact up to snapIndex: it has applied everything through it and does
// not serve entries to anyone. A leader must be more careful — if it truncates
// past a peer's matchIndex before that peer has been caught up, and the peer's
// snapshot transfer then fails, the peer is left with neither the log entries
// nor a snapshot to recover from. So the leader never truncates past the
// minimum peer matchIndex. Shipping a lagging peer a snapshot (see sendAppend)
// advances its matchIndex, which is what lets truncation proceed.
//
// The cost, stated plainly: a peer that is permanently down pins the log at its
// last matchIndex forever. With M4's fixed membership that is acceptable; the
// production answer (truncate on a quorum and track in-flight snapshots per
// peer) belongs with M5's membership/liveness work.
func (rn *RawNode) CompactableIndex(snapIndex uint64) uint64 {
	if rn.state != StateLeader {
		return snapIndex
	}
	safe := snapIndex
	for _, id := range rn.peers {
		if id == rn.id {
			continue
		}
		if m := rn.matchIndex[id]; m < safe {
			safe = m
		}
	}
	return safe
}

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
