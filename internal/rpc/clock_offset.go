package rpc

import (
	"context"
	"sync"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

// RemoteOffset is a single clock offset measurement between this node and
// a remote node, captured at the end of a Heartbeat round-trip.
type RemoteOffset struct {
	// Offset is the estimated difference (remote clock - local clock).
	// Positive means the remote clock is ahead.
	Offset time.Duration
	// Uncertainty is half the round-trip latency — the measurement error bound.
	Uncertainty time.Duration
	MeasuredAt  time.Time
}

// RemoteClockMonitor tracks the last measured clock offset for every peer
// that this node has exchanged heartbeats with.
type RemoteClockMonitor struct {
	mu        sync.Mutex
	offsets   map[int32]RemoteOffset
	maxOffset time.Duration
}

func NewRemoteClockMonitor(maxOffset time.Duration) *RemoteClockMonitor {
	return &RemoteClockMonitor{
		offsets:   make(map[int32]RemoteOffset),
		maxOffset: maxOffset,
	}
}

func (m *RemoteClockMonitor) UpdateOffset(nodeID int32, offset RemoteOffset) {
	m.mu.Lock()
	m.offsets[nodeID] = offset
	m.mu.Unlock()
}

func (m *RemoteClockMonitor) GetOffset(nodeID int32) (RemoteOffset, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.offsets[nodeID]
	return o, ok
}

// IsHealthy returns true if the measured offset for nodeID is within maxOffset.
// Returns true for unknown nodes (no measurement yet).
func (m *RemoteClockMonitor) IsHealthy(nodeID int32) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.offsets[nodeID]
	if !ok {
		return true
	}
	d := o.Offset
	if d < 0 {
		d = -d
	}
	return d <= m.maxOffset
}

// MeasureOffsetFromClient calls Heartbeat on client, measures RTT, and returns
// (remoteNodeID, RemoteOffset, error).
//
// Offset formula (NTP-style):
//
//	offset = serverTime - (tBefore + rtt/2)
//
// Positive offset means the remote clock is ahead of ours.
func MeasureOffsetFromClient(ctx context.Context, client pb.InternalClient, clock *hlc.Clock, localNodeID int32) (int32, RemoteOffset, error) {
	tBefore := time.Now()
	ts := clock.Now()

	resp, err := client.Heartbeat(ctx, &pb.PingRequest{
		NodeId:     localNodeID,
		ServerTime: pb.FromHLC(ts),
	})
	if err != nil {
		return 0, RemoteOffset{}, err
	}

	tAfter := time.Now()
	rtt := tAfter.Sub(tBefore)

	// Convert server's reported wall time to a time.Time for comparison.
	serverWall := time.Unix(0, resp.ServerTime.WallTime)
	localMid := tBefore.Add(rtt / 2)
	offset := serverWall.Sub(localMid)

	return resp.NodeId, RemoteOffset{
		Offset:      offset,
		Uncertainty: rtt / 2,
		MeasuredAt:  tAfter,
	}, nil
}
