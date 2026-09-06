package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/duchm1606/ducklingdb/internal/gossip"
	"github.com/duchm1606/ducklingdb/internal/kv/kvserver"
	"github.com/duchm1606/ducklingdb/internal/kv/kvserver/liveness"
	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/raft"
	"github.com/duchm1606/ducklingdb/internal/rpc"
	"github.com/duchm1606/ducklingdb/internal/sql/executor"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

const (
	defaultHeartbeatWorkerInterval = 1 * time.Second
	defaultPeerResolveTimeout      = 10 * time.Second
)

type NodeID int32

type NodeConfig struct {
	Addr              string
	DataDir           string
	JoinAddrs         []string
	Peers             []string
	MaxOffset         time.Duration
	NodeID            NodeID
	LivenessThreshold time.Duration // 0 → default (5s)
	HeartbeatInterval time.Duration // 0 → default (1s)
}

type Node struct {
	id           NodeID
	clusterID    ClusterID
	desc         *pb.NodeDescriptor
	engine       storage.Engine
	clock        *hlc.Clock
	rpcServer    *rpc.Server
	rpcContext   *rpc.Context
	gossip       *gossip.Gossip
	liveness     *liveness.NodeLiveness
	clockMonitor *rpc.RemoteClockMonitor
	stopper      *Stopper

	heartbeatInterval time.Duration

	replica      *kvserver.Replica
	replicaMu    sync.RWMutex
	addrToNodeID map[string]uint64
	addrMu       sync.RWMutex
	peerAddrs    []string

	// peerResolveTimeout bounds one attempt at resolving peer NodeIDs from
	// gossip. Overridable in tests so the retry loop can be exercised quickly.
	peerResolveTimeout time.Duration
}

func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.MaxOffset == 0 {
		cfg.MaxOffset = 500 * time.Millisecond
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = defaultHeartbeatWorkerInterval
	}

	engine, err := lsm.OpenLSM(lsm.LSMOptions{Dir: cfg.DataDir})
	if err != nil {
		return nil, fmt.Errorf("open engine: %w", err)
	}

	clock := hlc.NewClock(hlc.SystemWallClock(), cfg.MaxOffset)

	srv, err := rpc.NewServer(cfg.Addr)
	if err != nil {
		engine.Close()
		return nil, fmt.Errorf("create rpc server: %w", err)
	}

	rpcCtx := rpc.NewContext()

	var nodeID NodeID
	var clusterID ClusterID
	if cfg.NodeID != 0 {
		nodeID = cfg.NodeID
	} else {
		state, err := InitNode(engine, rpcCtx, cfg.JoinAddrs)
		if err != nil {
			srv.Stop()
			engine.Close()
			return nil, fmt.Errorf("init node: %w", err)
		}
		nodeID = state.NodeID
		clusterID = state.ClusterID
	}

	nodeAddr := normalizeAddr(cfg.Addr)
	desc := &pb.NodeDescriptor{
		NodeId:  int32(nodeID),
		Address: nodeAddr,
	}

	stopper := NewStopper()

	g := gossip.New(int32(nodeID), srv.Addr(), rpcCtx, stopper)
	for _, seed := range cfg.JoinAddrs {
		g.AddPeer(seed)
	}

	clockMonitor := rpc.NewRemoteClockMonitor(cfg.MaxOffset)

	nl := liveness.New(int32(nodeID), engine, clock, g, stopper)
	if cfg.LivenessThreshold > 0 {
		nl.SetThreshold(cfg.LivenessThreshold)
	}

	n := &Node{
		id:                 nodeID,
		clusterID:          clusterID,
		desc:               desc,
		engine:             engine,
		clock:              clock,
		rpcServer:          srv,
		rpcContext:         rpcCtx,
		gossip:             g,
		liveness:           nl,
		clockMonitor:       clockMonitor,
		stopper:            stopper,
		heartbeatInterval:  cfg.HeartbeatInterval,
		addrToNodeID:       make(map[string]uint64),
		peerAddrs:          cfg.Peers,
		peerResolveTimeout: defaultPeerResolveTimeout,
	}

	// Seed own address into the map.
	n.addrMu.Lock()
	n.addrToNodeID[nodeAddr] = uint64(nodeID)
	n.addrMu.Unlock()

	// Register gossip callback to populate addrToNodeID when node descriptors arrive.
	g.RegisterCallback(gossip.KeyNodeDescPrefix, func(key string, val []byte) {
		var nd pb.NodeDescriptor
		if err := proto.Unmarshal(val, &nd); err != nil {
			return
		}
		n.addrMu.Lock()
		n.addrToNodeID[nd.Address] = uint64(nd.NodeId)
		n.addrMu.Unlock()
	})

	allocator := newNodeIDAllocator(engine, clusterID)
	svc := &nodeServer{
		heartbeat: rpc.NewHeartbeatService(clock, int32(nodeID)),
		batch:     kvserver.NewBatchHandler(engine, clock),
		allocator: allocator,
		clusterID: clusterID,
		node:      n,
	}
	pb.RegisterInternalServer(srv.GRPCServer(), svc)
	pb.RegisterGossipServiceServer(srv.GRPCServer(), g)

	raftSvc := &raftServiceServer{node: n}
	pb.RegisterRaftServiceServer(srv.GRPCServer(), raftSvc)

	return n, nil
}

func (n *Node) Start() {
	n.rpcServer.Start()
	n.gossip.Start()
	n.liveness.Start()
	n.startHeartbeatWorker()

	// Gossip own descriptor so peers can resolve our NodeID.
	descBytes, _ := proto.Marshal(n.desc)
	n.gossip.AddInfo(gossip.MakeNodeDescKey(int32(n.id)), descBytes, 0)

	n.startRaftReplica()
}

// startRaftReplica brings up this node's Replica.
//
// A node always gets a Replica now, including a single-node cluster, which
// becomes a one-peer Raft group. There is no "no peers means no consensus"
// mode: previously a node started with no peers wrote straight to local MVCC,
// so whether a write was replicated depended on how the process was launched.
func (n *Node) startRaftReplica() {
	// No peers to discover — build the group synchronously so the node is
	// never briefly writable without Raft.
	if len(n.peerAddrs) == 0 {
		n.initReplica([]uint64{uint64(n.id)})
		return
	}
	go func() {
		// Retry with backoff rather than giving up. Returning after one failed
		// attempt left getReplica() nil forever, so a node that merely started
		// before its peers became permanently unable to accept writes —
		// "never ready" instead of "not ready yet".
		backoff := 500 * time.Millisecond
		const maxBackoff = 10 * time.Second
		for {
			peerIDs, err := n.waitForPeerIDs(n.peerResolveTimeout)
			if err == nil {
				n.initReplica(peerIDs)
				return
			}
			log.Printf("[raft] could not resolve peer NodeIDs (retrying in %s): %v", backoff, err)
			select {
			case <-time.After(backoff):
			case <-n.stopper.ShouldStop():
				return
			}
			if backoff < maxBackoff {
				backoff *= 2
			}
		}
	}()
}

func (n *Node) initReplica(peerIDs []uint64) {
	log.Printf("[raft] starting replica id=%d peers=%v", n.id, peerIDs)
	storage := raft.NewLSMLogStorage(n.engine)
	rn := raft.NewRawNode(uint64(n.id), peerIDs, storage)
	bh := kvserver.NewBatchHandler(n.engine, n.clock)
	r := kvserver.NewReplica(uint64(n.id), rn, storage, bh, func(msgs []raft.Message) {
		n.sendRaftMessages(msgs)
	})
	n.replicaMu.Lock()
	n.replica = r
	n.replicaMu.Unlock()
	r.Start()
	log.Printf("[raft] replica started")
}

// WaitForLeader blocks until this node's Raft group has a leader, or the
// timeout expires. Writes are rejected until then, so tests and clients that
// write immediately after Start need this.
func (n *Node) WaitForLeader(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if r := n.getReplica(); r != nil && r.Lead() != 0 {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

func (n *Node) waitForPeerIDs(timeout time.Duration) ([]uint64, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		n.addrMu.RLock()
		found := make([]uint64, 0, len(n.peerAddrs))
		allFound := true
		for _, addr := range n.peerAddrs {
			id, ok := n.addrToNodeID[addr]
			if !ok {
				allFound = false
				break
			}
			found = append(found, id)
		}
		n.addrMu.RUnlock()
		if allFound {
			return found, nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return nil, fmt.Errorf("timeout waiting for peer NodeIDs")
}

func (n *Node) sendRaftMessages(msgs []raft.Message) {
	for _, m := range msgs {
		m := m
		go func() {
			var targetAddr string
			n.addrMu.RLock()
			for addr, id := range n.addrToNodeID {
				if id == m.To {
					targetAddr = addr
					break
				}
			}
			n.addrMu.RUnlock()
			if targetAddr == "" {
				return
			}
			conn, err := n.rpcContext.GRPCDialNode(targetAddr)
			if err != nil {
				return
			}
			client := pb.NewRaftServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			// A failed send used to be discarded. For most message types Raft
			// retries on the next tick, so a dropped heartbeat/append is
			// self-healing — but a dropped MsgSnap silently strands the target
			// (its log may be compacted past what MsgApp can serve), so surface
			// every failure and call out MsgSnap specifically. Delivery is still
			// retried by the leader's next sendAppend, which does not advance
			// matchIndex until the follower acks.
			if _, err := client.Step(ctx, raftMessageToProto(m)); err != nil {
				if m.Type == raft.MsgSnap {
					log.Printf("[raft] node %d: MsgSnap to %d failed: %v (follower will be retried)",
						n.id, m.To, err)
				} else {
					log.Printf("[raft] node %d: Step %d to %d failed: %v",
						n.id, m.Type, m.To, err)
				}
			}
		}()
	}
}

// startHeartbeatWorker periodically pings all gossip peers, measures clock
// offsets, and updates the RemoteClockMonitor. It logs (but does not crash)
// when an offset exceeds maxOffset.
func (n *Node) startHeartbeatWorker() {
	n.stopper.RunWorker(func() {
		ticker := time.NewTicker(n.heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-n.stopper.ShouldStop():
				return
			case <-ticker.C:
				n.measurePeerOffsets()
			}
		}
	})
}

func (n *Node) measurePeerOffsets() {
	for _, addr := range n.gossip.Peers() {
		conn, err := n.rpcContext.GRPCDialNode(addr)
		if err != nil {
			continue
		}
		client := pb.NewInternalClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		remoteID, offset, err := rpc.MeasureOffsetFromClient(ctx, client, n.clock, int32(n.id))
		cancel()
		if err != nil {
			continue
		}
		n.clockMonitor.UpdateOffset(remoteID, offset)
	}
}

func (n *Node) Stop() {
	// Stop the Raft Ready loop before closing the engine it writes to.
	// Otherwise run() keeps ticking every 10ms and calls SaveHardState /
	// AppendEntries against a closed WAL — and in tests, against a directory
	// the framework is about to delete.
	if r := n.getReplica(); r != nil {
		r.Stop()
	}
	n.stopper.Stop()
	n.rpcServer.Stop()
	n.rpcContext.Close()
	n.engine.Close()
}

func (n *Node) getReplica() *kvserver.Replica {
	n.replicaMu.RLock()
	defer n.replicaMu.RUnlock()
	return n.replica
}

// normalizeAddr converts ":port" to "127.0.0.1:port" to match the peer
// address format used by the --peers CLI flag.
func normalizeAddr(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}

func (n *Node) NodeID() NodeID                        { return n.id }
func (n *Node) ClusterID() ClusterID                  { return n.clusterID }
func (n *Node) Descriptor() *pb.NodeDescriptor        { return n.desc }
func (n *Node) Engine() storage.Engine                { return n.engine }
func (n *Node) Clock() *hlc.Clock                     { return n.clock }
func (n *Node) RPCAddr() string                       { return n.rpcServer.Addr() }
func (n *Node) RPCContext() *rpc.Context              { return n.rpcContext }
func (n *Node) Gossip() *gossip.Gossip                { return n.gossip }
func (n *Node) Liveness() *liveness.NodeLiveness      { return n.liveness }
func (n *Node) ClockMonitor() *rpc.RemoteClockMonitor { return n.clockMonitor }
func (n *Node) Stopper() *Stopper                     { return n.stopper }

type nodeServer struct {
	pb.UnimplementedInternalServer
	heartbeat *rpc.HeartbeatService
	batch     *kvserver.BatchHandler
	allocator *NodeIDAllocator
	clusterID ClusterID
	node      *Node
}

func (s *nodeServer) Heartbeat(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.heartbeat.Heartbeat(ctx, req)
}

// errReplicaNotReady is returned while the Raft group is still forming. It is
// deliberately an error rather than a fall-back to a local write: a silent
// fall-back is how unreplicated data used to get accepted during the first
// seconds of a cluster's life, only to be overwritten by the Raft log later.
var errReplicaNotReady = errors.New("raft replica not ready on this node")

// errMixedBatch rejects a batch containing both reads and writes.
//
// Such a batch would take the Raft path, and the response is rebuilt from the
// request shape — so a Get inside it would come back as an empty PutResponse
// and the read result would be silently lost. Refusing is better than returning
// a wrong-shaped answer. (Supporting them properly means returning the real
// BatchResponse produced on the apply path back to the waiting proposer.)
var errMixedBatch = errors.New("batch mixes reads and writes; send them separately")

// batchKind classifies a batch. Read-only batches are served from the local
// engine; anything that mutates state must go through Raft.
func batchKind(req *pb.BatchRequest) (hasRead, hasWrite bool) {
	for _, ru := range req.Requests {
		switch ru.Value.(type) {
		case *pb.RequestUnion_Get, *pb.RequestUnion_Scan:
			hasRead = true
		default:
			hasWrite = true
		}
	}
	return hasRead, hasWrite
}

// writeBatchResponse builds the response for a batch that was applied through
// Raft. Put and Delete carry no payload, so the response mirrors the request's
// shape.
func writeBatchResponse(req *pb.BatchRequest) *pb.BatchResponse {
	resp := &pb.BatchResponse{Responses: make([]*pb.ResponseUnion, 0, len(req.Requests))}
	for _, ru := range req.Requests {
		switch ru.Value.(type) {
		case *pb.RequestUnion_Delete:
			resp.Responses = append(resp.Responses, &pb.ResponseUnion{
				Value: &pb.ResponseUnion_Delete{Delete: &pb.DeleteResponse{}}})
		default:
			resp.Responses = append(resp.Responses, &pb.ResponseUnion{
				Value: &pb.ResponseUnion_Put{Put: &pb.PutResponse{}}})
		}
	}
	return resp
}

// Batch serves the public KV API.
//
// Writes are proposed to Raft and only acknowledged once committed and applied;
// they are no longer executed directly against local MVCC. Reads are still
// served from the local engine — gating reads on leadership is a separate piece
// of work, so a follower can still return a stale read here.
func (s *nodeServer) Batch(ctx context.Context, req *pb.BatchRequest) (*pb.BatchResponse, error) {
	hasRead, hasWrite := batchKind(req)
	if hasRead && hasWrite {
		return nil, errMixedBatch
	}
	if !hasWrite {
		return s.batch.Batch(ctx, req)
	}

	r := s.node.getReplica()
	if r == nil {
		return nil, errReplicaNotReady
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}
	if err := r.Propose(ctx, data); err != nil {
		// A deterministic request failure is a client-level error, not a
		// transport failure: keep returning it in-band as resp.Error, the way
		// the direct-MVCC path used to, so existing callers still work.
		var reqErr *kvserver.RequestError
		if errors.As(err, &reqErr) {
			return &pb.BatchResponse{Error: &pb.Error{Message: reqErr.Message}}, nil
		}
		return nil, err
	}
	return writeBatchResponse(req), nil
}

func (s *nodeServer) AllocateNodeID(_ context.Context, _ *pb.AllocateNodeIDRequest) (*pb.AllocateNodeIDResponse, error) {
	id, err := s.allocator.Allocate()
	if err != nil {
		return nil, err
	}
	return &pb.AllocateNodeIDResponse{
		NodeId:    int32(id),
		ClusterId: s.clusterID[:],
	}, nil
}

func (s *nodeServer) ExecSQL(ctx context.Context, req *pb.SQLRequest) (*pb.SQLResponse, error) {
	r := s.node.getReplica()
	if r == nil {
		// No silent fall-back to a direct engine write: that accepted
		// unreplicated data while the Raft group was still forming.
		return &pb.SQLResponse{Error: errReplicaNotReady.Error()}, nil
	}
	sender := func(sctx context.Context, batch *pb.BatchRequest) (*pb.BatchResponse, error) {
		if r.Lead() != uint64(s.node.NodeID()) {
			return nil, fmt.Errorf("not the leader (leader is node %d)", r.Lead())
		}
		data, err := proto.Marshal(batch)
		if err != nil {
			return nil, err
		}
		if err := r.Propose(sctx, data); err != nil {
			return nil, err
		}
		return &pb.BatchResponse{}, nil
	}
	exec := executor.NewWithSender(s.node.engine, s.node.clock, sender)

	result, err := exec.Execute(req.Sql)
	if err != nil {
		return &pb.SQLResponse{Error: err.Error()}, nil
	}
	rows := make([]*pb.SQLRow, len(result.Rows))
	for i, row := range result.Rows {
		rows[i] = &pb.SQLRow{Values: row}
	}
	return &pb.SQLResponse{
		Columns: result.Columns,
		Rows:    rows,
		Message: result.Message,
	}, nil
}
