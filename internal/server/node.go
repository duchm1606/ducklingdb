package server

import (
	"context"
	"fmt"
	"time"

	"github.com/duchm1606/ducklingdb/internal/gossip"
	"github.com/duchm1606/ducklingdb/internal/kv/kvserver"
	"github.com/duchm1606/ducklingdb/internal/kv/kvserver/liveness"
	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/rpc"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

const defaultHeartbeatWorkerInterval = 1 * time.Second

type NodeID int32

type NodeConfig struct {
	Addr               string
	DataDir            string
	JoinAddrs          []string
	MaxOffset          time.Duration
	NodeID             NodeID
	LivenessThreshold  time.Duration // 0 → default (5s)
	HeartbeatInterval  time.Duration // 0 → default (1s)
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

	desc := &pb.NodeDescriptor{
		NodeId:  int32(nodeID),
		Address: srv.Addr(),
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
		id:                nodeID,
		clusterID:         clusterID,
		desc:              desc,
		engine:            engine,
		clock:             clock,
		rpcServer:         srv,
		rpcContext:        rpcCtx,
		gossip:            g,
		liveness:          nl,
		clockMonitor:      clockMonitor,
		stopper:           stopper,
		heartbeatInterval: cfg.HeartbeatInterval,
	}

	allocator := newNodeIDAllocator(engine, clusterID)
	svc := &nodeServer{
		heartbeat: rpc.NewHeartbeatService(clock, int32(nodeID)),
		batch:     kvserver.NewBatchHandler(engine, clock),
		allocator: allocator,
		clusterID: clusterID,
	}
	pb.RegisterInternalServer(srv.GRPCServer(), svc)
	pb.RegisterGossipServiceServer(srv.GRPCServer(), g)

	return n, nil
}

func (n *Node) Start() {
	n.rpcServer.Start()
	n.gossip.Start()
	n.liveness.Start()
	n.startHeartbeatWorker()
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
	n.stopper.Stop()
	n.rpcServer.Stop()
	n.rpcContext.Close()
	n.engine.Close()
}

func (n *Node) NodeID() NodeID                       { return n.id }
func (n *Node) ClusterID() ClusterID                 { return n.clusterID }
func (n *Node) Descriptor() *pb.NodeDescriptor       { return n.desc }
func (n *Node) Engine() storage.Engine               { return n.engine }
func (n *Node) Clock() *hlc.Clock                    { return n.clock }
func (n *Node) RPCAddr() string                      { return n.rpcServer.Addr() }
func (n *Node) RPCContext() *rpc.Context             { return n.rpcContext }
func (n *Node) Gossip() *gossip.Gossip               { return n.gossip }
func (n *Node) Liveness() *liveness.NodeLiveness     { return n.liveness }
func (n *Node) ClockMonitor() *rpc.RemoteClockMonitor { return n.clockMonitor }
func (n *Node) Stopper() *Stopper                    { return n.stopper }

type nodeServer struct {
	pb.UnimplementedInternalServer
	heartbeat *rpc.HeartbeatService
	batch     *kvserver.BatchHandler
	allocator *NodeIDAllocator
	clusterID ClusterID
}

func (s *nodeServer) Heartbeat(ctx context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	return s.heartbeat.Heartbeat(ctx, req)
}

func (s *nodeServer) Batch(ctx context.Context, req *pb.BatchRequest) (*pb.BatchResponse, error) {
	return s.batch.Batch(ctx, req)
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
