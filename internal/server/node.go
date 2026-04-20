package server

import (
	"fmt"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/rpc"
	"github.com/duchm1606/ducklingdb/internal/storage"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

type NodeID int32

type NodeConfig struct {
	Addr      string
	DataDir   string
	JoinAddrs []string
	MaxOffset time.Duration
	NodeID    NodeID
}

type Node struct {
	id         NodeID
	desc       *pb.NodeDescriptor
	engine     storage.Engine
	clock      *hlc.Clock
	rpcServer  *rpc.Server
	rpcContext *rpc.Context
	stopper    *Stopper
}

func NewNode(cfg NodeConfig) (*Node, error) {
	if cfg.MaxOffset == 0 {
		cfg.MaxOffset = 500 * time.Millisecond
	}
	if cfg.NodeID == 0 {
		cfg.NodeID = 1
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

	desc := &pb.NodeDescriptor{
		NodeId:  int32(cfg.NodeID),
		Address: srv.Addr(),
	}

	n := &Node{
		id:         cfg.NodeID,
		desc:       desc,
		engine:     engine,
		clock:      clock,
		rpcServer:  srv,
		rpcContext: rpcCtx,
		stopper:    NewStopper(),
	}

	hb := rpc.NewHeartbeatService(clock, int32(cfg.NodeID))
	pb.RegisterInternalServer(srv.GRPCServer(), hb)

	return n, nil
}

func (n *Node) Start() {
	n.rpcServer.Start()
}

func (n *Node) Stop() {
	n.stopper.Stop()
	n.rpcServer.Stop()
	n.rpcContext.Close()
	n.engine.Close()
}

func (n *Node) NodeID() NodeID           { return n.id }
func (n *Node) Descriptor() *pb.NodeDescriptor { return n.desc }
func (n *Node) Engine() storage.Engine    { return n.engine }
func (n *Node) Clock() *hlc.Clock         { return n.clock }
func (n *Node) RPCAddr() string           { return n.rpcServer.Addr() }
func (n *Node) RPCContext() *rpc.Context   { return n.rpcContext }
func (n *Node) Stopper() *Stopper         { return n.stopper }
