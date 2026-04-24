package gossip

import (
	"context"
	"io"
	"sync"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/rpc"
	"github.com/duchm1606/ducklingdb/internal/util/stop"
)

const defaultInterval = 1 * time.Second

type Gossip struct {
	pb.UnimplementedGossipServiceServer

	nodeID int32
	addr   string
	store  *InfoStore

	mu      sync.RWMutex
	peers   map[string]struct{}

	rpcCtx  *rpc.Context
	stopper *stop.Stopper

	interval time.Duration
}

func New(nodeID int32, addr string, rpcCtx *rpc.Context, stopper *stop.Stopper) *Gossip {
	return &Gossip{
		nodeID:   nodeID,
		addr:     addr,
		store:    NewInfoStore(),
		peers:    make(map[string]struct{}),
		rpcCtx:   rpcCtx,
		stopper:  stopper,
		interval: defaultInterval,
	}
}

func (g *Gossip) AddInfo(key string, val []byte, ttl time.Duration) {
	g.store.AddInfo(key, val, ttl, g.nodeID)
}

func (g *Gossip) GetInfo(key string) (*Info, bool) {
	return g.store.GetInfo(key)
}

func (g *Gossip) AddPeer(addr string) {
	if addr == "" || addr == g.addr {
		return
	}
	g.mu.Lock()
	g.peers[addr] = struct{}{}
	g.mu.Unlock()
}

func (g *Gossip) Start() {
	g.stopper.RunWorker(func() {
		ticker := time.NewTicker(g.interval)
		defer ticker.Stop()
		for {
			select {
			case <-g.stopper.ShouldStop():
				return
			case <-ticker.C:
				g.gossipRound()
			}
		}
	})
}

func (g *Gossip) gossipRound() {
	g.mu.RLock()
	peers := make([]string, 0, len(g.peers))
	for addr := range g.peers {
		peers = append(peers, addr)
	}
	g.mu.RUnlock()

	for _, addr := range peers {
		_ = g.gossipWithPeer(addr)
	}
}

// --- Part E/F: streaming RPC ---

// Gossip implements GossipServiceServer. The client opens a stream,
// sends its delta + high-water, and we respond with our own delta.
// The stream stays open for repeated exchanges.
func (g *Gossip) Gossip(stream pb.GossipService_GossipServer) error {
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		// Merge their delta into our store.
		for _, info := range msg.Delta {
			g.store.Combine(protoToInfo(info))
		}

		// Learn about this peer for future rounds.
		g.AddPeer(msg.Addr)

		// Compute what they haven't seen yet and reply.
		delta := g.store.Delta(int32MapCopy(msg.HighWater))
		resp := &pb.GossipMessage{
			NodeId:    g.nodeID,
			Addr:      g.addr,
			Delta:     infosToProto(delta),
			HighWater: g.store.HighWater(),
		}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// gossipWithPeer is the client side: dial peer, exchange one round.
func (g *Gossip) gossipWithPeer(addr string) error {
	conn, err := g.rpcCtx.GRPCDialNode(addr)
	if err != nil {
		return err
	}

	client := pb.NewGossipServiceClient(conn)
	stream, err := client.Gossip(context.Background())
	if err != nil {
		return err
	}

	msg := &pb.GossipMessage{
		NodeId:    g.nodeID,
		Addr:      g.addr,
		Delta:     infosToProto(g.store.AllInfos()),
		HighWater: g.store.HighWater(),
	}
	if err := stream.Send(msg); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}

	resp, err := stream.Recv()
	if err != nil && err != io.EOF {
		return err
	}
	if resp != nil {
		for _, info := range resp.Delta {
			g.store.Combine(protoToInfo(info))
		}
		g.AddPeer(resp.Addr)
	}
	return nil
}

// --- converters between gossip.Info and pb.Info ---

func protoToInfo(p *pb.Info) *Info {
	var ttl time.Duration
	if p.TtlSeconds > 0 {
		ttl = time.Duration(p.TtlSeconds) * time.Second
	}
	return &Info{
		Key:       p.Key,
		Value:     p.Value,
		OrigStamp: p.OrigStamp,
		NodeID:    p.NodeId,
		TTL:       ttl,
		CreatedAt: time.Now(),
	}
}

func infoToProto(i *Info) *pb.Info {
	ttlSec := int64(0)
	if i.TTL > 0 {
		ttlSec = int64(i.TTL / time.Second)
	}
	return &pb.Info{
		Key:       i.Key,
		Value:     i.Value,
		OrigStamp: i.OrigStamp,
		NodeId:    i.NodeID,
		TtlSeconds: ttlSec,
	}
}

func infosToProto(infos map[string]*Info) map[string]*pb.Info {
	out := make(map[string]*pb.Info, len(infos))
	for k, v := range infos {
		out[k] = infoToProto(v)
	}
	return out
}

// int32MapCopy converts proto's map[int32]int64 to a plain Go map.
func int32MapCopy(m map[int32]int64) map[int32]int64 {
	out := make(map[int32]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
