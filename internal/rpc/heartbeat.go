package rpc

import (
	"context"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

type HeartbeatService struct {
	pb.UnimplementedInternalServer
	clock  *hlc.Clock
	nodeID int32
}

func NewHeartbeatService(clock *hlc.Clock, nodeID int32) *HeartbeatService {
	return &HeartbeatService{clock: clock, nodeID: nodeID}
}

func (h *HeartbeatService) Heartbeat(_ context.Context, req *pb.PingRequest) (*pb.PingResponse, error) {
	if req.ServerTime != nil {
		h.clock.Update(pb.ToHLC(req.ServerTime))
	}
	now := h.clock.Now()
	return &pb.PingResponse{
		NodeId:     h.nodeID,
		ServerTime: pb.FromHLC(now),
	}, nil
}
