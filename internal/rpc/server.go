package rpc

import (
	"net"

	"google.golang.org/grpc"
)

// MaxMessageBytes is the ceiling for a single gRPC message in either direction.
//
// It exists because a Raft MsgSnap carries the entire state-machine snapshot in
// one message (see kvserver.serializeEngineState). gRPC's default 4 MiB limit
// would make any snapshot past that size fail — and the send error was
// historically discarded, stranding the receiving follower permanently and
// silently. 64 MiB matches the M5 range-split threshold that will bound how
// large a single range's snapshot can get, so no legitimate snapshot exceeds it.
const MaxMessageBytes = 64 << 20 // 64 MiB

type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
}

func NewServer(addr string) (*Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &Server{
		grpcServer: grpc.NewServer(
			grpc.MaxRecvMsgSize(MaxMessageBytes),
			grpc.MaxSendMsgSize(MaxMessageBytes),
		),
		listener: lis,
	}, nil
}

func (s *Server) GRPCServer() *grpc.Server {
	return s.grpcServer
}

func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

func (s *Server) Start() {
	go s.grpcServer.Serve(s.listener)
}

func (s *Server) Stop() {
	s.grpcServer.GracefulStop()
}
