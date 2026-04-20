package rpc

import (
	"net"

	"google.golang.org/grpc"
)

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
		grpcServer: grpc.NewServer(),
		listener:   lis,
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
