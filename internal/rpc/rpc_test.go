package rpc

import (
	"context"
	"testing"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

func TestServerStartStop(t *testing.T) {
	srv, err := NewServer(":0")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	addr := srv.Addr()
	if addr == "" {
		t.Fatal("Addr() returned empty string")
	}

	srv.Start()
	srv.Stop()
}

func TestHeartbeatRoundTrip(t *testing.T) {
	wall := hlc.NewManualClock(1000)
	clock := hlc.NewClock(wall, 500*time.Millisecond)

	srv, err := NewServer(":0")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	pb.RegisterInternalServer(srv.GRPCServer(), NewHeartbeatService(clock, 1))
	srv.Start()
	defer srv.Stop()

	rpcCtx := NewContext()
	defer rpcCtx.Close()

	conn, err := rpcCtx.GRPCDialNode(srv.Addr())
	if err != nil {
		t.Fatalf("GRPCDialNode: %v", err)
	}

	client := pb.NewInternalClient(conn)
	resp, err := client.Heartbeat(context.Background(), &pb.PingRequest{
		NodeId:     2,
		ServerTime: pb.FromHLC(hlc.Timestamp{WallTime: 500, Logical: 0}),
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	if resp.NodeId != 1 {
		t.Fatalf("node_id: got %d, want 1", resp.NodeId)
	}
	if resp.ServerTime == nil {
		t.Fatal("server_time is nil")
	}
	if resp.ServerTime.WallTime == 0 {
		t.Fatal("server_time.wall_time is zero")
	}
}

func TestConnectionPoolCaching(t *testing.T) {
	srv, err := NewServer(":0")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv.Start()
	defer srv.Stop()

	rpcCtx := NewContext()
	defer rpcCtx.Close()

	conn1, err := rpcCtx.GRPCDialNode(srv.Addr())
	if err != nil {
		t.Fatalf("first dial: %v", err)
	}

	conn2, err := rpcCtx.GRPCDialNode(srv.Addr())
	if err != nil {
		t.Fatalf("second dial: %v", err)
	}

	if conn1 != conn2 {
		t.Fatal("expected same connection for same address, got different pointers")
	}
}

func TestHeartbeatUpdatesHLC(t *testing.T) {
	wall := hlc.NewManualClock(1000)
	clock := hlc.NewClock(wall, 500*time.Millisecond)

	srv, err := NewServer(":0")
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	pb.RegisterInternalServer(srv.GRPCServer(), NewHeartbeatService(clock, 1))
	srv.Start()
	defer srv.Stop()

	rpcCtx := NewContext()
	defer rpcCtx.Close()

	conn, err := rpcCtx.GRPCDialNode(srv.Addr())
	if err != nil {
		t.Fatalf("GRPCDialNode: %v", err)
	}

	client := pb.NewInternalClient(conn)

	// Send a heartbeat with a timestamp far in the future.
	futureTS := hlc.Timestamp{WallTime: 999999, Logical: 0}
	resp, err := client.Heartbeat(context.Background(), &pb.PingRequest{
		NodeId:     2,
		ServerTime: pb.FromHLC(futureTS),
	})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	// The server's clock should have advanced past the future timestamp
	// because HLC.Update propagates causality.
	serverTS := pb.ToHLC(resp.ServerTime)
	if !futureTS.Less(serverTS) {
		t.Fatalf("server clock did not advance past future timestamp: server=%v, sent=%v",
			serverTS, futureTS)
	}
}
