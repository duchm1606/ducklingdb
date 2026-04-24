package server

import (
	"context"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/raft"
)

func raftMessageToProto(m raft.Message) *pb.RaftMessage {
	entries := make([]*pb.RaftEntry, len(m.Entries))
	for i, e := range m.Entries {
		entries[i] = &pb.RaftEntry{Term: e.Term, Index: e.Index, Data: e.Data}
	}
	return &pb.RaftMessage{
		From: m.From, To: m.To, Term: m.Term,
		LogTerm: m.LogTerm, Index: m.Index, Commit: m.Commit,
		Type: uint32(m.Type), Reject: m.Reject, RejectHint: m.RejectHint,
		Entries: entries,
	}
}

func protoToRaftMessage(m *pb.RaftMessage) raft.Message {
	entries := make([]raft.Entry, len(m.Entries))
	for i, e := range m.Entries {
		entries[i] = raft.Entry{Term: e.Term, Index: e.Index, Data: e.Data}
	}
	return raft.Message{
		From: m.From, To: m.To, Term: m.Term,
		LogTerm: m.LogTerm, Index: m.Index, Commit: m.Commit,
		Type: raft.MessageType(m.Type), Reject: m.Reject, RejectHint: m.RejectHint,
		Entries: entries,
	}
}

type raftServiceServer struct {
	pb.UnimplementedRaftServiceServer
	node *Node
}

func (s *raftServiceServer) Step(_ context.Context, m *pb.RaftMessage) (*pb.RaftMessageResponse, error) {
	if r := s.node.getReplica(); r != nil {
		r.Step(protoToRaftMessage(m))
	}
	return &pb.RaftMessageResponse{}, nil
}
