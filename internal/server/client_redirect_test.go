package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/duchm1606/ducklingdb/internal/client"
)

// TestFollowerRejectsReadWithLeaderAddress asserts the typed redirect carries a
// DIALABLE address and that the redirect-following client helper uses it to
// reach the right value. It lives here (not in redirect_test.go) so the client
// package and the test that exercises it commit together — the server-side
// tests in redirect_test.go do not depend on the client.
func TestFollowerRejectsReadWithLeaderAddress(t *testing.T) {
	nodes := startPeersCluster(t, 3)
	leader := waitForClusterLeader(t, nodes, 10*time.Second)
	follower := followerOf(t, nodes, leader)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	key, value := []byte("redir-key"), []byte("redir-value")

	// A redirect-following client that deliberately STARTS at a follower. Its
	// write must be carried to the leader by following the redirect.
	cli := client.New(follower.RPCAddr())
	defer cli.Close()
	wresp, err := cli.Batch(ctx, writeBatch(key, value))
	if err != nil {
		t.Fatalf("client write starting at a follower (should follow redirect): %v", err)
	}
	if wresp.GetNotLeader() != nil {
		t.Fatal("client surfaced NotLeader instead of following the redirect")
	}
	rresp, err := cli.Batch(ctx, readBatch(key))
	if err != nil {
		t.Fatalf("client read: %v", err)
	}
	if got := rresp.GetResponses()[0].GetGet().GetValue().GetRawBytes(); !bytes.Equal(got, value) {
		t.Fatalf("client read via redirect returned %q, want %q", got, value)
	}

	// Confirm the raw redirect a follower hands back is actually dialable: dial
	// the follower directly, take the address it returns, and dial THAT.
	fc := dialNode(t, follower.RPCAddr())
	raw, err := fc.Batch(ctx, readBatch(key))
	if err != nil {
		t.Fatalf("direct follower read: %v", err)
	}
	nl := raw.GetNotLeader()
	if nl == nil {
		t.Fatal("direct read to follower must redirect, not serve")
	}
	if nl.GetLeaderAddress() == "" {
		t.Fatal("redirect address is empty — a client cannot follow it")
	}
	lc := dialNode(t, nl.GetLeaderAddress())
	lresp, err := lc.Batch(ctx, readBatch(key))
	if err != nil {
		t.Fatalf("dialing the redirect address failed — not dialable: %v", err)
	}
	if lresp.GetNotLeader() != nil {
		t.Fatalf("redirect pointed at a non-leader: %+v", lresp.GetNotLeader())
	}
	if got := lresp.GetResponses()[0].GetGet().GetValue().GetRawBytes(); !bytes.Equal(got, value) {
		t.Fatalf("read at the redirect target returned %q, want %q", got, value)
	}
}
