// Package client provides a redirect-following client for the DucklingDB node
// API. A request that reaches a non-leader comes back with a NotLeaderError
// carrying the leader's address (see internal/proto NotLeaderError); the client
// transparently redials the leader and retries, so callers do not have to know
// which node currently leads.
package client

import (
	"context"
	"errors"
	"time"

	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/rpc"
)

// maxRedirects bounds how many leader redirects a single call will follow before
// giving up, so a redirect loop (e.g. two nodes disagreeing about the leader)
// cannot hang the caller.
const maxRedirects = 10

// redirectBackoff is how long to wait before retrying when the contacted node is
// a follower but does not yet know the leader's address (a redirect with an
// empty address, seen during an election).
const redirectBackoff = 50 * time.Millisecond

// ErrTooManyRedirects is returned when a call is redirected more than
// maxRedirects times without reaching a leader.
var ErrTooManyRedirects = errors.New("client: too many leader redirects")

// Client talks to a DucklingDB cluster, following NotLeaderError redirects to
// whichever node currently leads. It is safe to reuse across calls; the
// best-known leader address is remembered so a steady cluster is reached in one
// hop after the first redirect.
type Client struct {
	rpcCtx *rpc.Context
	addr   string
}

// New returns a Client that starts by contacting addr.
func New(addr string) *Client {
	return &Client{rpcCtx: rpc.NewContext(), addr: addr}
}

// Close releases pooled connections.
func (c *Client) Close() { c.rpcCtx.Close() }

// ExecSQL runs sql, following leader redirects. The returned SQLResponse never
// has NotLeader set — a redirect is followed, not surfaced.
func (c *Client) ExecSQL(ctx context.Context, sql string) (*pb.SQLResponse, error) {
	for attempt := 0; attempt < maxRedirects; attempt++ {
		conn, err := c.rpcCtx.GRPCDialNode(c.addr)
		if err != nil {
			return nil, err
		}
		resp, err := pb.NewInternalClient(conn).ExecSQL(ctx, &pb.SQLRequest{Sql: sql})
		if err != nil {
			return nil, err
		}
		if resp.GetNotLeader() == nil {
			return resp, nil
		}
		if err := c.followRedirect(ctx, resp.GetNotLeader()); err != nil {
			return nil, err
		}
	}
	return nil, ErrTooManyRedirects
}

// Batch runs a KV batch, following leader redirects. The returned BatchResponse
// never has NotLeader set.
func (c *Client) Batch(ctx context.Context, req *pb.BatchRequest) (*pb.BatchResponse, error) {
	for attempt := 0; attempt < maxRedirects; attempt++ {
		conn, err := c.rpcCtx.GRPCDialNode(c.addr)
		if err != nil {
			return nil, err
		}
		resp, err := pb.NewInternalClient(conn).Batch(ctx, req)
		if err != nil {
			return nil, err
		}
		if resp.GetNotLeader() == nil {
			return resp, nil
		}
		if err := c.followRedirect(ctx, resp.GetNotLeader()); err != nil {
			return nil, err
		}
	}
	return nil, ErrTooManyRedirects
}

// followRedirect points the client at the leader named in nl. When the leader's
// address is known it switches immediately; when it is unknown (mid-election),
// it waits briefly so the next attempt retries the same node once a leader has
// settled. Returns ctx.Err() if the context expires during the wait.
func (c *Client) followRedirect(ctx context.Context, nl *pb.NotLeaderError) error {
	if addr := nl.GetLeaderAddress(); addr != "" && addr != c.addr {
		c.addr = addr
		return nil
	}
	select {
	case <-time.After(redirectBackoff):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
