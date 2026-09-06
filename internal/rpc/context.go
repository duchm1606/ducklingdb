package rpc

import (
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Context struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	opts  []grpc.DialOption
}

func NewContext() *Context {
	return &Context{
		conns: make(map[string]*grpc.ClientConn),
		opts: []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// Match the server's ceiling so a large MsgSnap is not rejected on
			// send or receive. See MaxMessageBytes.
			grpc.WithDefaultCallOptions(
				grpc.MaxCallRecvMsgSize(MaxMessageBytes),
				grpc.MaxCallSendMsgSize(MaxMessageBytes),
			),
		},
	}
}

func (c *Context) GRPCDialNode(addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}

	conn, err := grpc.NewClient(addr, c.opts...)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *Context) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for addr, conn := range c.conns {
		conn.Close()
		delete(c.conns, addr)
	}
}
