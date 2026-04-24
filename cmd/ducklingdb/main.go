package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/duchm1606/ducklingdb/internal/gossip"
	pb "github.com/duchm1606/ducklingdb/internal/proto"
	"github.com/duchm1606/ducklingdb/internal/rpc"
	"github.com/duchm1606/ducklingdb/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ducklingdb <start|status> [flags]")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "start":
		runStart(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		os.Exit(1)
	}
}

// ── start ────────────────────────────────────────────────────────────────────

func runStart(args []string) {
	fs := flag.NewFlagSet("start", flag.ExitOnError)
	addr := fs.String("addr", ":26257", "gRPC listen address")
	dataDir := fs.String("data", "", "data directory (required)")
	joinStr := fs.String("join", "", "comma-separated seed addresses to join an existing cluster")
	statusInterval := fs.Duration("status-interval", 5*time.Second, "how often to print cluster status (0 = off)")
	fs.Parse(args)

	if *dataDir == "" {
		log.Fatal("--data is required")
	}

	var joinAddrs []string
	if *joinStr != "" {
		for _, a := range strings.Split(*joinStr, ",") {
			a = strings.TrimSpace(a)
			if a == "" {
				continue
			}
			if strings.HasPrefix(a, ":") {
				a = "127.0.0.1" + a
			}
			joinAddrs = append(joinAddrs, a)
		}
	}

	n, err := server.NewNode(server.NodeConfig{
		Addr:      *addr,
		DataDir:   *dataDir,
		JoinAddrs: joinAddrs,
	})
	if err != nil {
		log.Fatalf("init node: %v", err)
	}

	// Register callbacks before Start so no early gossip rounds are missed.
	n.Gossip().RegisterCallback(gossip.KeyNodeDescPrefix, func(key string, val []byte) {
		log.Printf("[gossip] node-desc  key=%s bytes=%d", key, len(val))
	})
	n.Gossip().RegisterCallback(gossip.KeyNodeLivenessPrefix, func(key string, val []byte) {
		log.Printf("[gossip] liveness   key=%s bytes=%d", key, len(val))
	})
	n.Gossip().RegisterCallback(gossip.KeyStoreDescPrefix, func(key string, val []byte) {
		log.Printf("[gossip] store-desc key=%s bytes=%d", key, len(val))
	})

	n.Start()

	clusterID := n.ClusterID()
	log.Printf("DucklingDB node %d started", n.NodeID())
	log.Printf("  listen:   %s", n.RPCAddr())
	log.Printf("  cluster:  %x", clusterID)
	log.Printf("  data dir: %s", *dataDir)
	if len(joinAddrs) == 0 {
		log.Printf("  role:     bootstrap (first node)")
	} else {
		log.Printf("  joined:   %s", strings.Join(joinAddrs, ", "))
	}

	// Optional periodic status printer.
	if *statusInterval > 0 {
		go func() {
			tick := time.NewTicker(*statusInterval)
			defer tick.Stop()
			for range tick.C {
				peers := n.Gossip().Peers()
				if len(peers) == 0 {
					log.Printf("[status] node=%d  peers=(none)", n.NodeID())
				} else {
					log.Printf("[status] node=%d  peers=%s", n.NodeID(), strings.Join(peers, ", "))
				}
			}
		}()
	}

	// Block until SIGINT / SIGTERM.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig

	log.Printf("shutting down node %d…", n.NodeID())
	n.Stop()
	log.Printf("node %d stopped", n.NodeID())
}

// ── status ───────────────────────────────────────────────────────────────────

func runStatus(args []string) {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	addr := fs.String("addr", ":26257", "address of the node to query")
	fs.Parse(args)

	target := *addr
	if strings.HasPrefix(target, ":") {
		target = "127.0.0.1" + target
	}

	rpcCtx := rpc.NewContext()
	defer rpcCtx.Close()

	conn, err := rpcCtx.GRPCDialNode(target)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := pb.NewInternalClient(conn).Heartbeat(ctx, &pb.PingRequest{NodeId: 0})
	if err != nil {
		log.Fatalf("heartbeat: %v", err)
	}

	fmt.Printf("node_id:     %d\n", resp.NodeId)
	fmt.Printf("server_time: wall=%d logical=%d\n",
		resp.ServerTime.WallTime, resp.ServerTime.Logical)
	fmt.Printf("addr:        %s\n", *addr)
}
