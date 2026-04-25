package main

import (
	"bufio"
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
	"github.com/duchm1606/ducklingdb/internal/sql/executor"
	"github.com/duchm1606/ducklingdb/internal/storage/lsm"
	"github.com/duchm1606/ducklingdb/internal/util/hlc"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: ducklingdb <start|status|repl> [flags]")
		os.Exit(1)
	}
	switch os.Args[1] {
	case "start":
		runStart(os.Args[2:])
	case "status":
		runStatus(os.Args[2:])
	case "repl":
		runRepl(os.Args[2:])
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
	peersStr := fs.String("peers", "", "comma-separated list of ALL cluster addresses (including self) for Raft")
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

	var peerAddrs []string
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			p = strings.TrimSpace(p)
			if p == "" {
				continue
			}
			if strings.HasPrefix(p, ":") {
				p = "127.0.0.1" + p
			}
			peerAddrs = append(peerAddrs, p)
		}
	}

	n, err := server.NewNode(server.NodeConfig{
		Addr:      *addr,
		DataDir:   *dataDir,
		JoinAddrs: joinAddrs,
		Peers:     peerAddrs,
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

// ── repl ─────────────────────────────────────────────────────────────────────

func runRepl(args []string) {
	fs := flag.NewFlagSet("repl", flag.ExitOnError)
	dataDir := fs.String("data", "", "data directory (local engine mode)")
	addr := fs.String("addr", "", "node address to connect to via gRPC (remote mode)")
	fs.Parse(args)

	if *addr != "" {
		runReplRemote(*addr)
		return
	}

	if *dataDir == "" {
		fmt.Fprintf(os.Stderr, "error: --data or --addr is required\n")
		os.Exit(1)
	}

	engine, err := lsm.OpenLSM(lsm.LSMOptions{Dir: *dataDir})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: open engine: %v\n", err)
		os.Exit(1)
	}
	defer engine.Close()

	clock := hlc.NewClock(hlc.SystemWallClock(), 500*time.Millisecond)
	exec := executor.New(engine, clock)

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Fprint(os.Stdout, "duck> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == `\q` || line == `\quit` {
			break
		}
		res, err := exec.Execute(line)
		if err != nil {
			fmt.Fprintf(os.Stdout, "ERROR: %v\n", err)
			continue
		}
		if len(res.Columns) > 0 {
			printTable(res)
		} else {
			fmt.Fprintln(os.Stdout, res.Message)
		}
	}
}

func runReplRemote(addr string) {
	if strings.HasPrefix(addr, ":") {
		addr = "127.0.0.1" + addr
	}
	rpcCtx := rpc.NewContext()
	defer rpcCtx.Close()

	conn, err := rpcCtx.GRPCDialNode(addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: dial %s: %v\n", addr, err)
		os.Exit(1)
	}
	client := pb.NewInternalClient(conn)

	fmt.Printf("Connected to %s\n", addr)
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Fprint(os.Stdout, "duck> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if line == `\q` || line == `\quit` {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		resp, err := client.ExecSQL(ctx, &pb.SQLRequest{Sql: line})
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stdout, "ERROR: %v\n", err)
			continue
		}
		if resp.Error != "" {
			fmt.Fprintf(os.Stdout, "ERROR: %s\n", resp.Error)
			continue
		}
		if len(resp.Columns) > 0 {
			// Convert to executor.Result for display.
			rows := make([][]string, len(resp.Rows))
			for i, r := range resp.Rows {
				rows[i] = r.Values
			}
			printTable(&executor.Result{
				Columns: resp.Columns,
				Rows:    rows,
				Message: resp.Message,
			})
		} else {
			fmt.Fprintln(os.Stdout, resp.Message)
		}
	}
}

// printTable renders a Result with column headers as an ASCII table.
func printTable(res *executor.Result) {
	// Compute column widths — at least the header width.
	widths := make([]int, len(res.Columns))
	for i, col := range res.Columns {
		widths[i] = len(col)
	}
	for _, row := range res.Rows {
		for i, val := range row {
			if len(val) > widths[i] {
				widths[i] = len(val)
			}
		}
	}

	// Header row.
	printRow(res.Columns, widths)

	// Separator.
	parts := make([]string, len(widths))
	for i, w := range widths {
		parts[i] = strings.Repeat("-", w+2)
	}
	fmt.Fprintln(os.Stdout, strings.Join(parts, "+"))

	// Data rows.
	for _, row := range res.Rows {
		printRow(row, widths)
	}

	// Row count.
	n := len(res.Rows)
	if n == 1 {
		fmt.Fprintln(os.Stdout, "(1 row)")
	} else {
		fmt.Fprintf(os.Stdout, "(%d rows)\n", n)
	}
}

func printRow(vals []string, widths []int) {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = fmt.Sprintf(" %-*s ", widths[i], v)
	}
	fmt.Fprintln(os.Stdout, strings.Join(parts, "|"))
}
