# DucklingDB

A distributed SQL database built from scratch in Go for learning purposes, inspired by [CockroachDB](https://github.com/cockroachdb/cockroach).

## Why

CockroachDB is a masterclass in distributed systems design — MVCC, Raft consensus, distributed transactions, range-based partitioning — but its 2M+ line codebase is hard to learn from. DucklingDB rebuilds the core layers from scratch with clarity as the primary goal.

## Architecture

```
 SQL Client (pgwire)
        │
 SQL Layer (parser → executor)
        │
 Distributed KV (TxnCoordSender)
        │
 KV Server (Range + Raft + Lease)
        │
 Transactions + Concurrency
        │
 MVCC Layer
        │
 Storage Engine (LSM Tree)
```

## Milestones

| # | Milestone | Status |
|---|-----------|--------|
| M1 | LSM Tree Storage Engine + MVCC + HLC | Planned |
| M2 | Single-Node ACID Transactions (SI/SSI) | Planned |
| M3 | gRPC Networking + Gossip Protocol | Planned |
| M4 | Raft Consensus + Ranges + Leases | Planned |
| M5 | Distributed Transactions + Concurrency | Planned |
| M6 | SQL Layer (stretch goal) | Planned |

See [`docs/SCOPE.md`](docs/SCOPE.md) for the full project scope, feature breakdown, and implementation plan.

## License

MIT
