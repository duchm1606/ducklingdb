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

## Build

```bash
make build
# binary: bin/ducklingdb
```

## Running

### SQL REPL

Open an interactive SQL prompt against a local data directory:

```bash
./bin/ducklingdb repl --data /tmp/duck1
```

```
duck> CREATE TABLE users (id INT PRIMARY KEY, name TEXT);
OK
duck> INSERT INTO users VALUES (1, 'alice');
INSERT 1
duck> SELECT * FROM users;
 id | name
----+-------
  1 | alice
(1 row)
duck> \q
```

Supported statements: `CREATE TABLE`, `INSERT`, `SELECT` (full scan + `WHERE pk = val`), `UPDATE`, `DELETE`. Each statement auto-commits. Type `\q` or `\quit` to exit.

### Multi-node cluster (gossip)

Open three terminals. Each node needs its own data directory.

**Terminal 1 — bootstrap node**

```bash
./bin/ducklingdb start --addr :26257 --data /tmp/duck1
```

**Terminal 2 — join node**

```bash
./bin/ducklingdb start --addr :26258 --data /tmp/duck2 --join :26257
```

**Terminal 3 — join node**

```bash
./bin/ducklingdb start --addr :26259 --data /tmp/duck3 --join :26257
```

Within a few seconds you'll see gossip log lines in each terminal as nodes exchange liveness and descriptor updates:

```
[gossip] liveness   key=node-liveness:2 bytes=81
[gossip] node-desc  key=node-desc:3 bytes=120
```

**Check a node's status** (separate terminal):

```bash
./bin/ducklingdb status --addr :26257
```

> Note: nodes share cluster metadata via gossip but do not replicate SQL data yet — that requires the Raft layer (M4).

## Milestones

| #   | Milestone                                    | Status  |
| --- | -------------------------------------------- | ------- |
| M1  | LSM Tree Storage Engine + MVCC + HLC         | Done    |
| M2  | Single-Node ACID Transactions (SI/SSI)       | Done    |
| M3  | gRPC Networking + Gossip Protocol + SQL REPL | Done    |
| M4  | Raft Consensus + Ranges + Leases             | Planned |
| M5  | Distributed Transactions + Concurrency       | Planned |
| M6  | pgwire (psql compatibility)                  | Planned |

## License

MIT
