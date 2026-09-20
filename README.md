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

### Multi-node cluster (gossip + Raft)

Open three terminals. Each node needs its own data directory.

Two flags drive cluster membership, and they do different jobs:

- `--join` seeds **gossip** — how a node discovers its peers' liveness and descriptors.
- `--peers` defines the **Raft group** — the full list of member addresses, *including the node's own*, identical on every node. Omit it and the node forms a single-node Raft group that replicates nothing.

**Terminal 1 — bootstrap node**

```bash
./bin/ducklingdb start --addr :26257 --data /tmp/duck1 \
  --peers :26257,:26258,:26259
```

**Terminal 2 — join node**

```bash
./bin/ducklingdb start --addr :26258 --data /tmp/duck2 --join :26257 \
  --peers :26257,:26258,:26259
```

**Terminal 3 — join node**

```bash
./bin/ducklingdb start --addr :26259 --data /tmp/duck3 --join :26257 \
  --peers :26257,:26258,:26259
```

Start the bootstrap node first — a joining node exits if it can't reach a seed.

Within a few seconds you'll see gossip log lines in each terminal as nodes exchange liveness and descriptor updates:

```
[gossip] liveness   key=node-liveness:2 bytes=81
[gossip] node-desc  key=node-desc:3 bytes=120
```

**Check a node's status** (separate terminal):

```bash
./bin/ducklingdb status --addr :26257
```

**Run SQL against the cluster** (separate terminal):

```bash
./bin/ducklingdb repl --addr :26258
```

Writes are replicated through Raft, so a table created via one node is readable
through any other. Reads and writes are served by the Raft leader; a REPL
pointed at a follower follows the redirect automatically.

> Note: the cluster runs a single Raft group over the whole keyspace — range splits and a separate leaseholder role are M5 work.

## Milestones

| #   | Milestone                                    | Status  |
| --- | -------------------------------------------- | ------- |
| M1  | LSM Tree Storage Engine + MVCC + HLC         | Done    |
| M2  | Single-Node ACID Transactions (SI/SSI)       | Done    |
| M3  | gRPC Networking + Gossip Protocol + SQL REPL | Done    |
| M4  | Raft Consensus (single group) + Snapshots    | Done    |
| M5  | Ranges + Leases + Distributed Transactions   | Planned |
| M6  | pgwire (psql compatibility)                  | Planned |

## Acknowledgements

DucklingDB is heavily inspired by [CockroachDB](https://github.com/cockroachdb/cockroach) and its design documents. Several key ideas were studied directly from the CockroachDB codebase and engineering blog:

- MVCC key encoding and timestamp-ordered storage layout
- Hybrid Logical Clock (HLC) for causally consistent timestamps across nodes
- Raft consensus via the `RawNode` / `Ready` loop pattern
- Gossip-based cluster membership and node liveness tracking
- BatchRequest / BatchResponse as the uniform write path through Raft

The [CockroachDB design docs](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md) and the Cockroach Labs engineering blog were invaluable references throughout.

## License

MIT
