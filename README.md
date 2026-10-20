# NodusDB

[![CI](https://github.com/Pragadeesh-19/nodusdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Pragadeesh-19/nodusdb/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/nodusdb)](https://pypi.org/project/nodusdb/)
[![Python](https://img.shields.io/pypi/pyversions/nodusdb)](https://pypi.org/project/nodusdb/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

**An embedded graph database and lake write buffer for Python.** It runs inside your process. No server and no JVM to install: `pip install nodusdb` and import it. It makes network calls only if you ask it to ship its log to a bucket.

NodusDB has two engines built from the same low-level ideas:

- **A graph engine** that holds a directed graph in memory and answers traversals and permission checks in microseconds, while edges are added and removed thousands of times a second. It can persist itself with a write-ahead log, copy that log to a bucket, and serve read replicas, erasure requests, change feeds and time travel from it.
- **A lake engine** that takes streaming upserts and deletes one at a time and commits them as a few large Parquet files that DuckDB, Spark, PyArrow and Iceberg tooling can read.

Both keep their hot paths free of garbage-collector work. Both were written for state that changes all the time.

## Try it

```sh
pip install nodusdb
```

You need Python 3.10 or newer on Linux (x86_64 or arm64), macOS 11 or newer, or Windows x86_64. The wheel carries a native library built with GraalVM, so there is nothing else to install.

```python
import nodusdb

with nodusdb.Graph() as g:
    g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])

    print("reachable within 3 hops:", g.khop(start=1, max_depth=3))
    print("common neighbors of 3 and 5:", g.common_neighbors(3, 5))
```

```
reachable within 3 hops: [2, 5, 3, 4]
common neighbors of 3 and 5: [4]
```

And the lake:

```python
import nodusdb

schema = {"amount": "int64", "status": "int32", "label": "utf8"}

with nodusdb.LakeTable("orders", schema) as table:
    table.upsert("order-101", {"amount": 5000, "status": 1, "label": "cleared"})
    table.upsert("order-102", {"amount": 1200, "status": 0, "label": "pending"})
    table.delete("order-102")
    print(table.get("order-101"))    # {'amount': 5000, 'status': 1, 'label': 'cleared'}
    print(table.get("order-102"))    # None
```

Leaving the block flushes the rows to `orders/data-00000000.parquet` and the delete to `orders/delete-00000000.parquet`. Every example in this repository is a runnable script in [`examples/`](examples).

## Why it exists

Picture a graph that changes constantly: an agent's working memory, a live permission graph, a presence graph of who is looking at what. Edges come in and go out thousands of times a second, and a question has to be answered before the next event arrives.

The usual tools struggle with that shape of work.

| Design | Good at | Under constant churn |
|---|---|---|
| Compressed sparse row (Kùzu and similar) | Scanning every edge in order | One insert shifts an array, so a single write can cost as much as the graph |
| A disk B-tree with a recursive query (SQLite and a CTE) | Durability and a familiar language | Every hop walks pages, and every write splits or rewrites them |

NodusDB keeps each node's neighbors as a small set that supports insert, delete and membership in constant time, and sweeps in order. Nothing in a write depends on the size of the rest of the graph. [How it works](docs/how-it-works.md) builds that up from nothing in a few pages.

The lake engine exists for a related reason. Open table formats store immutable Parquet files, so a writer that commits one record at a time makes a file per commit, and a day of that makes thousands of small files that every query must open. NodusDB sits in front of the table, takes the records one by one, and commits them in batches sized for the lake.

## The graph engine

You add and remove directed edges, and ask questions that start at a node: who it points at, who points at it, what is within *k* hops, what two nodes share. A node can be an integer, a string or a UUID.

```python
import nodusdb

with nodusdb.Graph(path="/data/graph", sync_mode="sync") as g:
    g.add_edges_from([("user:alice", "role:admin"), ("user:bob", "role:admin")])
    g.common_neighbors("user:alice", "user:bob")      # ['role:admin']
    g.checkpoint()

with nodusdb.Graph(path="/data/graph") as g:           # recovers from the directory
    g.has_edge("user:alice", "role:admin")             # True
```

Give it a directory and it is durable: every change goes to a checksummed write-ahead log, a checkpoint writes a snapshot, and a restart or a crash recovers both. `max_memory_mb` caps its native memory and rejects a write that would pass the cap before anything changes. The full guide, with every call and its return value, is [docs/graph.md](docs/graph.md).

### What it measures

From `python/benchmarks/compare.py`, run on 2026-10-20 against the code in this repository, built with the toolchain of the release wheels. It loads 1,000,000 directed edges over 200,000 nodes, deletes 500,000, and runs 200 queries of each kind. Every backend is checked for identical answers before timing starts.

| Configuration | Insert edges/s | Delete edges/s | 3-hop median | Common neighbors median |
|---|---:|---:|---:|---:|
| NodusDB, flat `array('q')` | 810,031 | 807,466 | 11.5 µs | 8.3 µs |
| NodusDB, batch of tuples | 477,112 | 469,098 | 11.3 µs | 8.3 µs |
| NodusDB, single-edge calls | 163,443 | 168,178 | 11.7 µs | 8.3 µs |
| NetworkX, single | 176,695 | 405,820 | 25.3 µs | 5.0 µs |
| NetworkX, batch | 207,937 | 393,485 | 26.2 µs | 5.1 µs |
| SQLite, `executemany` | 187,884 | 175,200 | 80.9 µs | 16.5 µs |
| SQLite, one statement per row | 147,211 | 141,748 | 78.5 µs | 15.9 µs |

Read it plainly. The flat buffer loads edges 3.9 times as fast as NetworkX's batch insert and 4.3 times as fast as SQLite's `executemany`, and deletes them twice as fast as NetworkX. A 3-hop query is 2.2 times faster than NetworkX and about 7 times faster than SQLite. NetworkX is still faster on common neighbors (5.0 µs against 8.3 µs). Single-edge calls are limited by the cost of a Python call into the library. These are single runs on a laptop CPU that boosts and throttles.

**This is not what 0.1.0 on PyPI does yet.** The 0.1.0 library read native memory through an API that costs about 300 ns per access when compiled ahead of time, and it loaded 162,689 edges per second on this benchmark with a 27.3 µs 3-hop median. The fix is in this repository and goes out in the next release. [docs/designs/engineering-review-native-memory.md](docs/designs/engineering-review-native-memory.md) tells the story.

On the real SNAP soc-Pokec social network (30,622,564 edges), measured on 2026-10-04 and 2026-10-05:

| Workload | NodusDB | Next best |
|---|---:|---:|
| Mixed churn, 100,000 operations (60% insert, 30% delete, 10% query) | 81,525 ops/s | NetworkX 55,451, SQLite 3,617 |
| Median 3-hop query on the full graph | 1.21 ms | igraph 7.5 ms, SQLite 49.8 ms |
| Permission check (generated workload, 81,500 edges) | 38.0 µs | NetworkX 171 µs, SQLite 400 µs |
| Durable ingest, synchronous log, batches | 1.17 million edges/s | |
| Recovery of 30.6M edges plus 500,000 log frames | 2.8 s | |

Those suites ran with an earlier build of the library and have not been re-run since. [docs/benchmarks.md](docs/benchmarks.md) has every table, the methods, and where each other engine wins.

## Permissions

The graph can hold typed relationship tuples under a schema and answer "can this subject do this to that object", in the style of Google's Zanzibar. A document inherits access from its folder, a folder grants it to a group, and groups nest.

```python
SCHEMA = """
schema 1
type user
type group { relation member: user | group#member }
type folder {
  relation viewer: user | group#member
  relation parent: folder
  permission view = viewer + parent->view
}
type document {
  relation parent: folder
  relation viewer: user | group#member
  permission view = viewer + parent->view
}
"""

with nodusdb.Graph(path="/data/authz", sync_mode="sync") as g:
    g.apply_schema(SCHEMA)
    g.add_tuple("document:readme", "parent", "folder:eng")
    g.add_tuple("folder:eng", "viewer", "group:staff", "member")
    g.add_tuple("group:staff", "member", "group:backend", "member")
    token = g.add_tuple("group:backend", "member", "user:alice")

    g.check("document:readme", "view", "user:alice", at_least=token)    # True
    g.check("document:readme", "view", "user:bob")                       # False
```

Every write returns a token, and passing it as `at_least` makes a check refuse to answer from state older than that write. A transaction groups several writes so that all of them apply or none do. For deep group trees, a derived index of the nesting turns a check against the root of 1,111 nested groups from over 100 µs into under 1 µs. See [docs/permissions.md](docs/permissions.md).

## The lake engine

`LakeTable` takes keyed upserts and deletes, keeps the rows in memory as columns, and writes them out as Parquet when a buffer fills or a timer fires. Reads see every accepted write at once. Two buffers alternate, so writers carry on while the previous buffer is written in the background.

```python
import pyarrow as pa
import nodusdb

schema = {"amount": "int64", "score": "float64", "status": "int32", "label": "utf8"}

with nodusdb.LakeTable("orders", schema, flush_rows=250_000, flush_interval=5.0) as table:
    table.upsert_table(pa.table({"id": ids, "amount": amounts, "score": scores,
                                 "status": statuses, "label": labels}), key="id")
    table.delete(1_000)
    table.get(1_001)                       # a dict, or None
    table.sum("amount")                    # a running total over the rows still in memory
```

The files are standard Parquet v1 with Snappy compression, dictionary encoding for low-cardinality columns, and min, max and null-count statistics. Updates and deletes land as new rows and tombstone files, and the guide shows the DuckDB query that reads the current state. See [docs/lake-guide.md](docs/lake-guide.md).

### What it measures

On the first 1,000,000 rows of the NYC TLC Yellow Taxi file for January 2024, each engine in its own process, measured on 2026-10-20. NodusDB runs the code in this repository; the other engines ran the same day on the same laptop:

| Measure | NodusDB | DuckDB | PyArrow | SQLite |
|---|---:|---:|---:|---:|
| Ingest of 1,000,000 rows, rows/s | 994,900 | 147,314 | 547,340 | 285,608 |
| 500,000 upserts, one Arrow batch, rows/s | 2,002,694 | | | |
| 500,000 upserts, one call per row, rows/s | 36,051 | did not finish | 329,166 | 243,167 |
| Flush of a 250,000-row buffer | 0.231 s | 1.124 s | 0.194 s | 0.092 s (database file) |
| Sum and average of 1,000,000 rows | 2.6 ms in memory | 7.9 ms over its files | | |
| DuckDB grouped scan of the files each wrote, median | 12.8 ms | 15.0 ms | 17.8 ms | |

Ingest runs at 1.8 times PyArrow, 3.5 times SQLite and 6.8 times DuckDB. A columnar batch of upserts runs at 2 million rows a second, 6 times PyArrow's Python dict and 8 times SQLite. The files are as small as the others (3.95 MB for the flush against 3.66 for DuckDB and 3.96 for PyArrow), and DuckDB scans them faster than its own. PyArrow takes about 16% less time to flush with Snappy, and anything that crosses from Python once per row, such as a single `upsert` or `delete`, is limited by the cost of that call.

**This is not what 0.1.0 on PyPI does yet.** The 0.1.0 library read native memory through an API that costs about 300 ns per access when compiled ahead of time: it ingested 21,017 rows a second, flushed in 9.9 s and summed a million rows in 922.6 ms. The fix is in this repository and goes out in the next release. [docs/benchmarks.md](docs/benchmarks.md) has both runs.

## Beyond one process

Everything above works in one process. A durable graph can go further, and each step builds on the one before it.

| You want | NodusDB does |
|---|---|
| Survive a crash | A checksummed write-ahead log and snapshots. Recovery is checked against a simulated disk with a torn write at every byte |
| A copy outside the machine | Ships the log to S3 or any S3-compatible store as a signed, hash-linked chain, and can project it into an Iceberg table |
| Read replicas and a spare writer | Followers rebuild the graph from the bucket and verify every object against your public key. Restore and takeover continue the chain |
| Forget a person | `erase` scrubs memory, the directory, the bucket and the Iceberg table, and `wait_erased` tells you when it is done |
| React when something changes | `changes` and `watch` stream committed changes; `AccessWatch` tells you whose access flipped |
| See the past | `as_of(token or time)` opens a read-only copy of the graph as it stood then |

[docs/operations.md](docs/operations.md) has the details, the failure behavior and the costs.

## How it works, briefly

- A node's neighbors are a **sparse set**: a packed array plus an index, so insert, delete and membership take constant time, and a scan reads one array in order.
- The index is an **open-addressing hash table with backward-shift deletion**, so churn never leaves tombstones behind.
- A node with fewer than 16 neighbors uses a fixed block in a shared slab. A node with more gets its own set. The switch back waits until degree 8, so a node near the boundary does not thrash.
- Everything lives in **native memory** in chunks that double and never move. The garbage collector has nothing to scan, and a memory budget can refuse a write before it happens.
- **One writer and many readers**, with readers using a sequence counter and retrying if a write overlapped them. Reads take no lock.
- The lake buffer is **a column per field** with a hash index from key to row, and deletes swap the last row into the gap.
- The kernel is Java compiled with GraalVM Native Image into a shared library. Python calls it through `ctypes`. No JVM runs in your process.

[docs/how-it-works.md](docs/how-it-works.md) builds each of these from first principles. [docs/architecture.md](docs/architecture.md) has the byte layouts.

## Compatibility

This is what 0.1.x promises. A golden graph directory in the test suite pins the first two items: a test opens it with the current code on every build.

- **Graph directories.** The on-disk format does not change within 0.1.x. A directory written by any 0.1.x release opens in every later 0.1.x release. A release that meets a newer format refuses to open it and leaves the files alone.
- **Directories from 0.1.0rc1** are refused until `nodusdb.upgrade(path)` converts them. The upgrade keeps the originals in `pre-v2/` and cannot be undone.
- **The Python API.** The names and signatures in these docs stay in 0.1.x. Releases may add new ones. A change that breaks an existing call waits for 0.2.
- **Not covered yet.** The signed chain in the bucket and the Iceberg table carry their own format versions, but this promise does not cover them.

## Limits

- **One writer.** Writes on one handle run one at a time. Readers run in parallel.
- **Failover is manual.** Nothing decides by itself that the writer is dead.
- **Checkpoints pause writes** while they write the snapshot, about 9 seconds at 30.6 million edges.
- **Recovery is not sub-second** at that size. It takes about 3 seconds in a fresh process.
- **0.1.0 on PyPI is slower than this repository.** Its native library reads memory through an API that costs about 300 ns per access, so its lake ingest and flush are 40 to 50 times slower and its graph inserts about 5 times slower than the numbers above. The next release carries the fix.
- **No compaction in the lake**, and reads that reach committed files decode whole files.
- **The schema language has unions only.** Intersection and exclusion are not built.
- **Each Python call costs a few microseconds.** Use the batch forms for bulk work.
- **All benchmark numbers come from one laptop.** CI checks that the code builds and passes on three systems. It does not measure speed.
- **No wheels yet** for Alpine (musl), Linux with glibc older than 2.34, or Windows on ARM.

[TODOS.md](TODOS.md) lists each open item with its reason.

## Contributing

NodusDB is small enough to read in a weekend, and it needs more people than one. [CONTRIBUTING.md](CONTRIBUTING.md) covers how to build it, the rules the code follows, and where help is wanted. The shortest path in is [docs/how-it-works.md](docs/how-it-works.md), then one of the open items in [TODOS.md](TODOS.md).

```sh
git clone https://github.com/Pragadeesh-19/nodusdb && cd nodusdb
mvn test                       # the Java suite; no GraalVM needed
```

## Documentation

[docs/README.md](docs/README.md) is the index. In short:

- Using it: [graph](docs/graph.md), [permissions](docs/permissions.md), [lake](docs/lake-guide.md), [operations](docs/operations.md), [benchmarks](docs/benchmarks.md).
- Understanding it: [how it works](docs/how-it-works.md), [architecture](docs/architecture.md), [decisions](docs/decisions.md).
- Working on it: [CONTRIBUTING](CONTRIBUTING.md), [building and testing](docs/building.md), [TODOS](TODOS.md), [CHANGELOG](CHANGELOG.md).

## License and status

Licensed under the [Apache License, Version 2.0](LICENSE).

Status: 0.1.0, Beta. These are built and tested in CI on Linux, macOS and Windows: the graph engine, durability, typed tuples and permission checks, the reachability index for nested groups, log shipping with followers, restore and takeover, erasure, change watching, time travel, the Iceberg projection and the lake write path. Still open: more than one writer, compaction and the Arrow export for the lake, and wheels for Alpine, older Linux and Windows on ARM.
