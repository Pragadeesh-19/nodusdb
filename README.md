# NodusDB

[![CI](https://github.com/Pragadeesh-19/nodusdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Pragadeesh-19/nodusdb/actions/workflows/ci.yml)

NodusDB is an engine that runs inside your process. It has two parts, built from the same low-level pieces:

- A **graph engine** that holds a directed graph in memory, answers traversal queries in microseconds, and can persist itself to a directory with a write-ahead log.
- A **lakehouse delta engine**, called Spillway, that absorbs streaming writes and commits them as Parquet files that Apache Iceberg and DuckLake can read.

Neither part starts a server or makes a network call. Both keep their hot paths free of garbage collector work.

The last CI run on `main` passed on Ubuntu, macOS (arm64), and Windows. It built the GraalVM native library, ran 254 Java tests and 43 Python tests against that library, and included the durability tests.

## The graph engine

Use it for state that changes all the time: agent working memory, live permission graphs, session graphs. Edges come in and go out constantly, and a query has to finish before the next event arrives.

These results come from `python/benchmarks/compare.py`. The run loads 1,000,000 directed edges over 200,000 nodes, deletes 500,000 of them, and then runs 200 queries of each kind. Every backend is checked for identical answers before timing starts.

| Configuration | Insert edges/s | Delete edges/s | 3-hop median | Common neighbors median |
|---|---:|---:|---:|---:|
| NodusDB, flat `array('q')` | 1,246,720 | 1,331,386 | 10.5 µs | 7.1 µs |
| NodusDB, batch of tuples | 462,169 | 522,139 | 11.0 µs | 7.3 µs |
| NodusDB, single-edge calls | 203,442 | 202,660 | 11.7 µs | 7.2 µs |
| NetworkX, batch | 149,633 | 299,071 | 36.0 µs | 6.6 µs |
| SQLite, `executemany` | 124,846 | 123,609 | 113.7 µs | 22.5 µs |
| SQLite, one statement per row | 100,403 | 96,992 | 152.0 µs | 22.3 µs |

From the same run:

- Loading through the flat buffer is about 8 times faster than NetworkX's batch insert. Tuple batches are about 3 times faster.
- The 3-hop median is about 11 times faster than SQLite's batched form, and about 14 times faster than SQLite with one statement per row.
- NetworkX was slightly faster on common neighbors (6.6 µs against 7.3 µs). Treat that query as a tie.

The in-process JMH run measures a different graph and gives a 3-hop median of 15.2 µs. The raw data is in [`bench/baseline/windows-dev.json`](bench/baseline/windows-dev.json). The machine and the exact command are in [`bench/baseline/MACHINE.md`](bench/baseline/MACHINE.md).

These are single runs on a laptop CPU that boosts and throttles, so absolute throughput moved by a wide margin between runs during development. The ordering held in every run except the common-neighbors tie.

### Durability on a real graph

The durable benchmark uses the SNAP soc-Pokec graph: 30,622,564 directed edges. It is a real social network, not generated data. Each phase runs in its own process with a 6 GB heap. The full commands and the digests are in [`bench/baseline/durability-windows-dev.txt`](bench/baseline/durability-windows-dev.txt).

| Phase | Result |
|---|---|
| In-memory ingest, batches of 1,048,576 | 1,252,289 edges/s |
| Durable ingest, asynchronous log | 1,038,823 and 1,112,830 edges/s across two runs |
| Durable ingest, synchronous log, batches | 1,167,272 edges/s |
| Durable ingest, one call per edge, asynchronous | 1,454,562 edges/s |
| Durable ingest, one call per edge, synchronous | 203 edges/s (median 3.6 ms, p99 34 ms per call) |
| Checkpoint of the full graph | 8.7 to 9.1 s, writes a 500 MB snapshot |
| Recovery in a fresh process, snapshot plus 500,000 log frames | 2.8 s (22 to 29 s before the parallel loader) |
| Replay of 500,000 log frames alone | 0.13 s |

After recovery, the degrees, in-degrees, and 3-hop results match the state before the crash. The benchmark compares a digest of all of them.

Two rows need reading with care. Batch calls group-commit: the log waits for the disk once per batch of frames, so a synchronous batch runs at nearly the asynchronous rate. A single call in synchronous mode waits for its own disk flush, and on this laptop's Windows disk that costs about 3.6 ms. An NVMe drive with a fast flush path would be much quicker. The benchmark measured this laptop, so the number belongs to it.

## The lakehouse delta engine

Open table formats such as Apache Iceberg and DuckLake store data as immutable Parquet files, and every commit adds more of them. A writer that commits every few seconds produces thousands of small files a day, and each query then has to open all of them.

Spillway sits in front of the table. It takes one upsert or delete at a time, keeps the rows in columns in memory, and commits them in batches sized for the lake. Reads see every accepted write right away, whether the row is still in memory or already committed.

What it does today:

- Upserts and deletes by key. A delete writes a tombstone, so an older committed row stays hidden after a flush.
- Two buffers. Writers keep going while the previous buffer is written to Parquet in the background.
- A Parquet v1 writer and reader that depend on nothing beyond the JDK. Pages are Snappy-compressed or uncompressed, low-cardinality columns are dictionary-encoded, and every column carries min and max statistics. Doubles are stored as raw bits, so NaN payloads and negative zero survive the round trip.

Numbers from `LakeTableBench` and `DeltaMemTableBench` on the same laptop:

| Case | Result |
|---|---|
| Upsert into memory, no flush running | 227 ns per upsert, about 4.4 million per second |
| Upsert while buffers flush continuously (65,536-row buffers) | 998 ns per upsert, about 1.0 million per second |
| Allocation on the ingestion thread during flush cycles | 0.0014 bytes per upsert after warm-up |
| Flush of one 65,536-row buffer to Parquet | about 70 ms |

The second row is the realistic figure under sustained load. Once two buffers are full and the previous flush is still running, the writer waits. The flush rate then sets the ingestion rate.

The Parquet output matched PyArrow bit for bit on 100,000 mixed rows. The CI image does not install `pyarrow`, so that test skips there.

Not built yet: Iceberg table metadata and Avro manifests (the manifest is Iceberg-shaped, but it is local), compaction, the Arrow C Data export, and reads that do not decode a whole file. [`docs/lake.md`](docs/lake.md) has the design and the open items.

## Against other engines

Two suites compare NodusDB with other engines on real data. The graph suite runs on the SNAP soc-Pokec social network, 30,622,564 directed edges. The lakehouse suite runs on the first 1,000,000 rows of the NYC TLC Yellow Taxi file for January 2024. Each engine and workload runs in its own process. The answers are cross-checked: the permission digests, the Pokec hub degrees, and the taxi revenue total agree across every engine that finished. Each number is a single run on the same laptop as the rest of this file. The raw output is in `bench/baseline/`. The graph suite takes about 2.5 hours and the lake suite about 20 minutes.

### Graph suite

- **A, load and traverse.** Load all 30.6 million Pokec edges. Then time 2-hop and 3-hop queries from 100 median-degree nodes and from 50 hub nodes.
- **B, churn.** Preload 200,000 edges among the top 50,000 Pokec nodes. Run 100,000 operations: 60% inserts, 30% deletes, and 5% each of 2-hop and 3-hop queries. Each engine gets a 1,200-second budget.
- **C, permissions.** A synthetic relationship-based access graph with 10,000 users, 2,000 groups, 50,000 resources, and 81,500 membership edges. Then 9,936 membership updates and 10,000 reachability checks. About half the checks succeed. This workload is generated, not real data.

Times are medians unless noted.

**Workload A, Pokec, 30.6 million edges**

| Engine | Ingest, edges/s | 2-hop | 3-hop | Hub 2-hop | Hub 3-hop | Peak RSS, MB |
|---|---:|---:|---:|---:|---:|---:|
| NodusDB, in memory | 181,345 | 56.8 µs | 1.21 ms | 6.8 ms | 96.6 ms | 4,404 |
| NodusDB, durable (async) | 163,841 | 129.8 µs | 1.25 ms | 8.9 ms | 101.6 ms | 4,240 |
| igraph | 92,916 | 0.84 ms | 7.50 ms | 12.0 ms | 106.4 ms | 2,464 |
| DuckDB | 158,027 | 13.5 ms | 68.0 ms | 58.0 ms | 766.6 ms | 3,420 |
| SQLite | 19,052 | 0.95 ms | 49.8 ms | 174 ms | 12.9 s | 917 |

On disk after ingest, the durable graph takes 258 MB, SQLite 822 MB, and DuckDB 1,458 MB. Kùzu did not finish its bulk load, and NetworkX is skipped above 3 million edges.

**Workload B, churn, 100,000 operations**

| Engine | Operations/s | Insert | Delete | 2-hop | 3-hop | Operations done |
|---|---:|---:|---:|---:|---:|---:|
| NodusDB, in memory | 81,525 | 4.7 µs | 5.3 µs | 12.4 µs | 26.3 µs | 100,000 |
| NodusDB, durable (async) | 73,773 | 6.0 µs | 6.7 µs | 12.6 µs | 27.1 µs | 100,000 |
| NetworkX | 55,451 | 4.6 µs | 4.6 µs | 31.1 µs | 106.5 µs | 100,000 |
| SQLite | 3,617 | 86.5 µs | 89.3 µs | 141 µs | 448 µs | 100,000 |
| DuckDB | 164 | 4.70 ms | 6.27 ms | 11.5 ms | 16.3 ms | 100,000 |
| Kùzu | 60 | 9.85 ms | 21.4 ms | 24.3 ms | 29.4 ms | 71,758 |
| igraph | 11 | 99.2 ms | 102.2 ms | 303 µs | 458 µs | 12,898 |

**Workload C, permission checks**

| Engine | Check | Update | Wall time, with load |
|---|---:|---:|---:|
| NodusDB, in memory | 38.0 µs | 9.5 µs | 2.0 s |
| NodusDB, durable (async) | 38.8 µs | 12.1 µs | 2.2 s |
| NetworkX | 171 µs | 7.4 µs | 5.0 s |
| SQLite | 400 µs | 98.9 µs | 7.4 s |
| Kùzu | 10.8 ms | 27.9 ms | 401 s |
| DuckDB | 11.9 ms | 9.56 ms | 221 s |
| igraph | 571 µs | 68.5 ms | 726 s |

#### Where NodusDB wins

- **High-churn mutation.** Workload B runs at 81,525 operations per second. The next fastest engines are NetworkX at 55,451 and SQLite at 3,617. An in-memory edit takes about 5 µs, and an igraph edit takes about 100 ms.
- **Traversal on a graph held in memory.** On Pokec, the median 2-hop query takes 57 µs and the median 3-hop query takes 1.2 ms. Every other engine that finished workload A is slower on both: igraph takes 0.84 ms and 7.5 ms, SQLite 0.95 ms and 49.8 ms, DuckDB 13.5 ms and 68.0 ms. On hub 3-hop queries, NodusDB and igraph are close, at 96.6 ms and 106.4 ms.
- **Permission checks.** The median check takes 38 µs. NetworkX takes 171 µs and SQLite 400 µs.
- **Ingest and footprint.** The in-memory graph loads faster than DuckDB or igraph. The durable graph loads about 10% slower than in memory and uses less disk than either SQLite or DuckDB.

#### Where the other engines win

- **Single updates in workload C.** NetworkX applies a membership update in 7.4 µs, against 9.5 µs for NodusDB. In workload B the median insert is a tie, 4.6 µs against 4.7 µs.
- **Whole-graph analytics.** No workload here reads every node and edge, as PageRank or connected components do. Columnar engines such as Kùzu and DuckDB are built for that work, and this suite does not measure it.

#### Caveats

- Kùzu's bulk load failed in workload A with `Buffer manager exception: ... The buffer pool is full`. It ran at default settings everywhere. I have not tuned the buffer pool, so its numbers may change with configuration.
- Kùzu and igraph hit the 1,200-second budget in workload B. Their rates count only the operations they completed.
- Peak RSS in workload A includes the Pokec data that each worker loads. Use the on-disk figures to compare footprint.
- Traversal on Pokec is not sub-microsecond. The design targets sub-microsecond k-hop queries, and this suite does not reach that target. The smaller benchmarks above (10 to 15 µs) do not reach it either.

### Lakehouse suite

The lake suite writes the taxi rows through each engine, keyed by row index. Four workloads:

- **W1, ingest.** Load all 1,000,000 rows.
- **W2, mutations.** 500,000 upserts and 100,000 deletes. NodusDB runs the upserts two ways: one call per row, and one columnar batch.
- **W3, flush.** Write a 250,000-row buffer to Parquet.
- **W4, read.** A DuckDB query totals revenue and trips over the files each engine wrote. Every source gives the same revenue total, 27,486,629.10.

| Measure | NodusDB | DuckDB | PyArrow | SQLite |
|---|---:|---:|---:|---:|
| W1 ingest, rows/s | 846,137 | 164,793 | 114,582 | 95,113 |
| W2 upserts/s, one call per row | 39,335 | 960 | 393,661 | 253,594 |
| W2 upserts/s, columnar batch | 1,763,882 | | | |
| W2 deletes/s | 467,542 | 930 | 755,709 | 269,022 |
| W3 flush of 250,000 rows, Snappy | 0.265 s | 1.143 s | 0.187 s | 0.123 s, database file |
| W3 flush of 250,000 rows, no compression | 0.187 s | | | |
| W4 grouped scan, median | 12.8 ms | 14.0 ms | 17.7 ms | n/a |

Each engine's W4 row is DuckDB reading that engine's own files. The W4 layout of NodusDB wrote three files. The full tables, with memory, file sizes, and the 5,000-row streaming layout, are in [`docs/lake.md`](docs/lake.md). The raw output is in [`bench/baseline/lake-heavyweights-windows-dev-v2.json`](bench/baseline/lake-heavyweights-windows-dev-v2.json).

#### Where NodusDB wins

- **Bulk ingest.** 5.1 times DuckDB and 7.4 times PyArrow.
- **Batch upserts.** 1.8 million rows per second. That is about 4.5 times PyArrow's Python dict, about 7 times SQLite, and about 1,800 times DuckDB's per-row updates.
- **Keyed deletes.** 1.7 times SQLite and about 500 times DuckDB.
- **Scans of its own output.** The grouped scan has a 12.8 ms median, against 14.0 ms for DuckDB's own files and 17.7 ms for PyArrow's.
- **Native aggregates.** A sum and an average over 1,000,000 rows held in memory take 4.0 ms. DuckDB takes 7.7 ms for the same ungrouped query over its own files. The two are not the same workload, because the native call does not read files.

#### Where the analytical engines win

- **Single-call upserts.** PyArrow runs about 10 times faster, and SQLite about 6 times faster. Each call pays Python overhead that I have not profiled.
- **Flush with Snappy.** PyArrow takes 0.187 s, and NodusDB takes 0.265 s. With compression off, NodusDB ties PyArrow at 0.187 s, but its files are 36% larger.
- **Deletes into a Python dict.** PyArrow is 1.6 times faster, but it has no storage behind it, so this is not a comparable engine.
- **Many small files.** The 5,000-row streaming layout scans at 25.5 ms, about twice the clean layout. Each file is a separate scan task, and compaction is not built.

#### Caveats

- W1 and the single-call W2 path pass each row from Python into the native library, so they include that cost. The columnar batch path does not.
- The native aggregate covers only the rows in the active buffer. It does not read committed files.
- The clean W4 layout wrote three files, not four. Ingest now outruns the flush, so the buffer grows past its freeze threshold before it freezes.
- The W4 p99 for the clean layout is 35.9 ms, against 15.3 ms for DuckDB's own files. The median is faster, but the tail is worse.
- Each engine ran once, on one laptop.

## Quickstart

Install the Python package from a checkout. It needs the native library, so build it or point `NODUSDB_LIBRARY` at a copy. See [Building](#building). Release wheels install with `pip install nodusdb` and need no compiler or GraalVM. The release workflow builds them, and they become available once a release is published.

```sh
pip install -e python/
```

Graph in memory:

```python
import nodusdb

g = nodusdb.Graph()
g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])

print("Reachable nodes:", g.khop(start=1, max_depth=3))
print("Common neighbors:", g.common_neighbors(3, 5))

g.close()
```

```
Reachable nodes: [2, 5, 3, 4]
Common neighbors: [4]
```

Graph with durability. The graph recovers from the directory when it opens, and each edit is logged:

```python
import nodusdb

g = nodusdb.Graph(path="/data/graph", sync_mode="async")
g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])
g.checkpoint()
g.close()  # writes a final snapshot and releases the directory lock

g = nodusdb.Graph(path="/data/graph")
print("Reachable after restart:", g.khop(start=1, max_depth=3))
g.close()
```

`sync_mode="sync"` makes each single-edge call wait until its log entry is on disk. The default, `"async"`, batches writes every 10 milliseconds. Batch calls group-commit in either mode.

String and UUID keys. A node can be named by a string or a `uuid.UUID` instead of an integer:

```python
import uuid
import nodusdb

g = nodusdb.Graph()
g.add_edge("user:alice", "role:admin")
g.add_edge(uuid.uuid4(), "role:admin")
print(g.khop("user:alice", 1))  # ['role:admin']
```

A graph uses one kind of key. The first key type written claims the graph, and a durable graph keeps the claim across restarts. Mixing kinds raises `TypeError`. Strings are stored once in a symbol table and mapped to dense integer ids, so the kernel still runs on integers. Reads never add a key: an unknown key answers as absent. The symbol table is forced to disk before any edge that uses a new key is written. A single call that adds new keys therefore costs one sync, and `add_edges_from` pays that cost once per batch.

Lakehouse writes:

```python
import nodusdb

schema = {"amount": "int64", "score": "float64", "status": "int32", "label": "utf8"}

with nodusdb.LakeTable("/data/orders", schema, flush_rows=100_000) as table:
    table.upsert("order-101", {"amount": 5000, "score": 0.95, "status": 1, "label": "cleared"})
    table.delete("order-099")
    print(table.get("order-101"))
    table.flush()  # writes data-*.parquet and delete-*.parquet into /data/orders
```

`flush_rows` sets how many rows a buffer holds before it is written out. `flush_interval`, in seconds, commits on a timer. Both triggers run inside the native library. `compression` is `"snappy"` by default, or `"none"`.

Arrow batches go in without a Python object per row. Pass a `pyarrow` table or record batch with an `int64` key column, and read sums or averages over the rows still in memory:

```python
import pyarrow as pa

with nodusdb.LakeTable("/data/orders", schema) as table:
    table.upsert_table(pa.table({"id": ids, "amount": amounts, "score": scores,
                                 "status": statuses, "label": labels}), key="id")
    print(table.sum("amount"), table.average("score"))
```

`add_edges_from` also takes a flat buffer such as `array('q', [1, 2, 2, 3])`, which is the fastest input form.

## Durability

A durable graph lives in one directory with four files:

- `nodus.wal` is the write-ahead log. It starts with a 16-byte header: the ASCII magic `NODU`, a version, two reserved bytes, and a creation time. Every accepted edge change then adds a fixed 24-byte frame.
- `snapshot.bin` is the last checkpoint. It stores each node's outgoing edges, then each node's incoming edges, with a header and a CRC32 over the whole body. Version 2 is written now. Version 1 files, which hold outgoing edges only, still load. Version 2 files cannot be read by builds that predate this change.
- `symbols.nodus` holds the string and UUID keys, in the order they were first used. Each record carries a CRC32C checksum. A key is forced to disk before any edge that uses it is written. The header also records whether the graph is keyed by integers or by strings.
- `nodus.lock` holds a file lock, so a second process or a second handle cannot open the same directory.

A frame is 24 bytes. It holds the operation (add or remove), one reserved byte, two zero bytes, a CRC32 over the operation and both node IDs, and the two IDs. The layout keeps every frame aligned to eight bytes, and the log is only ever appended to. Nothing is overwritten in place.

**Writing.** An edge change first checks whether it does anything. A duplicate add or a missing remove is a no-op and writes nothing. A real change goes into the log buffer before the in-memory graph changes. A background thread writes the buffer to disk, either every 10 milliseconds in `async` mode or as each call requires in `sync` mode.

**Checkpoint.** A checkpoint writes the snapshot to a temporary file, forces it to disk, renames it into place, and then replaces the log with an empty one. The writer pauses for the duration. A crash between the rename and the log replacement is safe: replaying the old log over the new snapshot gives the same final state, because the last operation on each edge decides whether it exists.

**Recovery.** On open, the process deletes any half-written temporary files, loads the snapshot if one exists, and then replays the log. A frame with a bad checksum or only part of its bytes at the end of the file ends the replay. The file is then cut back to the last good frame. The damage is reported as truncated bytes, and the replay does not continue past it. The symbol file follows the same rule for a torn final record. A corrupt record anywhere else refuses to open, because an edge could then refer to the wrong key. A test writes 100,000 edges, appends an incomplete 14-byte record, and reopens: every edge comes back and the torn bytes are removed.

### What survives what

| Failure | `async` | `sync` (single calls) |
|---|---|---|
| The process is killed | Everything the OS has accepted survives. A write still in the buffer is lost | Every acknowledged write survives |
| The machine loses power | Up to the last 10 ms of writes can be lost | Every acknowledged write survives |

Call `sync()` or `checkpoint()` to force a point where everything is on disk.

## Concurrency

The kernel supports one writer and any number of readers. Every mutation runs inside a seqlock: a version counter is odd while an edge change is in progress and even otherwise. A reader checks the counter before and after each query. If a write overlapped the query, the reader discards the result and runs it again. Readers never take a lock and never block the writer, and a query never returns a half-applied change.

The stress test in `GraphConcurrencyTest` runs one writer that toggles 200,000 edges on a high-degree node while eight readers run `kHop` and `commonNeighbors`. Every result must hold distinct, in-range targets. The test also checks that the readers allocate nothing after the writer stops. Breaking the validation step makes it fail with a duplicated target.

The Python `Graph` and `LakeTable` objects serialize their calls with a lock, so threads may share one object. Each OS thread runs its native calls on its own GraalVM isolate thread. Every C-ABI call on a graph handle also takes that handle's monitor, so threads may share a handle. Calls on one handle run one at a time, so readers do not yet run in parallel within a handle. `python/tests/test_concurrency.py` covers both Python cases, and `python/tests/test_crash.py` kills a writer with SIGKILL and checks that every acknowledged sync write survives.

## How it works

Both engines rest on the same few ideas. Each section answers a question the previous one leaves open.

### A set as a packed array and an index

Store a set of integers as two arrays. The dense array holds the members back to back. The index maps each member to its position. A lookup takes two reads. An insert appends. A removal moves the last member into the gap and updates that member's index entry, so the dense array never has a hole.

Every operation takes constant time, and a scan reads memory in order, which the CPU prefetcher handles well. Briggs and Torczon described this structure in 1993. The weak point is the index. Indexed directly by value, it needs a slot for every possible value, which wastes space when a node's neighbors are spread across a large ID range.

### A hash table with backward-shift deletion

So the index is an open-addressing hash table. A key hashes to a starting slot, and a lookup scans forward until it finds the key or an empty slot. Three choices keep it fast:

- The hash is the 64-bit finalizer from MurmurHash3. Consecutive IDs spread across the table, so sequential loads do not form long runs.
- The table doubles once it is half full. Runs stay short, and a lookup usually reads one or two slots.
- Deletion uses backward shift (Knuth, *The Art of Computer Programming*, volume 3, Algorithm R) instead of tombstones. Tombstones pile up under churn and slow every later probe. After a removal, the entries that follow move back into the gap whenever their probe path crosses it.

The shift test must use cyclic distance. The table wraps, so a run can cross the last slot and continue at slot 0. A plain comparison of slot numbers loses keys in that case, with no error raised. The test suite covers this case.

### Small nodes and large nodes

Most nodes in a real graph have a few neighbors, and a few hubs have thousands. A hash table per node wastes memory on the small ones.

- A node with fewer than 16 neighbors keeps them in a fixed block inside one shared `long[]` slab. Because the slab is a single array, the garbage collector does not track per-node objects.
- A node with 16 or more neighbors switches to the packed array and hash table described above.

A node switches back only when its degree falls to 8. A node whose degree hovers near 16 does not reallocate on every change.

Each graph keeps an outgoing table and an incoming table, and every edge edit updates both. Both use the same tiers, so in-degree costs the same as out-degree.

### Columns for the lake

The lake buffer applies the same idea in a different shape. Each column is its own primitive array, and row *n* is the *n*th entry in every array. Text goes into one shared byte slab, with an offset and a length for each row. A Parquet column is a sequential copy, because the buffer already holds its values in that order.

A delete swaps the last row into the gap across every column, updates the index entry for the moved key, and then removes the deleted key. A delete that must survive a flush writes a tombstone instead.

### No allocation on the hot path

Queries and in-tier edits allocate nothing once their structures exist. Allocation happens in three places: a node changes tier, an array grows, or a buffer is replaced. The lake reuses its buffers. After a flush, the frozen buffer is cleared and reused, so its arrays keep their grown size and the ingestion thread stops allocating.

The graph allocation test reads the thread's allocated-bytes counter. Across the JMH baseline, the allocation rate stayed near 0.001 MB/s in every iteration. The first measured window after warm-up allocates a fixed 72 to 96 bytes, which the test records as a known exception (decision D12).

### Crossing into Python

The Java code compiles to a native shared library with GraalVM Native Image, so Python does not need a JVM. Each exported function takes a GraalVM isolate thread, which GraalVM requires. Functions return sentinel values such as `false` or `-1` instead of throwing, so an exception cannot escape into the host process.

Results are written into caller-supplied buffers, and the call returns the full count. A caller that guessed too small can grow the buffer and retry. Batch calls validate the whole batch before they change anything, so one bad ID leaves the graph as it was.

## Verification

The `ci` workflow runs on Ubuntu, macOS (arm64), and Windows for every push. The last run on `main` before durability passed on all three:

- The Java suite ran 232 tests with assertions enabled.
- The GraalVM native library built on each system and was packaged into the Python package.
- The Python suite ran 38 tests against the native library. Four of them exercise the lake. The fifth lake test, which reads the output with PyArrow, skipped in CI because `pyarrow` is not installed there.

The durability work passed the same CI run, and it was also checked locally on Windows:

- The Java suite ran 254 tests, including 22 durability tests. Those cover clean restarts, crashes in both sync modes, torn and corrupted log tails, snapshot rolls, a crash between the snapshot rename and the log reset, corrupted and truncated snapshots, the directory lock, and the no-op rules.
- The native library built with GraalVM CE 22.0.2 and the Python suite ran 43 tests against it.
- The Pokec run was checked by digest, as described above.

The CI build targets JDK 22 with GraalVM CE 22. JDK 25 has not been tested.

## Building

Prerequisites:

- JDK 22 and Maven 3.9 or newer.
- Python 3.9 or newer. The tests ran on 3.12.
- GraalVM CE 22.0.2 with `native-image`, for the native library only.
- On Windows, Visual Studio 2022 with the C++ build tools.

```sh
# Java tests
mvn test

# Native library, written to target/native/
GRAALVM_HOME=/path/to/graalvm-community-openjdk-22.0.2 mvn -Pnative -pl core package -DskipTests

# Python tests, which load the native library
python -m unittest discover -s python/tests -v

# Wheel for the current platform, written to the current directory
python -m pip wheel python/ --no-deps -w dist

# Comparison against NetworkX and SQLite (several minutes)
python python/benchmarks/compare.py

# Multi-engine suites on real data. Install the competitors first.
# The first run downloads the Pokec archive (132 MB) and the taxi file into
# the system temp directory, or into NODUS_BENCH_CACHE if it is set.
pip install -r python/benchmarks/requirements.txt
python python/benchmarks/benchmark_graph_heavyweights.py run --out bench/baseline/graph-heavyweights-windows-dev.json
python python/benchmarks/benchmark_lake_heavyweights.py run --out bench/baseline/lake-heavyweights-windows-dev-v2.json

# JMH microbenchmarks
mvn -pl bench -am package -DskipTests
java -jar bench/target/benchmarks.jar -prof gc
```

The Python loader looks in `NODUSDB_LIBRARY`, then `python/nodusdb/bin/<os>-<arch>/`, then `target/native/`. CI copies the library into `bin/`. If you rebuild `target/native/` by hand, copy it into `bin/` too, or the loader keeps using the old copy.

Release wheels come from `.github/workflows/wheels.yml`. It builds the native library on Linux (x86_64 and arm64), macOS (x86_64 and arm64), and Windows, and then runs cibuildwheel for CPython 3.9 to 3.13. Each wheel is smoke-tested against the installed package. The workflow runs on every push. It publishes to PyPI only when a release is published, through trusted publishing and a required reviewer on the `pypi` environment. Linux wheels are tagged `manylinux_2_34`, so they need glibc 2.34 or newer: Ubuntu 22.04 and later, Debian 12, and RHEL 9. The Linux wheels are installed and smoke-tested on the host runner, because the manylinux2014 build container cannot install a 2.34 wheel. macOS wheels target 11.0 and later. Windows wheels are x86_64 only.

## Limits

- **Recovery is not yet sub-second.** At 30.6 million edges, a fresh process with a 3 GB heap recovers in 2.8 seconds, with the snapshot load taking about 2.2 of them. The live graph occupies about 1.5 GB. Most of the remaining time is building the membership indexes of high-degree nodes, and garbage collection during that build.
- **Synchronous single calls are disk-bound.** On this laptop each one waits about 3.6 ms for its flush. Use batch calls for bulk loads.
- **Calls on one handle are serialized.** The kernel allows one writer and many readers. Python objects and C handles are both safe to share across threads, but calls on one handle run one at a time. Readers do not run in parallel within a handle yet.
- **Node keys.** Integer keys are `long` values from 0 to `Integer.MAX_VALUE - 9`. String and UUID keys are mapped to integers and stored in a symbol table, which is not compacted yet.
- **Checkpoints pause writes.** A checkpoint writes the whole graph while the writer waits. It took 3.7 to 5.6 seconds for 30.6 million edges.
- **Lake reads decode whole files.** A `get` that reaches committed data reads and decodes each file it checks. A key-column cache is the next step.
- **No compaction.** Superseded rows and tombstones stay in the files until something removes them.
- **Python call cost.** Each `ctypes` call takes a few microseconds. Use batch calls for bulk work.
- **One laptop.** The benchmark numbers come from one machine. CI checks that the code builds and passes on three systems. It does not measure speed.

## License and status

Licensed under the [Apache License, Version 2.0](LICENSE).

Status: the graph engine (Rings 1 to 3) is built, and durability (write-ahead log, checkpoint, and recovery) is built on top of it. The lake write path is built. String and UUID keys, the kernel concurrency test, and the release wheel workflow are built. Python `Graph` and `LakeTable` objects can be shared across threads. Calls on one handle are serialized, and readers do not run in parallel within a handle yet. Multiple writers are not. Iceberg metadata, compaction, and the Arrow export are still open.
