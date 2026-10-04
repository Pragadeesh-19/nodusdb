# NodusDB

[![CI](https://github.com/Pragadeesh-19/nodusdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Pragadeesh-19/nodusdb/actions/workflows/ci.yml)

NodusDB is an engine that runs inside your process. It has two parts, built from the same low-level pieces:

- A **graph engine** that holds a directed graph in memory, answers traversal queries in microseconds, and can persist itself to a directory with a write-ahead log.
- A **lakehouse delta engine**, called Spillway, that absorbs streaming writes and commits them as Parquet files that Apache Iceberg and DuckLake can read.

Neither part starts a server or makes a network call. Both keep their hot paths free of garbage collector work.

The last CI run on `main` passed on Ubuntu, macOS (arm64), and Windows. That run covers the lake write path and the graph engine before durability was added. The durability work is verified locally, and its CI run is pending until it is pushed.

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
| Checkpoint of the full graph | 3.7 to 5.6 s, writes a 250 MB snapshot |
| Recovery in a fresh process, snapshot plus 500,000 log frames | 22.0 s |
| Replay of 500,000 log frames alone | 0.13 s |

After recovery, the degrees, in-degrees, and 3-hop results match the state before the crash. The benchmark compares a digest of all of them.

Two rows need reading with care. Batch calls group-commit: the log waits for the disk once per batch of frames, so a synchronous batch runs at nearly the asynchronous rate. A single call in synchronous mode waits for its own disk flush, and on this laptop's Windows disk that costs about 3.6 ms. An NVMe drive with a fast flush path would be much quicker. The benchmark measured this laptop, so the number belongs to it.

## The lakehouse delta engine

Open table formats such as Apache Iceberg and DuckLake store data as immutable Parquet files, and every commit adds more of them. A writer that commits every few seconds produces thousands of small files a day, and each query then has to open all of them.

Spillway sits in front of the table. It takes one upsert or delete at a time, keeps the rows in columns in memory, and commits them in batches sized for the lake. Reads see every accepted write right away, whether the row is still in memory or already committed.

What it does today:

- Upserts and deletes by key. A delete writes a tombstone, so an older committed row stays hidden after a flush.
- Two buffers. Writers keep going while the previous buffer is written to Parquet in the background.
- A Parquet v1 writer that depends on nothing beyond the JDK. Doubles are stored as raw bits, so NaN payloads and negative zero survive the round trip.

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

## Quickstart

Install the Python package from a checkout. It needs the native library, so build it or point `NODUSDB_LIBRARY` at a copy. See [Building](#building).

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

`flush_rows` sets how many rows a buffer holds before it is written out. `flush_interval`, in seconds, commits on a timer. Both triggers run inside the native library. `add_edges_from` also takes a flat buffer such as `array('q', [1, 2, 2, 3])`, which is the fastest input form.

## Durability

A durable graph lives in one directory with three files:

- `nodus.wal` is the write-ahead log. It starts with a 16-byte header: the ASCII magic `NODU`, a version, two reserved bytes, and a creation time. Every accepted edge change then adds a fixed 24-byte frame.
- `snapshot.bin` is the last checkpoint. It stores each node's outgoing edges, with a header and a CRC32 over the whole body. Incoming edges are not stored, because the loader rebuilds them from the outgoing side.
- `nodus.lock` holds a file lock, so a second process or a second handle cannot open the same directory.

A frame is 24 bytes. It holds the operation (add or remove), one reserved byte, two zero bytes, a CRC32 over the operation and both node IDs, and the two IDs. The layout keeps every frame aligned to eight bytes, and the log is only ever appended to. Nothing is overwritten in place.

**Writing.** An edge change first checks whether it does anything. A duplicate add or a missing remove is a no-op and writes nothing. A real change goes into the log buffer before the in-memory graph changes. A background thread writes the buffer to disk, either every 10 milliseconds in `async` mode or as each call requires in `sync` mode.

**Checkpoint.** A checkpoint writes the snapshot to a temporary file, forces it to disk, renames it into place, and then replaces the log with an empty one. The writer pauses for the duration. A crash between the rename and the log replacement is safe: replaying the old log over the new snapshot gives the same final state, because the last operation on each edge decides whether it exists.

**Recovery.** On open, the process deletes any half-written temporary files, loads the snapshot if one exists, and then replays the log. A frame with a bad checksum or only part of its bytes at the end of the file ends the replay. The file is then cut back to the last good frame. The damage is reported as truncated bytes, and the replay does not continue past it.

### What survives what

| Failure | `async` | `sync` (single calls) |
|---|---|---|
| The process is killed | Everything the OS has accepted survives. A write still in the buffer is lost | Every acknowledged write survives |
| The machine loses power | Up to the last 10 ms of writes can be lost | Every acknowledged write survives |

Call `sync()` on the graph, or `checkpoint()`, to force a point where everything is on disk. The Python wrapper currently exposes `checkpoint()`.

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

The durability work was verified locally on Windows:

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

# Comparison against NetworkX and SQLite (several minutes)
python python/benchmarks/compare.py

# JMH microbenchmarks
mvn -pl bench -am package -DskipTests
java -jar bench/target/benchmarks.jar -prof gc
```

The Python loader looks in `NODUSDB_LIBRARY`, then `python/nodusdb/bin/<os>-<arch>/`, then `target/native/`. CI copies the library into `bin/`. If you rebuild `target/native/` by hand, copy it into `bin/` too, or the loader keeps using the old copy.

## Limits

- **Recovery is linear in graph size.** At 30.6 million edges, a fresh process needs 22 seconds, and nearly all of it is loading the snapshot. The log replays fast: 500,000 frames take 0.13 seconds. Sub-second recovery of a graph this size would need the in-memory layout stored directly, which the snapshot does not do.
- **Synchronous single calls are disk-bound.** On this laptop each one waits about 3.6 ms for its flush. Use batch calls for bulk loads.
- **One writer at a time.** Use a graph handle or a lake table from one thread at a time. Lake writes are serialized by a lock. The graph kernel is not synchronized, so concurrent readers are not supported yet.
- **Integer node IDs.** Node IDs are `long` values from 0 to `Integer.MAX_VALUE - 9`. Map external identifiers to integers first.
- **Checkpoints pause writes.** A checkpoint writes the whole graph while the writer waits. It took 3.7 to 5.6 seconds for 30.6 million edges.
- **Lake reads decode whole files.** A `get` that reaches committed data reads and decodes each file it checks. A key-column cache is the next step.
- **No compaction.** Superseded rows and tombstones stay in the files until something removes them.
- **Python call cost.** Each `ctypes` call takes a few microseconds. Use batch calls for bulk work.
- **One laptop.** The benchmark numbers come from one machine. CI checks that the code builds and passes on three systems. It does not measure speed.

## License and status

Licensed under the [Apache License, Version 2.0](LICENSE).

Status: the graph engine (Rings 1 to 3) is built, and durability (write-ahead log, checkpoint, and recovery) is built on top of it. The lake write path is built. Ring 4's concurrency work, a seqlock for lock-free reads, has not started. Iceberg metadata, compaction, and the Arrow export are still open.
