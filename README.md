# NodusDB

An embedded, in-process dynamic relationship engine built for high-churn working memory.

NodusDB holds a directed graph inside the process that uses it. It targets workloads where edges are inserted and deleted all the time: agent working memory, live permission graphs, session state. Traversals and edge edits run in local memory, with no server, no disk round trip, and no garbage collector work in steady state.

## Why another graph store

Start with three questions. Can you find an edge quickly? Can you add or remove one without rewriting much? Can you walk outward from a node without stalling on memory? Most embedded graph stores favor one of these and pay for the others.

**Compressed Sparse Row (CSR).** CSR packs every edge into one array, grouped by source node, with an offsets array that says where each group starts. Walking one node's neighbors reads one contiguous range, which is very fast. Writes are the problem. Inserting an edge in the middle shifts every later entry and rewrites the offsets, so the worst case is O(E). Kùzu and its forks use this layout. It suits bulk-loaded analytical data, and it suffers under constant mutation.

**Disk-backed B-trees.** SQLite with recursive common table expressions stores edges as rows in an index. Each hop of a traversal is an index lookup: walk pages, decode rows, follow pointers. Page splits and serialization add cost to every write, and every hop can land on memory the CPU has not touched recently.

**NodusDB.** Every structure lives in memory as flat primitive arrays. The layout is chosen per node by its degree, so a node with three neighbors and a node with three thousand each get a structure that fits. Edge edits are O(1) in expectation, and a traversal reads neighbors from packed arrays instead of chasing pointers.

## Benchmarks

Measured with `python/benchmarks/compare.py` on one machine. The workload is 1,000,000 unique random directed edges over 200,000 nodes, then 500,000 random existing edges deleted, then 200 random start nodes per traversal query. Every configuration is cross-checked for identical answers before timing starts.

Hardware: AMD Ryzen 7 5700U (8 cores, 16 threads), 7.3 GB RAM, Windows 11 Home, CPython 3.12, GraalVM CE 22.0.2, NetworkX 3.6.1, SQLite 3 from the Python standard library.

| Configuration | Insert edges/s | Delete edges/s | 2-hop median | 3-hop median | Common neighbors median |
|---|---:|---:|---:|---:|---:|
| NodusDB, single-edge calls | 203,442 | 202,660 | 8.9 µs | 11.7 µs | 7.2 µs |
| NodusDB, batch of tuples | 462,169 | 522,139 | 8.7 µs | 11.0 µs | 7.3 µs |
| NodusDB, flat `array('q')` | 1,246,720 | 1,331,386 | 8.7 µs | 10.5 µs | 7.1 µs |
| NetworkX, single-edge calls | 116,057 | 272,182 | 16.7 µs | 36.1 µs | 6.7 µs |
| NetworkX, batch | 149,633 | 299,071 | 17.0 µs | 36.0 µs | 6.6 µs |
| SQLite, single-row statements | 100,403 | 96,992 | 70.3 µs | 152.0 µs | 22.3 µs |
| SQLite, `executemany` | 124,846 | 123,609 | 58.0 µs | 113.7 µs | 22.5 µs |

Traversal and common-neighbor columns are medians over 200 queries per configuration.

Batch against batch, from the same run:

- Ingestion: NodusDB is 3.1 times faster than NetworkX and 3.7 times faster than SQLite.
- Deletion: NodusDB is 1.7 times faster than NetworkX and 4.2 times faster than SQLite.
- 3-hop traversal: NodusDB is 3.3 times faster than NetworkX and 10.3 times faster than SQLite.
- Common neighbors: NetworkX is slightly faster in this run (6.6 µs against 7.3 µs).

The flat-buffer row is the fastest way to load data. Passing the same edges as a flat buffer instead of tuples is 2.7 times faster on NodusDB, and it is 8.3 times faster than NetworkX's batch insert.

### In-process microbenchmarks (JMH)

Recorded on the same machine. The raw results are in [`bench/baseline/windows-dev.json`](bench/baseline/windows-dev.json), and the hardware and command are in [`bench/baseline/MACHINE.md`](bench/baseline/MACHINE.md).

| Benchmark | Parameter | Time per operation |
|---|---|---:|
| Sparse set intersection | smaller set of 16 | 124 ns |
| Sparse set intersection | smaller set of 256 | 2.37 µs |
| Sparse set intersection | smaller set of 4,096 | 51.0 µs |
| Sparse set intersection | smaller set of 65,536 | 2.26 ms |
| Remove then add (churn on 1,024 elements) | | 94 ns |
| Common neighbors (100k nodes, average out-degree 8) | | 304 ns |
| 3-hop traversal (same graph) | | 15.2 µs |

The in-process traversal figure is higher than the 10 to 11 µs the Python benchmark reports for the same query shape. The two use different graphs, and the JMH run is a 1-fork, 5-iteration run. Treat the JMH figures as the reference for the kernel and the Python figures as the reference for what a caller sees.

Read these numbers as indicative. They come from one run on a laptop CPU that boosts and throttles. The first development run measured single-edge ingestion at about 309,000 edges per second, against 203,000 in this run, so absolute throughput can move by a large margin between runs. The orderings held across the runs taken during development, except for common neighbors, which is a close call and went NetworkX's way in this run.

## Quickstart

Install the Python package from a checkout (a native library must be present, see [Building and testing](#building-and-testing)):

```sh
pip install -e python/
```

```python
import nodusdb

g = nodusdb.Graph()
g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])

print("Reachable nodes:", g.khop(start=1, max_depth=3))
print("Common neighbors:", g.common_neighbors(3, 5))

g.close()
```

Output:

```
Reachable nodes: [2, 5, 3, 4]
Common neighbors: [4]
```

`add_edges_from` also accepts a flat buffer such as `array('q', [1, 2, 2, 3])`, which skips the per-tuple conversion and is the fastest input form.

## Architecture from first principles

This section builds the design from the smallest pieces up. Each part answers a question the previous part left open.

### 1. A graph is a set of pairs

A directed edge is a pair `(u, v)`: an arrow from `u` to `v`. A graph is a set of such pairs. The simplest correct structure is therefore a set of pairs, and the work is making that set cheap to query and cheap to change.

The queries NodusDB answers all reduce to two operations on per-node neighbor sets:

- Does `(u, v)` exist? That is membership of `v` in the neighbor set of `u`.
- Which nodes are reachable from `u` within `k` hops? That is repeated expansion of neighbor sets, with a visited check.

So the core problem is a fast set of integers per node, many of them, with frequent inserts and deletes.

### 2. A set as a packed array plus an index: the sparse set

Here is a set of integers stored as two arrays. The **dense array** holds the members packed at the front, in no particular order. The **sparse index** maps each member value to its position in the dense array.

- Membership of `v`: look up `pos = index[v]`, then check that `pos < size` and `dense[pos] == v`. Two reads.
- Insertion: append `v` to the dense array and record its position.
- Removal: move the last dense element into the freed slot, then update the moved element's index entry. The hole is closed by the move, so nothing is left behind.

Every operation is constant time, and the dense array is always a contiguous block. Briggs and Torczon described this structure in 1993, and the EnTT entity library uses the same idea for component storage. Scanning the members means reading the dense array front to back, which the CPU prefetcher handles well.

The catch is the sparse index. Indexed by value, it needs one slot per possible value. If node 7 has neighbors 4 and 9,999,998, the index must span ten million entries, and every high-degree node pays that cost. Memory grows with the size of the ID space, not with the number of edges.

### 3. Replace the array index with a hash table

NodusDB keeps the dense array and replaces the value-indexed array with an **open-addressing hash table** that maps value to position. Memory is now proportional to the degree of the node, whatever the spread of its neighbor IDs.

Open addressing stores entries in one flat array. A key hashes to a starting slot. Lookup scans forward until it finds the key or an empty slot. Three details matter:

- **Hash function.** NodusDB uses the 64-bit finalizer from MurmurHash3. It spreads consecutive IDs across the table, so the sequential IDs a loader produces do not form one long run.
- **Load factor.** The table grows once it is half full. Probe runs stay short, so a lookup usually touches one or two slots.
- **Deletion.** Clearing a slot in the middle of a probe run breaks every lookup that passed through it. Two standard fixes exist. Tombstones mark deleted slots, but they pile up under churn and make every probe slower. NodusDB uses **backward-shift deletion** (Knuth, *The Art of Computer Programming*, vol. 3, Algorithm R). After a removal, later entries in the same run move back into the gap whenever their probe path crosses it. The table stays clean, with no tombstones.

Backward-shift needs one careful rule. The table is circular, so a run can wrap past the last slot back to slot 0. The test that decides whether an entry may move into a gap must use cyclic distance, measured from the entry's home slot. A naive comparison of slot numbers gets wrapped runs wrong, and it makes keys disappear from lookups without any error. The test suite pins this case.

### 4. Most nodes are small: two tiers per node

Real graphs are skewed. Most nodes have a few neighbors, and a few hubs have very many. Giving every node a hash table would waste memory on the many small nodes, and a hash table costs more than a few values in a list.

So each node has one of two states:

- **Low degree (fewer than 16 neighbors).** The neighbors sit in a fixed block of 16 `long` values inside one shared slab, a single large `long[]`. The node stores only an integer block number. Blocks are handed out and returned through a free-list stack, so allocation is O(1). The slab holds no per-node objects, so the garbage collector never tracks them.
- **High degree (16 or more neighbors).** The node holds an indexed sparse set (sections 2 and 3): one packed `long[]` of neighbors and one open-addressing table.

A block is 128 bytes, two 64-byte CPU cache lines. A low-degree lookup scans at most 15 contiguous values, so it costs one or two cache-line fetches. Java does not guarantee array alignment, so the cache-line count is an expectation, not a promise.

**Hysteresis.** A node promotes to high degree when its 16th neighbor arrives. It demotes back to a block only when its degree falls to 8. A node that oscillates around 16 therefore does not reallocate its set on every crossing. Between 9 and 15 neighbors, the node keeps whichever state it already has.

### 5. Two directions, kept in step

Each graph keeps an outgoing table (`u -> v`) and an incoming table (`v <- u`). Both use the same tiered structure. An edge edit updates both, and the test suite checks that the two always agree. The incoming table makes in-degree a constant-time query and lets a reverse traversal run without scanning every node.

### 6. The queries

- **`has_edge(u, v)`.** A hash probe for a high-degree node, or a scan of at most 15 values for a low-degree node.
- **`common_neighbors(u, v)`.** Sweep the smaller neighbor set, and test each member against the larger set. The cost follows the smaller degree, so a hub paired with a leaf is cheap.
- **`khop(start, depth)`.** A breadth-first search. The visited marks are an integer array stamped with a query generation number. Starting a new query increments the generation instead of clearing the array, so each query is O(visited nodes), not O(all nodes). When the generation counter wraps, the array is cleared once.

### 7. Allocation and the garbage collector

Java's garbage collector is the usual cost in latency-sensitive code. NodusDB avoids it on the hot path. Queries and in-tier edits allocate nothing once the structures are warm. Allocation happens only when a node is promoted or demoted, or when a slab or array grows.

The allocation test checks this with the thread's allocated-bytes counter. Across the JMH baseline, the allocation rate stayed at about 0.001 MB/s in every iteration, although the operations ranged from roughly 10 million per second to about 440 per second. A real per-operation allocation would scale with throughput, so the per-operation figures that JMH prints only look non-zero for the slowest operations, where the constant background is divided by a small count. The first measured window after warm-up allocates a fixed 72 to 96 bytes, which the test records as a known exception (decision D12).

### 8. Crossing into other languages

The Java kernel compiles to a native shared library with GraalVM Native Image, so it runs without a JVM. The C interface uses an isolate thread, which GraalVM requires, and it returns sentinel values (`false`, `-1`) instead of throwing, so an exception never reaches the host process. Result buffers are filled up to the caller's capacity, and the call returns the full count, so a caller that guessed too small can grow its buffer and retry.

The Python wrapper calls that interface with `ctypes`. Batch calls send two `int64` columns in one call, and the kernel validates the whole batch before it changes anything. A single bad node ID leaves the graph as it was.

## Building and testing

### Prerequisites

- JDK 22. The build compiles with `--release 22`.
- Maven 3.9 or newer.
- Python 3.9 or newer. The test suite was run on 3.12.
- GraalVM CE 22.0.2 with `native-image`, only for building the native library.
- On Windows, Visual Studio 2022 with the C++ build tools, for `native-image`.

### Commands

```sh
# Java unit, property, and oracle tests
mvn test

# Native shared library (writes target/native/)
GRAALVM_HOME=/path/to/graalvm-community-openjdk-22.0.2 mvn -Pnative -pl core package -DskipTests

# Python tests (they load the native library)
python -m unittest discover -s python/tests -v
# or
pytest python/tests

# Comparative benchmark against NetworkX and SQLite (several minutes)
python python/benchmarks/compare.py

# JMH microbenchmarks
mvn -pl bench -am package -DskipTests
java -jar bench/target/benchmarks.jar -prof gc
```

The Python loader searches `NODUSDB_LIBRARY`, then `python/nodusdb/bin/<os>-<arch>/`, then `target/native/`. CI packages the library into `bin/` for Linux, macOS, and Windows.

### Continuous integration

`.github/workflows/ci.yml` runs on Ubuntu, macOS, and Windows. Each job runs the Java tests, builds the native library with GraalVM CE, packages it into the Python package, installs the package with `pip install -e`, runs the Python tests, and uploads the library as an artifact.

## Limitations

- **In memory only.** There is no persistence and no write-ahead log yet. A process that exits loses its graph.
- **Single writer.** One graph handle must be used from one thread at a time. The handle registry is synchronized, but the kernel is not.
- **Dense integer node IDs.** Node IDs are non-negative `long` values up to `Integer.MAX_VALUE - 9`. Map external identifiers to integers before they enter the graph.
- **Python call overhead.** Each `ctypes` call costs a few microseconds. Use the batch methods and flat buffers for bulk work.
- **Measured on one laptop.** The numbers above come from one machine. The GitHub Actions runs check that the build and tests pass on all three operating systems. They do not measure performance.

## License and status

Licensed under the [Apache License, Version 2.0](LICENSE).

Status: Rings 1 to 3 are complete. Ring 1 is the core data structures, Ring 2 is the graph kernel and traversal, and Ring 3 is the native C interface and Python package. Ring 4 (concurrency and a write-ahead log) has not started.
