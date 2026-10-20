# NodusDB

[![CI](https://github.com/Pragadeesh-19/nodusdb/actions/workflows/ci.yml/badge.svg)](https://github.com/Pragadeesh-19/nodusdb/actions/workflows/ci.yml)

NodusDB is an engine that runs inside your process. It has two parts, built from the same low-level pieces:

- A **graph engine** that holds a directed graph in memory, answers traversal queries in microseconds, and can persist itself to a directory with a write-ahead log.
- A **lakehouse delta engine**, called Spillway, that absorbs streaming writes and commits them as Parquet files that Apache Iceberg and DuckLake can read.

Neither part starts a server or makes a network call. Both keep their hot paths free of garbage collector work.

CI builds the GraalVM native library on Ubuntu, macOS (arm64), and Windows and runs the Java and Python suites against it. [Verification](#verification) lists what the tests cover.

## The graph engine

Use it for state that changes all the time: agent working memory, live permission graphs, session graphs. Edges come in and go out constantly, and a query has to finish before the next event arrives.

These results come from `python/benchmarks/compare.py`, run on 2026-10-18 against the native library the wheels carry (GraalVM Community Edition 25.0.2). The run loads 1,000,000 directed edges over 200,000 nodes, deletes 500,000 of them, and then runs 200 queries of each kind. Every backend is checked for identical answers before timing starts.

| Configuration | Insert edges/s | Delete edges/s | 3-hop median | Common neighbors median |
|---|---:|---:|---:|---:|
| NodusDB, flat `array('q')` | 162,689 | 137,975 | 27.3 µs | 15.6 µs |
| NodusDB, batch of tuples | 132,281 | 119,379 | 27.3 µs | 15.7 µs |
| NodusDB, single-edge calls | 71,962 | 66,951 | 27.3 µs | 19.0 µs |
| NetworkX, single | 149,164 | 324,683 | 35.4 µs | 6.1 µs |
| NetworkX, batch | 136,750 | 307,860 | 41.2 µs | 9.1 µs |
| SQLite, `executemany` | 133,632 | 132,545 | 113.6 µs | 23.3 µs |
| SQLite, one statement per row | 101,553 | 100,175 | 114.1 µs | 23.3 µs |

From the same run:

- Inserts are close to NetworkX and SQLite. The flat buffer loads 1.2 times as fast as NetworkX's batch insert and as SQLite's `executemany`.
- NetworkX removes edges 2.2 to 2.4 times as fast as NodusDB's flat buffer does.
- The 3-hop median is 1.3 to 1.5 times faster than NetworkX and about 4 times faster than SQLite.
- NetworkX is faster on common neighbors (6.1 and 9.1 µs against 15.6 µs).

An earlier version of this table, measured on 2026-10-03 at commit `0b05475` (before typed log records, native-memory storage, the memory budget and the change feed), reported 1.25 million inserts per second and a 10.5 µs 3-hop median. That commit, rebuilt with GraalVM CE 22.0.2 and run on the same laptop on 2026-10-18, measured 1.06 to 1.30 million inserts per second and 10.3 to 10.7 µs. The code in this release is slower on this benchmark: 6.5 to 8 times on inserts and about 2.6 times on the 3-hop median. A native-image build runs the insert path 11 to 13 times slower than the same code on a JVM (`python/benchmarks/native_ratio.py`).

The in-process JMH run (`GraphTraversalBench`, a different graph) gives a 3-hop median of 21.2 µs and a common-neighbors median of 0.50 µs, against 15.2 µs and 0.30 µs on 2026-10-03. The raw data is in [`bench/baseline/release-windows-dev.txt`](bench/baseline/release-windows-dev.txt), which also compares three GraalVM toolchains; the older baseline is [`bench/baseline/windows-dev.json`](bench/baseline/windows-dev.json). The machine and the exact command are in [`bench/baseline/MACHINE.md`](bench/baseline/MACHINE.md).

These are single runs on a laptop CPU that boosts and throttles. Three runs of the NodusDB insert rate agreed within 5%.

### Durability on a real graph

The durable benchmark uses the SNAP soc-Pokec graph: 30,622,564 directed edges. It is a real social network, not generated data. Each phase runs in its own process with a 6 GB heap. The full commands and the digests are in [`bench/baseline/durability-windows-dev.txt`](bench/baseline/durability-windows-dev.txt). These numbers were measured between 2026-10-04 and 2026-10-05 on JDK 22.0.2, before the authorization work, and were not re-run for this release.

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

The Parquet output matched PyArrow bit for bit on 100,000 mixed rows. CI installs `pyarrow`, so that comparison runs there too.

Not built yet: Iceberg table metadata and Avro manifests (the manifest is Iceberg-shaped, but it is local), compaction, the Arrow C Data export, and reads that do not decode a whole file. [`docs/lake.md`](docs/lake.md) has the design and the open items.

## Against other engines

Two suites compare NodusDB with other engines on real data. The graph suite runs on the SNAP soc-Pokec social network, 30,622,564 directed edges. The lakehouse suite runs on the first 1,000,000 rows of the NYC TLC Yellow Taxi file for January 2024. Each engine and workload runs in its own process. The answers are cross-checked: the permission digests, the Pokec hub degrees, and the taxi revenue total agree across every engine that finished. Each number is a single run on the same laptop as the rest of this file. Both suites ran on 2026-10-04 with a native library built by GraalVM CE 22.0.2, before the authorization work, and have not been re-run since. The graph results describe the engine as it was then; the first table in this file shows that its per-call cost has changed. The raw output is in `bench/baseline/`. The graph suite takes about 2.5 hours and the lake suite about 20 minutes.

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
- Traversal on Pokec is not sub-microsecond. The design targets sub-microsecond k-hop queries, and this suite does not reach that target. The smaller benchmarks above (21 to 27 µs) do not reach it either.

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

Install the Python package from a checkout. It needs the native library, so build it or point `NODUSDB_LIBRARY` at a copy. See [Building](#building). Release wheels install with `pip install nodusdb` on Python 3.10 or newer and need no compiler or GraalVM.

```sh
pip install -e python/
```

Graph in memory:

```python
import nodusdb

with nodusdb.Graph() as g:
    g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])

    print("Reachable nodes:", g.khop(start=1, max_depth=3))
    print("Common neighbors:", g.common_neighbors(3, 5))
```

```
Reachable nodes: [2, 5, 3, 4]
Common neighbors: [4]
```

A `Graph` and a `LakeTable` own native memory. Close them with a `with` block or `close()`, which can be called twice. If one is garbage collected while still open, a finalizer releases its native handle and emits a `ResourceWarning` that names the leak. A durable graph is closed the same way, so a final snapshot is written and the directory lock is released. The finalizer is a safety net: Python decides when it runs, and an unclosed object can hold a directory lock until then.

Graph with durability. The graph recovers from the directory when it opens, and each edit is logged:

```python
import nodusdb

with nodusdb.Graph(path="/data/graph", sync_mode="async") as g:
    g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])
    g.checkpoint()
# leaving the block writes a final snapshot and releases the directory lock

with nodusdb.Graph(path="/data/graph") as g:
    print("Reachable after restart:", g.khop(start=1, max_depth=3))
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

Permissions. A graph can hold typed tuples under a schema and answer permission checks:

```python
import nodusdb

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
    token = g.add_tuple("group:staff", "member", "user:alice")

    print(g.check("document:readme", "view", "user:alice", at_least=token))   # True
    print(g.check("document:readme", "view", "user:bob"))                     # False
```

A schema declares types, stored relations with the subject types they allow, and permissions built from unions, computed relations and arrows (`parent->view`). It moves forward one version at a time, and relation ids are never reused. `nodusdb.Transaction` groups tuple writes so that every tuple applies or none does. Each write returns a `Token(epoch, lsn)`. Passing it as `at_least` makes a check refuse to answer from state older than that write: `NodusStaleReadError` if the graph has not reached it, `NodusTokenLostError` if the write was acknowledged but never became durable. A check that cannot decide within the depth limit of 32 raises `NodusCheckDepthError` rather than answering `False`. `durability="lake"` also waits until the write is in the object store, and raises `NodusUnsupportedError` when shipping is not configured (see [Shipping the log](#shipping-the-log-to-object-storage)).

Reads take no lock, so threads can check against one graph while another thread writes.

Upgrading. A directory written by an earlier release is refused with `NodusUpgradeRequiredError` until it is converted:

```python
import nodusdb

report = nodusdb.upgrade("/data/graph")        # report.performed, report.edges, report.symbols
nodusdb.upgrade_cleanup("/data/graph")         # delete pre-v2/ once you trust the result
```

`upgrade` keeps the original files in `pre-v2/`, writes the new files, and reopens the result to compare an edge fingerprint and the symbol count with the original before it reports success. If it is interrupted, run it again.

Memory limit. `max_memory_mb` caps the native memory that holds the graph's adjacency data. The default, `None`, sets no cap:

```python
import nodusdb

with nodusdb.Graph(max_memory_mb=256) as g:
    try:
        g.add_edges_from(edges)
    except nodusdb.NodusMemoryError:
        ...  # the graph holds what fit and is still fully queryable
```

A write that would pass the limit raises `NodusMemoryError`, which is both a `NodusError` and a built-in `MemoryError`. It is raised before the graph changes, and a rejected edge is never written to the log of a durable graph. Edges already stored stay queryable. Removals always succeed, and a write that needs no new storage still works. `add_edges_from` applies edges in order and stops at the first one that does not fit, so the edges before it stay applied. The limit is a whole number of megabytes, at least 1.

Native storage grows in chunks that double, so the limit can trip while part of the budget is unused. A large node id needs a large node table, and that counts too. The limit covers the graph's native memory: the adjacency storage and the symbol table. Query result buffers live on the Java heap and are not counted. A durable graph whose stored snapshot does not fit the limit fails to open with `NodusMemoryError`. Open it again with a larger limit, or none.

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

A durable graph lives in one directory:

- `log/` holds the write-ahead log as segments of at most 64 MiB, named by the sequence number of their first record. Records are typed and variable length, each with a CRC32C. `log/FORCED` records how far the log is known to have reached the disk.
- `snapshot.bin` is the last checkpoint: a complete image of the graph, the schema, the symbols and the epoch history, in checksummed sections.
- `FORMAT` names the directory format, and `nodus.wal` is a short marker that makes older releases refuse the directory.
- `nodus.lock` holds a file lock, so a second process or a second handle cannot open the same directory.

**Writing.** A write is validated, its memory is reserved, and it is appended to the log. In `sync` mode the call then waits until the record is on disk. Only after that does the change appear in memory, so a reader never sees a write that a crash could undo. A duplicate add or a missing remove does nothing and writes nothing. A transaction is its records followed by a commit record, and recovery applies whole transactions only.

**Checkpoint.** A checkpoint forces the log, writes the snapshot to a temporary file, forces it, renames it into place, and deletes the log segments the snapshot covers. A crash between the rename and the deletion is safe: recovery skips every record the snapshot already holds.

**Recovery.** On open, the process deletes half-written temporary files, loads the snapshot, and replays the log from the first record after it. Where the log goes bad decides what happens. Damage in the part of the log known to have reached the disk means corruption, and the directory refuses to open with `NodusCorruptLogError`. Damage after that point is a torn tail from a crash: the log is cut back to its last commit and the cut bytes are reported. Every open then starts a new writer tenure, which is what lets a token name a write that was lost. If forcing the log fails, the write that waited gets `NodusIndeterminateError` and the log refuses further writes until the graph is reopened, because retrying a failed fsync is not safe on Linux.

The crash tests replay every prefix of the writes and forces of a run through a simulated disk, with torn writes at every byte, and require recovery to return a prefix of what was acknowledged.

### What survives what

| Failure | `async` | `sync` (single calls) |
|---|---|---|
| The process is killed | Everything the OS has accepted survives. A write still in the buffer is lost | Every acknowledged write survives |
| The machine loses power | Up to the last 10 ms of writes can be lost | Every acknowledged write survives |

Call `sync()` or `checkpoint()` to force a point where everything is on disk.

## Shipping the log to object storage

A durable graph can copy its log to a bucket as it writes. The bucket then holds every change in order, signed and hash-linked, in a format other tools read without the graph running.

```python
from nodusdb import Graph, Shipping, Transaction, environment_credentials, generate_signing_key

generate_signing_key("signing.pem", "signing.pub")     # once; keep signing.pem private

shipping = Shipping.s3(
    "my-bucket", "eu-west-1", prefix="graphs/prod",
    key_file="signing.pem", key_id=1, public_key_file="signing.pub",
    credentials=environment_credentials(),             # or file_credentials(), static_credentials(...)
    iceberg=True,                                       # also project the log into an Iceberg table
)
with Graph(path="graph", sync_mode="sync", shipping=shipping) as g:
    g.apply_schema(SCHEMA)
    token = g.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
    print(g.stats()["shipping"]["lag_lsn"])             # 0 once the bucket holds everything
```

`Shipping.directory(path, ...)` ships into a local directory instead, which is what the tests use. Any S3-compatible server works with `endpoint=` and `path_style=True`.

**What lands in the bucket.** Under the prefix, `_nodus/chain/` holds numbered objects. Each one carries a run of log records, an Ed25519 signature, and the SHA-256 of the object before it, so a reader can detect a gap, a reordering or a forged object. `_nodus/snapshots/` holds a copy of each checkpoint, and a snapshot reference in the chain says which one. `_nodus/epoch/` holds one small claim per writer tenure. With `iceberg=True`, `iceberg/` holds an Iceberg v2 table `nodus_log`, with one row per tuple change, names instead of ids, partitioned by day. pyiceberg, DuckDB and Spark read it.

**Durability `lake`.** A local write returns once the log has it. A `lake` write also waits until the object is in the bucket, 30 seconds at most (`timeout=`). On a timeout the write is still applied and logged locally, and `NodusShipTimeoutError.token` names it, so `wait_shipped(token)` can finish the wait later. Other threads keep using the graph during the wait.

**Opening.** The open checks the bucket before it takes any write. An empty bucket starts a chain with a snapshot of the graph. A bucket whose chain this directory can continue gets a new epoch, and the graph keeps shipping where the chain ends. A directory that is behind the chain, or that lost records the chain never received, is refused with `NodusWriterFencedError`. A store that ignores conditional writes is refused with `NodusUnsupportedError`, because the chain's safety rests on them. A second writer that claims a newer epoch fences the first: its writes fail with `NodusWriterFencedError` from then on.

**Outages.** A bucket that cannot be reached does not stop local writes at first. The unshipped part of the log grows, local trimming waits for it, and `stats()` reports `RETRYING` with the last error. At half of `backlog_cap_bytes` (1 GiB by default) the `nodusdb` logger warns. At the cap writes fail with `NodusLogBacklogError` until shipping catches up, then they resume by themselves.

**Operating it.** `stats()` returns a dictionary with the shipper phase, the shipped and applied positions, the lag, the backlog, the age of the last commit and snapshot reference, retention counters and the projection state. New events go to the `nodusdb` logger once each. Retention deletes chain objects and snapshots older than `retention_days` (7 by default), but never the part a restore or the projection still needs.

**Cost.** The shipper uploads at most once per `interval_ms` (100 ms by default) and only when there is something to send, so a busy writer makes about 864,000 PUT requests a day and an idle one makes none. A larger interval means fewer requests and a longer `lake` wait. A local write costs about what it did without shipping: `python/benchmarks/shipping_ratio.py` measures `add_tuple` with and without shipping in one run, and CI fails above a ratio of 3.

The bucket needs conditional writes (`If-None-Match`), which S3 supports, and an operator-owned signing key. NodusDB never trusts a key stored in the bucket: readers take the public key from their own configuration.

## Reading the chain back

The chain in the bucket holds everything a graph needs, so another process can rebuild the graph from it, follow it, or continue it as the writer. All of it verifies every object against a public key from its own configuration. NodusDB never trusts a key found in the bucket.

```python
from nodusdb import Follower, Graph, Shipping, environment_credentials, restore, salvage

token = writer_token   # the Token a writer returned from write()

follower = Follower.s3(
    "my-bucket", "eu-west-1", prefix="graphs/prod",
    public_key_file="signing.pub", key_id=1,
    credentials=environment_credentials(),
    read_wait_ms=1000,          # how long a check with a token waits for the follower to catch up
    max_staleness_ms=30_000,    # optional: refuse reads once the head was not confirmed for this long
    state_directory="/var/lib/nodus/follower",   # optional: remembers the furthest position it saw
)

with Graph(follow=follower) as replica:             # a read-only copy, rebuilt from the bucket
    replica.wait_until_current()
    replica.check("document:a", "view", "user:alice", at_least=token)   # waits for the write the token names
    print(replica.stats()["follower"]["phase"])      # CURRENT, CATCHING_UP, LAGGING, STALLED

restore(follower, "recovered-graph")                 # a durable graph directory, ready for Graph(path=...)
```

**A follower** downloads the newest snapshot it can verify, loads it, and then applies the chain object by object. Each transaction becomes visible whole. Writes raise `NodusUnsupportedError`. Reads raise `NodusStaleReadError` until the snapshot has loaded. By default a follower keeps answering from its last verified state when the store is down, and `stats()` says why it is behind. With `max_staleness_ms` it refuses instead. A check with `at_least=` waits up to `read_wait_ms` for the follower to reach the token, then raises `NodusStaleReadError`, or `NodusTokenLostError` when the write was acknowledged but a takeover dropped it.

**What stops a follower.** A bad signature, an unknown signer, a broken link, a fork or a bucket that went backwards stops it with the reason in `stats()["follower"]["stall_reason"]`, and `restore()` and a takeover raise `NodusChainTrustError`. A follower that falls so far behind that retention deleted the objects it needs stops too ("fell behind retention; restart to rebuild"). It keeps serving what it has, and a restart rebuilds it from the newest snapshot. With `state_directory`, the follower writes a small checksummed file of the furthest object it has seen, so a restart against a rolled-back bucket is refused. A damaged file stops the start with `NodusCorruptLogError` naming it, and deleting it accepts the loss.

**Restore.** `restore()` writes the directory into a sibling `.restore-tmp` and renames it into place when it is complete, so a crash never leaves something that looks like a graph. The target must not exist or must be empty.

**Takeover.** To replace a dead writer, open a new directory with `Graph(path="new-graph", shipping=shipping, takeover=True)`. It claims the next epoch, which fences the old writer on its next commit, restores into the new directory, opens it as the writer, and waits until the chain holds its first object. `graph.takeover_report` has the epochs and the last LSN carried over. A `lake` write that returned before the takeover is in the new graph. A `local` write the old writer had not shipped is not, and `salvage()` lists it.

**Salvage.** `salvage(follower, "old-graph")` reads the old writer's directory without writing to it, finds the last LSN the chain keeps, and returns the transactions above it with the names of what they changed. It refuses a directory another process has open. The handoff is `provisional` until a later writer has taken over. A checkpoint can have trimmed part of the range from the log, and `unavailable_through_lsn` says so.

**Cost.** A follower makes one GET per `poll_interval_ms` (100 ms by default) when idle and one LIST per second. On the development laptop it applies about 2.5 million tuples a second on one core (`FollowerApplyBench`), and the check path pays nothing measurable for seeing a schema migration (`CheckModelBench`).

## Erasure

`erase` removes a subject or an object for good: every tuple that names it, and the name itself everywhere NodusDB keeps it.

```python
token = g.erase("user:alice")          # memory is clean when this returns
g.wait_erased(token, timeout=300)      # the directory, the bucket and the Iceberg table are clean
print(g.stats()["erasure"])            # per layer: configured, covered_lsn, pending
```

The name is replaced by a pseudonym, `erased:` and the first 16 hex digits of a salted SHA-256 of the name. The salt is stored in the graph's snapshot. The erasure is a transaction in the log, so every follower applies it too. A name that does not exist returns the current token. A type or relation name used by the schema is refused with `NodusSchemaError`, and an integer-keyed graph or a follower raises `NodusUnsupportedError`. The same name written later is a new subject that starts with no grants.

A subject with many tuples is removed in transactions of at most 30,000 tuples, followed by the erasure itself. If the process dies in between, the name is still there with some tuples gone, and calling `erase` again finishes the job. Erasing a subject with 1,000 grants took 2.8 ms on the development laptop, and one with 100,000 grants took 51 ms (`EraseBench`).

**The copies on disk** are scrubbed in the background:

- **The graph directory.** A checkpoint writes the erased symbol as its pseudonym, and the log segments before it are deleted. Erasures are collected for `erasure_checkpoint_delay_s` (60 seconds by default, set on `Shipping`) so a burst shares one checkpoint. `wait_erased` checkpoints at once.
- **The bucket.** Chain objects are signed and linked, so they are never edited. Once a snapshot taken after the erasure is in the bucket, the writer commits a signed `REDACTION` entry and deletes every chain object and snapshot from before that point. A transaction that erases a name always starts a new chain object, so nothing the bucket keeps still holds the name. Followers bootstrap from the newest snapshot as before. One that was behind the deleted range stops with "fell behind retention" and rebuilds on restart.
- **The Iceberg table.** The projector rewrites every data file that holds a row naming the erased subject or object, commits one overwrite snapshot with a signed `nodus.redaction.v1` record, expires every older snapshot, and deletes the replaced files and manifests. Time travel therefore reaches back only to the erasure. The rows stay, with the pseudonym in place of the name.

`wait_erased` raises `NodusErasurePendingError` (code -16) when the time runs out. It names the layers still pending and carries the token. The erasure itself is already done and the background work continues. After a restart or a takeover the writer compares what each layer covers with the number of erased symbols and finishes the rest. The tests check the result byte for byte: after `wait_erased`, no file in the graph directory, the bucket or the Iceberg table contains the erased name.

**What erasure cannot reach.** Copies made outside NodusDB are out of its hands: a backup or copy of the directory, a salvaged old directory, `pre-v2/` until `upgrade_cleanup()` runs, object versions or replicas kept by the bucket's own settings, and disk blocks the file system freed but did not overwrite. Anyone who can read a snapshot has the salt and can test a guessed name against a pseudonym, so a short or predictable name is only as private as the snapshot. Prefer opaque ids, such as UUIDs, for subjects. In the Iceberg table a name written again after its erasure keeps its new rows. After a restart the writer knows when such a name was erased only up to the snapshot it loaded, so new rows written before that snapshot can also show the pseudonym.

## Watching changes

`changes(after)` returns what was committed after a token, in log order and in whole transactions, with the cursor to pass next. `watch()` does the same in a loop and yields each batch.

```python
cursor = g.token                                   # remember where you are
batch = g.changes(cursor, timeout=1.0)             # waits up to a second for the next commit
for change in batch.changes:
    print(change.kind, change.object, change.relation, change.subject, change.token)
cursor = batch.next                                # store it, and resume from it after a restart

for batch in g.watch(cursor):                      # the same, forever, until the graph closes
    ...
```

A change is an `add`, a `remove`, a `schema` (with its version), an `epoch` (a writer started or took over) or an `erase` (with the pseudonym). `change.token` is the token its write returned. Names are looked up when the batch is read, so an erased name always reads as its pseudonym, also in changes from before the erasure. A change inside a transaction of several writes can name a tuple that was already there: the feed is the log, as written.

`AccessWatch` turns the feed into gained and lost access for checks you register:

```python
from nodusdb import AccessWatch

access = AccessWatch(follower, [("document:plan", "view", "user:alice")])
for change in access.poll(timeout=1.0):           # an AccessChange per check whose answer flipped
    if not change.allowed:
        disconnect(change.subject, change.object)
```

Each check is answered once when the watch starts, and again after every batch, at that batch's token. It reports the net change per batch, so access granted and revoked inside one batch is not reported, and each poll costs one check per registered triple.

**Where old cursors are read from.** Recent changes come from a ring in memory (`Graph(watch_buffer_changes=...)`, 1,048,576 changes and 24 MiB by default), which is created on the first watch call and is fed by every write and, on a follower, by every applied transaction. A cursor older than the ring is read from the writer's local log and then from the bucket's signed chain, which a follower uses too. A reader that is catching up stays open between calls, so a long catch-up does not search again on every call. When no source holds the changes after a cursor, the call raises `NodusHistoryExpiredError` (code -17); read the current state and start again from `g.token`. A cursor naming a write that was lost when the writer restarted raises `NodusTokenLostError`. A clean close checkpoints and trims the log, so a graph without shipping keeps no watch history across a restart.

## Time travel

`as_of` opens a read-only copy of the graph as it stood at a token or at a moment:

```python
from datetime import datetime, timedelta, timezone

with g.as_of(datetime.now(timezone.utc) - timedelta(hours=3)) as then:
    print(then.token, then.commit_time)            # the last commit at or before that moment
    print(then.check("document:plan", "view", "user:alice"))
    then.advance(datetime.now(timezone.utc) - timedelta(hours=1))   # forward only

with g.as_of(token_of_a_write) as then:            # exactly the state that write produced
    ...
```

The copy is rebuilt from the bucket: the newest snapshot at or before the target, then the chain replayed up to the target's last commit, all verified as a follower verifies them. It works on the writer and on followers, and a graph without shipping raises `NodusUnsupportedError` (`Shipping.directory` serves a single machine). On the writer the call first waits until its current writes are shipped, so an instant in the last second is exact; a follower answers from the chain it can read, and `then.token` says which commit it stands on. Every read works on it (`check`, `has_edge`, `khop`, `stats`), writes raise `NodusUnsupportedError`, and nested groups answer at full speed from the start, because the reachability index is rebuilt before the view is returned.

A view holds a full copy of the graph. `max_memory_mb` limits one view (by default the graph's own limit), and `history_max_memory_mb` on `Shipping` or `Follower` limits all open views of a graph together. A view that does not fit raises `NodusMemoryError` and leaves nothing behind. `stats()["history"]` shows the open views and their memory, and a view's own `stats()["as_of"]` shows its snapshot, what it replayed and how long the build took. The build replays every chain object since the newest snapshot before the target, so it is as fast as your checkpoints are frequent: checkpoint on a schedule if you use time travel often.

**How far back.** As far as the bucket keeps history (7 days by default, `retention_days`), and never before the newest erasure, because an erasure deletes the bucket history before it. A target outside that range raises `NodusHistoryExpiredError`. A view also forgets a name erased after it was opened, and it refuses to move forward past such an erasure. The Iceberg table keeps every row, with erased names as pseudonyms, for audits that need to reach further back with SQL.

## Concurrency

The kernel supports one writer and any number of readers. Every mutation runs inside a seqlock: a version counter is odd while an edge change is in progress and even otherwise. A reader checks the counter before and after each query. If a write overlapped the query, the reader discards the result and runs it again. Readers never take a lock and never block the writer, and a query never returns a half-applied change.

The stress test in `GraphConcurrencyTest` runs one writer that toggles 200,000 edges on a high-degree node while eight readers run `kHop` and `commonNeighbors`. Every result must hold distinct, in-range targets. The test also checks that the readers allocate nothing after the writer stops. Breaking the validation step makes it fail with a duplicated target.

Readers on one handle run in parallel. Looking up a handle takes no lock, a read takes no monitor, and each thread keeps its own result buffer, so one reader never sees another's results. Writes, checkpoint and close serialize on the handle, and a write does not wait for readers. Each OS thread runs its native calls on its own GraalVM isolate thread. The Python `Graph` holds its lock for writes and close only. `GraphSessionReadersTest` and `python/tests/test_concurrency.py` run readers against a busy writer, and `python/tests/test_crash.py` kills a writer with SIGKILL and checks that every acknowledged sync write survives.

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

### Nested groups

A check through nested groups (`group:eng#member@group:backend#member`) would otherwise walk every group under the one it asks about. The kernel keeps a third table pair with the transitive closure of that nesting: for each group, every group nested under it at any depth, stored as derived tuples in the same packed-key format as real ones. A membership check then probes the user's direct memberships against that closure, sweeping whichever side is smaller. The user's direct memberships need no extra structure, because the incoming table already lists them, so adding a user to a group costs no index work.

Only changes to nesting update the closure, inside the same write section as the tuple. An update that would cost more than 4,096 steps, or that the memory budget or the entry cap refuses, marks the affected groups dirty instead. A dirty group is answered by the ordinary evaluator, so every answer stays exact, and a background thread rebuilds dirty groups in small slices that give way to writes. A group answered through the closure does not count toward the depth limit. The closure is never written to disk: a graph rebuilds it after a restart, and checks use the evaluator until it is ready. `stats()["reachability"]` shows its state, and `NODUS_REACHABILITY_INDEX=off` turns it off.

On the development laptop (`bench/baseline/reachability-windows-dev.json`), a check against the root of a tree of 1,111 nested groups took about 0.7 microseconds with the index and 110 to 116 microseconds without it, and a check through a chain of 30 groups 0.65 against 2.5 to 3.5 microseconds. Adding and removing a direct member costs the same either way. Nesting and unnesting a leaf group under that tree costs 7.7 microseconds for the two writes, against 2.2 without the index.

### Columns for the lake

The lake buffer applies the same idea in a different shape. Each column is its own native buffer, 64-byte aligned, and row *n* is the *n*th entry in every column. Text goes into one shared native slab, with an offset and a length for each row. A flush gathers the selected rows, encodes each column chunk from native memory, and streams the pages to the file, so no Java array holds column data at any point.

A delete swaps the last row into the gap across every column, updates the index entry for the moved key, and then removes the deleted key. A delete that must survive a flush writes a tombstone instead.

### No allocation on the hot path

Queries and in-tier edits allocate nothing once their structures exist. Allocation happens in three places: a node changes tier, a native buffer grows, or a buffer is replaced. The lake reuses its buffers. After a flush, the frozen buffer is cleared and reused, so its native columns keep their grown size and the ingestion thread stops allocating. Lake upserts take `MemorySegment` inputs, so a caller's arrays are wrapped once, not per call.

The graph allocation test reads the thread's allocated-bytes counter. Across the JMH baseline, the allocation rate stayed near 0.001 MB/s in every iteration. The first measured window after warm-up allocates a fixed 72 to 96 bytes, which the test records as a known exception (decision D12).

### Crossing into Python

The Java code compiles to a native shared library with GraalVM Native Image, so Python does not need a JVM. Each exported function takes a GraalVM isolate thread, which GraalVM requires. Functions return sentinel values such as `false` or `-1` instead of throwing, so an exception cannot escape into the host process. Every failure returns a negative code from a fixed contract, and the Python layer raises the matching exception: `-3` `NodusMemoryError`, `-4` `NodusStaleReadError`, `-5` `NodusCheckDepthError`, `-6` `NodusSchemaError`, `-9` `NodusCorruptLogError`, `-10` `NodusUnsupportedError`, `-11` `NodusUpgradeRequiredError`, `-12` `NodusIndeterminateError`, `-13` `NodusTokenLostError`. The message of the last failure on a thread is available through `nodus_last_error`.

Results are written into caller-supplied buffers, and the call returns the full count. A caller that guessed too small can grow the buffer and retry. Batch calls validate the whole batch before they change anything, so one bad ID leaves the graph as it was.

## Verification

The `ci` workflow runs on Ubuntu, macOS (arm64), and Windows for every push. It runs the Java suite with assertions enabled, builds the native library with Oracle GraalVM 25, packages it into the Python package, runs the Python suite against it and then the native performance gate.

On the development laptop (Windows, JDK 22, a native library built by GraalVM CE 22.0.2) the Java suite passes with 630 tests and the Python suite with 120, with `pyarrow` installed so that none skip. What they cover:

- The log: a crash simulator replays every prefix of the writes and forces of a run, with torn writes at every byte, and requires recovery to return a prefix of what was acknowledged. Corruption below the forced mark and a failed fsync have their own tests.
- Durability on the graph: clean restarts, crashes in both sync modes, torn log tails, snapshot rolls, a crash between the snapshot rename and the deletion of the covered segments, corrupted and truncated snapshots, the directory lock and the memory limit.
- Upgrade: the version 1 corpus (integer, string and snapshot-only directories) upgrades with equal edge fingerprints, and the upgrade is crashed after every step and finished by running it again.
- Permissions: a seeded differential test compares the iterative evaluator with a naive recursive one over random tuples on ten seeds. Disabling arrow traversal makes four of them fail.
- Concurrency: readers run against a busy writer on one kernel, one handle and one Python object, and a reader is not blocked while a write waits for its fsync.
- The native performance gate compares the same Java code on the JVM and in the native library. On this laptop the ratio is about 9 for `add_edge` and 16 for `has_edge`, and the gate fails above 50. The CE 23 library, about 130 times slower than CE 22, would fail it.
- The Pokec run was checked by digest, as described above.

The Java sources compile for JDK 22.

## Building

Prerequisites:

- JDK 22 and Maven 3.9 or newer.
- Python 3.10 or newer. The tests ran on 3.12.
- GraalVM Community Edition with `native-image`, for the native library only: 25.0.2 as in CI and in the wheels (25.0.1 for the macOS Intel wheel, because 25.0.2 has no macOS Intel build), or 22.0.2. The toolchain changes the speed of the library. On the development laptop 25.0.2 and 22.0.2 agree within 5%, and 25.0.1 is up to 10% slower on a lookup. Oracle GraalVM 25 took about a third less time per call on the same code, but its free license allows redistributing the compiled library only without a fee for products that bundle it, so the wheels, which are Apache-2.0, use the Community Edition. A local build with GraalVM CE 23 ran `add_edge` in about 1.2 ms where CE 22.0.2 took 9 microseconds, because memory-segment access was not optimized. Check a new toolchain with a timing before you trust it.
- On Windows, Visual Studio 2022 with the C++ build tools.

CI runs the Java tests on Oracle GraalVM 25 with 3 to 4 cores and a slow disk; only the native library uses the Community Edition. A test that passes on a 16-core machine can fail there when it assumes a background thread is idle, has finished, or has not started. Before you push a change that touches threads, shipping, followers or any test that waits, run the suite pinned to two cores with CPU spinners on them (Windows):

```powershell
$env:JAVA_HOME = "C:\path\to\graalvm-jdk-25"
powershell -File scripts\contention.ps1 "mvn -B -ntp -pl core test"
powershell -File scripts\contention.ps1 "python -m pytest python/tests -q" -Cores 2 -Spinners 4
```

```sh
# Java tests. The Parquet, Avro and Iceberg tests judge their output with real readers:
# pip install "pyiceberg[pyarrow]" fastavro pyarrow tzdata
# Set NODUS_PYTHON to that interpreter if it is not `python` on the path. Without the packages those
# tests skip, and NODUS_REQUIRE_PYTHON_VERIFIERS=1 turns the skip into a failure, as CI does.
mvn test

# Native library, written to target/native/
GRAALVM_HOME=/path/to/graalvm-community-openjdk-22.0.2 mvn -Pnative -pl core package -DskipTests

# Python tests, which load the native library
python -m unittest discover -s python/tests -v

# The same tests under pytest, as CI runs them. Needs: pip install pytest pyarrow
python -m pytest python/tests -ra

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

# Shipping overhead: add_tuple with and without shipping, one run, fails above a ratio of 3
python python/benchmarks/shipping_ratio.py

# JMH microbenchmarks
mvn -pl bench -am package -DskipTests
java -jar bench/target/benchmarks.jar -prof gc
```

The Python loader looks in `NODUSDB_LIBRARY`, then `python/nodusdb/bin/<os>-<arch>/`, then `target/native/`. CI copies the library into `bin/`. If you rebuild `target/native/` by hand, copy it into `bin/` too, or the loader keeps using the old copy.

Release wheels come from `.github/workflows/wheels.yml`. It builds the native library with GraalVM Community Edition on Linux (x86_64 and arm64), macOS (x86_64 and arm64), and Windows, and then runs cibuildwheel once per platform. The native library is the only compiled part of the package and Python loads it through `ctypes`, so each wheel is tagged `py3-none-<platform>` and works on every CPython from 3.10 on. Every wheel is then installed and smoke-tested on Python 3.10 to 3.14 on its own platform, and the smoke test calls each feature family and checks the license files and `__version__`. A separate step points `Shipping.s3` at `https://s3.amazonaws.com` with a fake key from the installed wheel and requires the S3 authentication error, which shows that the library trusts public certificates. The workflow runs on every push. It publishes to PyPI only when a release is published, through trusted publishing and a required reviewer on the `pypi` environment. Before it uploads, `scripts/check_release.py` requires the release tag to be `v` plus the package version, the release's pre-release flag to match the version suffix, and the five expected wheels to be present. Linux wheels are tagged `manylinux_2_34`, so they need glibc 2.34 or newer: Ubuntu 22.04 and later, Debian 12, and RHEL 9. The manylinux2014 build container cannot install a 2.34 wheel, so the install step runs on the host runner. macOS wheels target 11.0 and later. Windows wheels are x86_64 only.

## Compatibility

This is what 0.1.x promises. A golden graph directory in the test suite pins the first two items: a test opens it with the current code on every build.

- **Graph directories.** The on-disk format does not change within 0.1.x: format 2, with typed log records and a version 3 snapshot. A directory written by any 0.1.x release opens in every later 0.1.x release. A release that meets a newer format refuses to open it with `graph directory format N needs a newer nodusdb` and leaves the files alone.
- **Directories from 0.1.0rc1.** They use the earlier format and are refused with `NodusUpgradeRequiredError` until `nodusdb.upgrade(path)` converts them. The upgrade keeps the original files in `pre-v2/`. It cannot be undone: rc1 refuses an upgraded directory.
- **The Python API.** The names and signatures in this README stay in 0.1.x. Releases may add new ones. A change that breaks an existing call waits for 0.2.
- **The native library.** The C functions inside it are not an interface. The wheel carries its own copy, so the package and the library always match.
- **Not covered yet.** The signed chain in the bucket and the Iceberg table carry their own format versions, but this promise does not cover them yet.

## Limits

- **Recovery is not yet sub-second.** At 30.6 million edges, a fresh process with a 3 GB heap recovers in 2.3 to 3.7 seconds (3.7 seconds when it also replays 500,000 log frames). The graph lives in native memory, so the live Java heap after load is about 1 MB, down from 1,180 MB before the off-heap move. The remaining time is the snapshot load, which fills native blocks one edge at a time.
- **Synchronous single calls are disk-bound.** On this laptop each one waits about 3.6 ms for its flush. Use batch calls for bulk loads.
- **One writer.** The kernel allows one writer and many readers. Writes on one handle run one at a time.
- **Schema retypes.** A schema version that turns a relation holding tuples into a tupleset relation, or retires it, is refused. Moving the tuples between the two table pairs in the same transaction is not built yet.
- **Failover is manual.** A follower, a restore and a takeover read the chain back, but nothing decides by itself that the writer is dead. A follower that fell behind retention needs a restart. A directory that was written without shipping while a chain already existed, and has trimmed those records, is refused when shipping is turned back on.
- **The reachability index is rebuilt after a restart.** It is not part of the snapshot, so after an open or a follower bootstrap, checks through nested groups use the evaluator, and its depth limit, until the background rebuild finishes. A group whose closure the memory budget or the entry cap refused stays on the evaluator until the nesting changes again.
- **Transactions are bounded.** A transaction must fit one write of 1 MiB. Split larger loads, or use `add_edges_from`, which writes one record per edge.
- **Node keys.** Integer keys are `long` values from 0 to `Integer.MAX_VALUE - 9`. String and UUID keys are mapped to integers and stored in a symbol table, which is not compacted yet.
- **Checkpoints pause writes.** A checkpoint writes the whole graph while the writer waits. It took 3.7 to 5.6 seconds for 30.6 million edges. Checkpoints are therefore not automatic, and an `as_of` build replays everything since the newest snapshot before its target.
- **Watching is per transaction, not per permission.** `AccessWatch` re-checks the triples it is given; listing every object a subject gained or lost access to needs lookup APIs that are not built yet.
- **Time travel stops at the bucket.** `as_of` reaches back as far as the bucket's retention and the newest erasure; older states are only in the Iceberg table, for SQL.
- **Lake reads decode whole files.** A `get` that reaches committed data reads and decodes each file it checks. A key-column cache is the next step.
- **No compaction.** Superseded rows and tombstones stay in the files until something removes them.
- **Python call cost.** Each `ctypes` call takes a few microseconds. Use batch calls for bulk work.
- **One laptop.** The benchmark numbers come from one machine. CI checks that the code builds and passes on three systems. It does not measure speed.

## License and status

Licensed under the [Apache License, Version 2.0](LICENSE).

Status: the graph engine and its durability are built on a typed log and a version 3 snapshot, with an upgrade path from the earlier directory format. Typed tuples, schemas, tokens and permission checks are built on top of it, and readers run in parallel within a handle. The lake write path is built, and so are the release wheels. A durable graph can ship its log to object storage as a signed chain and project it into an Iceberg table. Multiple writers, followers, restore from the bucket, compaction, and the Arrow export are still open.
