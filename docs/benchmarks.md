# Benchmarks

Every number in this project's documentation comes from a measurement that is recorded in the repository, and this page says where, how it was taken, and what it does and does not show.

## Read this first

- **Everything ran on one laptop.** An AMD Ryzen 7 5700U with 8 cores and 16 threads, 7.3 GB of memory, Windows 11, with the clock boost left on. The machine and the commands are in [`bench/baseline/MACHINE.md`](../bench/baseline/MACHINE.md). Numbers from other hardware are not comparable. CI builds and tests the code on Ubuntu, macOS and Windows but does not measure speed, except for a ratio gate (see below).
- **Most results are single runs.** Where several runs exist, the text says how far they agreed.
- **Different suites ran at different times with different builds of the library.** Each table says when it was taken. The newest, the 1-million-edge comparison and the lake suite, use the library the release wheels carry (GraalVM Community Edition 25.0.2). The Pokec graph suite and the durability run used an earlier build (GraalVM CE 22.0.2) and were not re-run for 0.1.0.
- **The code got slower per call between 2026-10-03 and 0.1.0.** This is measured, not guessed, and it is the first open item under "Release and packaging" in [TODOS.md](../TODOS.md). See [The write path got slower](#the-write-path-got-slower).
- **We list where we lose.** Each comparison says which engine is faster and by how much.

## Graph: 1,000,000 edges against NetworkX and SQLite

`python/benchmarks/compare.py`, run on 2026-10-18 with the native library from the release wheels. It loads 1,000,000 directed edges over 200,000 nodes, deletes 500,000 of them, and runs 200 queries of each kind. Every backend is checked for identical answers before timing starts.

| Configuration | Insert edges/s | Delete edges/s | 3-hop median | Common neighbors median |
|---|---:|---:|---:|---:|
| NodusDB, flat `array('q')` | 162,689 | 137,975 | 27.3 µs | 15.6 µs |
| NodusDB, batch of tuples | 132,281 | 119,379 | 27.3 µs | 15.7 µs |
| NodusDB, single-edge calls | 71,962 | 66,951 | 27.3 µs | 19.0 µs |
| NetworkX, single | 149,164 | 324,683 | 35.4 µs | 6.1 µs |
| NetworkX, batch | 136,750 | 307,860 | 41.2 µs | 9.1 µs |
| SQLite, `executemany` | 133,632 | 132,545 | 113.6 µs | 23.3 µs |
| SQLite, one statement per row | 101,553 | 100,175 | 114.1 µs | 23.3 µs |

- Inserts are close to NetworkX and SQLite. The flat buffer loads 1.2 times as fast as NetworkX's batch insert and as SQLite's `executemany`.
- NetworkX removes edges 2.2 to 2.4 times as fast as NodusDB's flat buffer does.
- The 3-hop median is 1.3 to 1.5 times faster than NetworkX and about 4 times faster than SQLite.
- NetworkX is faster on common neighbors (6.1 and 9.1 µs against 15.6 µs).

Three runs of the NodusDB insert rate agreed within 5%.

### The write path got slower

An earlier version of this table, measured on 2026-10-03 at commit `0b05475` (before typed log records, native-memory storage, the memory budget and the change feed), reported 1.25 million inserts per second and a 10.5 µs 3-hop median. That commit, rebuilt with GraalVM CE 22.0.2 and run on the same laptop on 2026-10-18, measured 1.06 to 1.30 million inserts per second and 10.3 to 10.7 µs. The code in 0.1.0 is slower on this benchmark: 6.5 to 8 times on inserts and about 2.6 times on the 3-hop median.

A native-image build runs the insert path 11 to 13 times slower than the same code on a JVM on this laptop (`python/benchmarks/native_ratio.py`). In CI the ratio is 5.8 to 7.7 for `add_edge` and 12.5 to 14.2 for `has_edge`, and the gate fails above 30. The cause of the slowdown since 2026-10-03 is not found, and finding it is open work.

### Which toolchain builds the library

The same sources built by three GraalVM toolchains (`bench/baseline/release-windows-dev.txt`):

- CE 25.0.2 and CE 22.0.2 agree within 5% on this laptop.
- CE 25.0.1 is up to 10% slower on a lookup.
- Oracle GraalVM 25 takes about a third less time per call, but its free license allows redistributing the compiled library only without a fee for products that bundle it, so the Apache-2.0 wheels use the Community Edition. The wheel is also 24.5 MB against 13.5 MB.
- A local build with CE 23 ran `add_edge` in 1.2 ms where CE 22.0.2 took 9 µs, because memory-segment access was not optimized. Time a new toolchain before you trust it.

### Start-up cost

Importing `nodusdb` takes 174 ms and the first `Graph()` another 94 ms on this laptop, most of it loading the native library and creating the GraalVM isolate.

## Graph: microbenchmarks

JMH, `-prof gc`, recorded in `bench/baseline/windows-dev.json`.

| Benchmark | Result |
|---|---|
| `GraphTraversalBench` common neighbors | 499 ns (0.30 µs on 2026-10-03) |
| `GraphTraversalBench` 3-hop | 21.2 µs (15.2 µs on 2026-10-03) |
| `ChurnBench`, alternating add and remove | 0 B/op allocation in steady state |
| Native speed ratio, `add_edge` and `has_edge` | 5.8 to 7.7 and 12.5 to 14.2 across CI (gate 30) |

Reading `gc.alloc.rate.norm`: a true per-operation allocation scales with throughput. JMH's background allocation shows as a constant `gc.alloc.rate` of about 0.006 MB/s across benchmarks, so a per-operation value that rises as throughput falls is harness noise, not kernel allocation. Check `gc.count` too.

## Graph: the real-data suite (SNAP soc-Pokec)

Two suites compare NodusDB with other engines on real data. This one runs on the SNAP soc-Pokec social network: 30,622,564 directed edges. Each engine and workload runs in its own process. The answers are cross-checked: the permission digests and the Pokec hub degrees agree across every engine that finished. It ran on 2026-10-04 with a native library built by GraalVM CE 22.0.2, before the authorization work, and has not been re-run. The raw output is [`bench/baseline/graph-heavyweights-windows-dev.json`](../bench/baseline/graph-heavyweights-windows-dev.json), and the suite takes about 2.5 hours.

- **A, load and traverse.** Load all 30.6 million Pokec edges. Then time 2-hop and 3-hop queries from 100 median-degree nodes and from 50 hub nodes.
- **B, churn.** Preload 200,000 edges among the top 50,000 Pokec nodes. Run 100,000 operations: 60% inserts, 30% deletes, and 5% each of 2-hop and 3-hop queries. Each engine gets a 1,200-second budget.
- **C, permissions.** A synthetic relationship-based access graph with 10,000 users, 2,000 groups, 50,000 resources and 81,500 membership edges. Then 9,936 membership updates and 10,000 reachability checks. About half the checks succeed. This workload is generated, not real data.

Times are medians unless noted.

**Workload A, Pokec, 30.6 million edges**

| Engine | Ingest, edges/s | 2-hop | 3-hop | Hub 2-hop | Hub 3-hop | Peak RSS, MB |
|---|---:|---:|---:|---:|---:|---:|
| NodusDB, in memory | 181,345 | 56.8 µs | 1.21 ms | 6.8 ms | 96.6 ms | 4,404 |
| NodusDB, durable (async) | 163,841 | 129.8 µs | 1.25 ms | 8.9 ms | 101.6 ms | 4,240 |
| igraph | 92,916 | 0.84 ms | 7.50 ms | 12.0 ms | 106.4 ms | 2,464 |
| DuckDB | 158,027 | 13.5 ms | 68.0 ms | 58.0 ms | 766.6 ms | 3,420 |
| SQLite | 19,052 | 0.95 ms | 49.8 ms | 174 ms | 12.9 s | 917 |

On disk after ingest, the durable graph takes 258 MB, SQLite 822 MB and DuckDB 1,458 MB. Kùzu did not finish its bulk load, and NetworkX is skipped above 3 million edges.

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

### Where NodusDB wins

- **High-churn mutation.** Workload B runs at 81,525 operations per second. The next fastest engines are NetworkX at 55,451 and SQLite at 3,617. An in-memory edit takes about 5 µs, and an igraph edit takes about 100 ms.
- **Traversal on a graph held in memory.** On Pokec the median 2-hop query takes 57 µs and the median 3-hop query takes 1.2 ms. Every other engine that finished workload A is slower on both: igraph takes 0.84 ms and 7.5 ms, SQLite 0.95 ms and 49.8 ms, DuckDB 13.5 ms and 68.0 ms. On hub 3-hop queries, NodusDB and igraph are close, at 96.6 ms and 106.4 ms.
- **Permission checks.** The median check takes 38 µs. NetworkX takes 171 µs and SQLite 400 µs.
- **Ingest and footprint.** The in-memory graph loads faster than DuckDB or igraph. The durable graph loads about 10% slower than in memory and uses less disk than either SQLite or DuckDB.

### Where the other engines win

- **Single updates in workload C.** NetworkX applies a membership update in 7.4 µs, against 9.5 µs for NodusDB. In workload B the median insert is a tie, 4.6 µs against 4.7 µs.
- **Whole-graph analytics.** No workload here reads every node and edge, as PageRank or connected components do. Columnar engines such as Kùzu and DuckDB are built for that work, and this suite does not measure it.

### Caveats

- Kùzu's bulk load failed in workload A with `Buffer manager exception: ... The buffer pool is full`. It ran at default settings everywhere. The buffer pool was not tuned, so its numbers may change with configuration.
- Kùzu and igraph hit the 1,200-second budget in workload B. Their rates count only the operations they completed.
- Peak RSS in workload A includes the Pokec data that each worker loads. Use the on-disk figures to compare footprint.
- Traversal on Pokec is not sub-microsecond. The design targets sub-microsecond k-hop queries, and this suite does not reach that target. The smaller benchmarks (21 to 27 µs) do not reach it either.
- These numbers describe the engine as it was on 2026-10-04. The first table on this page shows that its per-call cost has changed since.

## Graph: durability on Pokec

See [Operations](operations.md#real-data-numbers) for the table. In short: 1.25 million edges per second in memory, 1.04 to 1.17 million durable with batches, 203 per second for single synchronous calls (the disk's flush time, about 3.6 ms), a checkpoint of about 9 seconds, and recovery in about 3 seconds in a fresh process. After recovery the degrees, in-degrees and 3-hop results match the state before the crash, by digest.

A recovery timed inside the process that still holds the original graph measured garbage collection, not recovery (185 s and 131 s runs). Recovery is measured only in a fresh process.

## Permissions and nested groups

`bench/baseline/reachability-windows-dev.json`, JMH on the development laptop:

| Check | With the reachability index | Without |
|---|---:|---:|
| Against the root of a tree of 1,111 nested groups | about 0.7 µs | 110 to 116 µs |
| Through a chain of 30 nested groups | 0.65 µs | 2.5 to 3.5 µs |
| Nesting and unnesting a leaf group (two writes) | 7.7 µs | 2.2 µs |

Adding and removing a direct member costs the same either way. The index trades slower nesting changes for much faster deep checks.

## Shipping, followers, erasure, watch and time travel

| Measure | Result | Where |
|---|---|---|
| Gate the shipper adds to one log append | 56.0 ns against 18.9 ns without, no allocation | `shipping-windows-dev.json` |
| End to end: `add_tuple` with and without shipping | ratio 1.01 (CI fails above 3) | `python/benchmarks/shipping_ratio.py` |
| Follower applying tuples, one core | about 2.5 million per second | `FollowerApplyBench`, `follower-windows-dev.json` |
| Erasing a subject with 1,000 grants | 2.8 ms | `EraseBench`, `erasure-windows-dev.txt` |
| Erasing a subject with 100,000 grants | 51 ms | `EraseBench` |
| Commit with a watcher attached, against the previous release | +7.7% (about 19 ns) | `watch-windows-dev.txt` |
| Reading a change from the ring | 126 ns | `ChangeFeedBench` |
| Reading a change from the log | 1.78 µs | `ChangeFeedBench` |
| Opening a past view of 10,000 grants | 95.6 ms from a snapshot, 99.8 ms by replay | `AsOfBench` |
| Opening a past view of 100,000 grants | 260 ms from a snapshot, 373 ms by replay | `AsOfBench` |

## Lake

`python/benchmarks/benchmark_lake_heavyweights.py` runs `LakeTable` against DuckDB, PyArrow and SQLite on the first 1,000,000 rows of the NYC TLC Yellow Taxi file for January 2024 (the file's SHA-256 is checked before use). Rows are keyed by row index. Each engine and workload runs in its own process. DuckDB's in-memory table is written with `COPY`, PyArrow's is a Python dict of rows written with `pq.write_table`, and SQLite is in memory and dumped with the backup API.

- **W1, ingest.** Load all 1,000,000 rows.
- **W2, mutations.** 500,000 upserts and 100,000 deletes. NodusDB runs the upserts two ways: one call per row, and one Arrow batch.
- **W3, flush.** Write a 250,000-row buffer to Parquet.
- **W4, read.** A DuckDB query totals revenue and trips over the files each engine wrote. Every source gives the same revenue total, 27,486,629.10.

### The current run: 2026-10-20, the 0.1.0 library

The native library from the 0.1.0 wheel, Python 3.12.7, pyarrow 26.0.0, DuckDB 1.5.6. Raw output: [`lake-heavyweights-release-windows-dev.json`](../bench/baseline/lake-heavyweights-release-windows-dev.json), with the tables and the analysis in [`lake-release-windows-dev.txt`](../bench/baseline/lake-release-windows-dev.txt).

| Measure | NodusDB | DuckDB | PyArrow | SQLite |
|---|---:|---:|---:|---:|
| W1 ingest, rows/s | 21,017 | 147,314 | 547,340 | 285,608 |
| W2 upserts/s, one call per row | 16,632 | did not finish | 329,166 | 243,167 |
| W2 upserts/s, one Arrow batch | 25,092 | | | |
| W2 deletes/s | 97,090 | did not finish | 710,651 | 259,898 |
| W3 flush of 250,000 rows, Snappy | 9.924 s | 1.124 s | 0.194 s | 0.092 s (database file) |
| W3 flush of 250,000 rows, no compression | 6.136 s | | | |
| W4 grouped scan of the files, median | 13.4 ms | 15.0 ms | 17.8 ms | n/a |
| W4 grouped scan, p99 | 40.4 ms | 16.2 ms | 22.0 ms | n/a |
| W4 NodusDB in-memory sum and average, 1M rows | 922.6 ms | 7.9 ms (over files) | | |

DuckDB's update loop did not finish: its worker had used about 35 minutes of CPU when it was stopped by hand, where the same step took about 9 minutes in the 2026-10-04 run. File sizes: NodusDB 3.95 MB (Snappy) or 5.40 MB (none), DuckDB 3.66, PyArrow 3.96, SQLite 11.85.

What this shows:

- **The files are fine.** NodusDB's output is the same size as before, DuckDB reads it in 13.4 ms against 15.0 ms for DuckDB's own files, and every source returns the same revenue total. The 5,000-row streaming layout produced 101 files and scanned in 25.6 ms, about twice the clean layout, because each file is a separate scan task.
- **The write path is not.** NodusDB is slower than PyArrow and SQLite on every write measure and slower than DuckDB on ingest and flush: 7 to 26 times slower on ingest and 9 to 108 times slower on the flush. Against its own 2026-10-04 run, ingest is 40 times slower, the flush 37 times slower, the Arrow batch upsert 70 times slower and the native sum 230 times slower.
- **The native aggregate is 117 times slower than DuckDB over files**, where on 2026-10-04 it was twice as fast.
- **The tail is worse than DuckDB's.** The clean layout's p99 is 40.4 ms against 16.2 ms.

### The previous run: 2026-10-04, a different implementation

The table below is what the project reported before. It measured the lake when its columns lived in Java arrays. The columns moved to native memory on 2026-10-05 (commits `c099214` to `d823261`), and none of these write-side figures describe 0.1.0. Raw output: [`lake-heavyweights-windows-dev-v2.json`](../bench/baseline/lake-heavyweights-windows-dev-v2.json).

| Measure | NodusDB | DuckDB | PyArrow | SQLite |
|---|---:|---:|---:|---:|
| W1 ingest, rows/s | 846,137 | 164,793 | 114,582 | 95,113 |
| W2 upserts/s, one call per row | 39,335 | 960 | 393,661 | 253,594 |
| W2 upserts/s, columnar batch | 1,763,882 | | | |
| W2 deletes/s | 467,542 | 930 | 755,709 | 269,022 |
| W3 flush of 250,000 rows, Snappy | 0.265 s | 1.143 s | 0.187 s | 0.123 s |
| W3 flush of 250,000 rows, no compression | 0.187 s | | | |
| W4 grouped scan, median | 12.8 ms | 14.0 ms | 17.7 ms | n/a |
| W4 native sum and average in memory, 1M rows | 4.0 ms | 7.7 ms (over files) | | |

The design target those numbers showed is still the target. Other engines ran at different speeds across the two runs (PyArrow ingest 114,582 against 547,340, for example), because the environments differ.

### Microbenchmarks

`DeltaMemTable` on a pre-sized table, in-place upserts (`DeltaMemTableBench`, JMH, `windows-dev`, JVM): 227 ns per upsert in the recorded baseline, about 4.4 million per second. A smoke run on 2026-10-20 (one fork) gave 299 ns. `LakeTableBench.upsertWhileFlushing` with 65,536-row buffers measured 998 ns per upsert on 2026-10-05 (`lake-flush-windows-dev.json`). Both ran on a JVM and say nothing about the native library.

### Where the time goes

A library built on 2026-10-07 with GraalVM CE 22 is as slow as the wheel's, so the toolchain is not the cause. The same Java code on a plain JVM is fast. `python/benchmarks/lake_native_probe.py` times the pieces through the wheel on 200,000 rows: an Arrow batch in 2.6 s (78,000 rows/s), a native sum in 97 ms (about 485 ns per element), a single upsert in 25.6 µs, a flush in 6.6 s. The pattern points at memory-segment access that the native-image compiler does not optimize in loops over native columns. It is the first lake item in [TODOS.md](../TODOS.md).

### Caveats

- W1 and the single-call W2 path pass each row from Python into the native library, so they include that cost. The Arrow batch path does not.
- PyArrow's W2 figures come from a Python dict with no storage behind it. They show a ceiling for an in-memory map, not a storage engine.
- The native aggregate covers only the rows in the active buffer. It does not read committed files.
- Peak RSS includes the Python process and the loaded source table, so compare it as a relative figure.
- Each engine ran once, on one laptop.

## Reproducing

Commands are in [Building and testing](building.md#benchmarks). Record the machine with the result, and compare two runs on the same hardware.
