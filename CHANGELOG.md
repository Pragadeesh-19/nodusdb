# Changelog

## Unreleased

### Fixed

- The native library is much faster. In 0.1.0 every read and write of native memory went through `MemorySegment` accessors, which native image compiles to a call of about 300 ns. They now go through `NativeMemory`, which uses GraalVM pointers in the native library and keeps the JDK accessors on the JVM. The native path still checks bounds and liveness, so a bad access raises an error and never reads arbitrary memory. Measured on the development laptop against 0.1.0:
  - Graph, 1,000,000 edges: flat inserts 162,689 to 810,031 a second, deletes 137,975 to 807,466, the 3-hop median 27.3 to 11.5 µs, common neighbors 15.6 to 8.3 µs.
  - Lake, NYC taxi suite: ingest 21,017 to 994,900 rows a second, columnar upserts 25,092 to 2,002,694 a second, a Snappy flush of 250,000 rows 9.924 to 0.231 s, an uncompressed flush 6.136 to 0.138 s, the in-memory sum and average of a million rows 922.6 to 2.6 ms.
- The Parquet files are byte-identical to those of 0.1.0. A test compares them with hashes recorded from the old writer.
- A lock-free graph reader could see a grown chunk array before its new chunk was set. Chunks are now published as one immutable record.

### Added

- `python/benchmarks/native_ratio.py` also gates lake ingest, sum and flush against the JVM, and both limits drop from 30 to 10.
- `nodus_selftest_memory` and `nodus_lake_probe` in the C interface, for the test suite and the speed gate. They are not part of the API promise.

### Documentation

- The README is rewritten as a front door, and its long sections moved into guides under `docs/`: the graph engine, permissions, the lake engine, operations (durability, shipping, followers, erasure, change feeds, time travel), benchmarks, building and testing, and a first-principles page, `docs/how-it-works.md`.
- `examples/` holds six runnable scripts, and `python/tests/test_examples.py` runs them.
- `CONTRIBUTING.md` describes how to build, test and change the code.

### Known limits found while measuring

- The lake write path in the native library is far slower than the figures reported on 2026-10-04: about 40 times slower on ingest and on a 250,000-row flush, measured on 2026-10-20 against the 0.1.0 wheel. The columns moved from Java arrays to native memory on 2026-10-05 and those figures describe the earlier implementation. The Parquet files themselves are unchanged and DuckDB scans them as fast as before. `bench/baseline/lake-release-windows-dev.txt` has the run, and `python/benchmarks/lake_native_probe.py` times the pieces. The README and `docs/lake.md` no longer quote the earlier figures as current. Fixed above.
- The README's status paragraph listed readers that run in parallel on one handle as open. They run in parallel since the engineering review of 2026-10-06 (D75); only writes, checkpoint and close serialize.
- The README gave 3.7 to 5.6 seconds for a checkpoint at 30.6 million edges. That was the figure before the two-section snapshot. It is about 9 seconds since commit `974ba2d`.

## 0.1.0

The same code as 0.1.0rc2 without the pre-release label. 0.1.0rc2 was published and installed from PyPI on Linux (x86_64 and arm64), macOS (Intel and Apple silicon) and Windows, on Python 3.10 and 3.14, before this release, so this section lists no further changes. The changes since 0.1.0rc1 are under 0.1.0rc2 below.

## 0.1.0rc2

### Added

- Log shipping. `Graph(path=..., shipping=Shipping.s3(...))` or `Shipping.directory(...)` copies the log to an object store as a signed, hash-linked chain, with snapshot references, epoch claims, retention and a backlog cap. The open refuses a directory that is behind the chain or lost unshipped records (`NodusWriterFencedError`) and a store that ignores conditional writes (`NodusUnsupportedError`).
- `durability="lake"` now waits until the write is in the bucket. `wait_shipped(token)` finishes a wait later, and a timeout raises `NodusShipTimeoutError` with the token of the applied write. Without shipping, `lake` still raises `NodusUnsupportedError`.
- `Graph.stats()` reports the shipper, the backlog, retention and the projection, and logs new shipping events to the `nodusdb` logger. `generate_signing_key` writes an Ed25519 key pair.
- An Iceberg v2 table `nodus_log` projected from the shipped log (`iceberg=True`), one row per tuple change with names instead of ids, readable by pyiceberg.
- C exports `nodus_open_durable_shipping`, `nodus_await_shipped`, `nodus_stats_json` and `nodus_signing_key_generate`.
- `python/benchmarks/shipping_ratio.py` compares `add_tuple` with and without shipping in one run. CI fails above a ratio of 3. It measures 1.01 on the development laptop.
- Followers. `Graph(follow=Follower.s3(...))` or `Follower.directory(...)` opens a read-only copy that rebuilds from the newest verified snapshot and follows the chain. Reads raise `NodusStaleReadError` before the first snapshot, a check with `at_least=` waits up to `read_wait_ms`, and an optional `max_staleness_ms` refuses reads once the head was not confirmed for that long. `wait_until_current()` blocks until the follower reaches the head, and `stats()["follower"]` reports the phase, position, failures and events. `state_directory` keeps a checksummed marker of the furthest object seen so a rolled-back bucket is refused.
- `restore(follower, directory)` rebuilds a graph directory from the bucket. The directory appears by one atomic rename, and the target must not exist or must be empty.
- `Graph(path=..., shipping=..., takeover=True)` fences the old writer, restores into the new directory and opens it as the writer, waiting until the chain holds its first object. `graph.takeover_report` has the claimed epoch, the new epoch, the handoff LSN and the attempts.
- `salvage(follower, directory)` lists, with names, the transactions an old writer's directory holds above the point where the chain moved on, without writing to it.
- Error code -15, `NodusChainTrustError`, for chain data that fails verification.
- C exports `nodus_open_follower`, `nodus_restore`, `nodus_takeover` and `nodus_salvage_json`. `nodus_stats_json` reports follower state on a follower handle.
- A seeded simulation of writers, followers, retention, restarts and fencing over a faulty store (300 seeds in CI), and chaos scenarios that lose, cut and delay S3 replies through a socket proxy.
- A reachability index for nested groups. The kernel keeps the transitive closure of userset nesting (`group#member@group:x#member`) up to date as tuples change, and a check through nested groups is answered by intersecting that closure with the subject's direct memberships instead of walking the groups. Answers are unchanged; groups whose update would cost too much are answered by the evaluator until a background thread rebuilds them. A group answered by the index no longer counts toward the depth limit of 32. `stats()["reachability"]` reports the index, and `NODUS_REACHABILITY_INDEX=off` turns it off.
- Erasure. `Graph.erase(name)` removes every tuple that names the subject or object and replaces the name with a salted pseudonym, `erased:` and 16 hex digits, in memory and on every follower. It returns a token. The copies on disk are scrubbed in the background: the graph directory by a checkpoint and a log trim, the bucket by a signed `REDACTION` entry followed by deleting the chain objects and snapshots from before the erasure, and the Iceberg table by an overwrite that rewrites the affected data files and expires every older snapshot. `Graph.wait_erased(token, timeout=None)` waits for all of them, `stats()["erasure"]` reports each layer, and `Shipping(erasure_checkpoint_delay_s=60)` sets how long erasures are collected before a checkpoint. A name written again after its erasure is a new subject with no grants.
- Error code -16, `NodusErasurePendingError`, when `wait_erased` runs out of time. It names the pending layers and carries the token.
- C exports `nodus_erase` and `nodus_await_erased`.
- Watching changes. `Graph.changes(after, limit=1000, timeout=0)` returns the changes committed after a token, as whole transactions in log order with names resolved when read, and the cursor to read from next; `Graph.watch(after)` yields them in a loop. Recent changes come from a ring in memory (`Graph(watch_buffer_changes=...)`), older cursors from the writer's log and from the bucket's chain, on writers and followers alike. `AccessWatch(graph, checks)` re-checks registered `(object, permission, subject)` triples after each batch and reports the ones whose answer changed. `stats()["watch"]` reports the ring and the readers.
- Time travel. `Graph.as_of(token_or_datetime)` opens a read-only `PastGraph` rebuilt from the bucket, on writers and followers, standing on the last commit at or before the target; `PastGraph.advance(later)` moves it forward. Views have their own memory limit and share `history_max_memory_mb` (`Shipping` and `Follower`); `stats()["history"]` reports them and a view's `stats()["as_of"]` reports its build.
- Error code -17, `NodusHistoryExpiredError`, when a watch cursor or an `as_of` target is older than the history that is still kept, or older than the newest erasure.
- C exports `nodus_changes`, `nodus_watch_buffer`, `nodus_open_as_of`, `nodus_past_advance` and `nodus_past_position`.
- A compatibility promise for 0.1.x, in the README: the graph directory format does not change within 0.1.x, a directory from rc1 opens only after `upgrade()`, and the Python API only grows. A golden directory in the tests pins the format.
- `nodusdb.__version__`, a PyPI description, and the Apache-2.0 license, a `NOTICE` and the license of the GraalVM runtime inside every wheel.

### Changed

- The wheels are one `py3-none-<platform>` file per platform instead of one per Python version. They install on every CPython from 3.10 on, including 3.14, and Python 3.9 is no longer supported.
- The native library in the wheels is built with GraalVM Community Edition 25. The development status is Beta.
- A snapshot reference is committed to the chain only once the chain holds every record up to its LSN, and a transaction that erases a name always starts a new chain object.

### Fixed

- Every C-ABI call on one graph handle holds that handle's monitor. Before, concurrent calls could overwrite each other's query results, and concurrent batch writes could return wrong counts.
- Calls on a closed session fail with an error. Closing twice is safe.

### Known limits

- Calls on one handle run one at a time. Readers do not run in parallel within a handle yet.
- Failover is manual: nothing decides by itself that the writer is dead. A follower that fell behind retention stops and needs a restart. Salvage loads the old directory's snapshot into memory. The Iceberg manifest list grows by one entry per commit.
- A `lake` write waits 30 seconds at most through the C interface.
- The in-memory write path is slower than the figures the README gave on 2026-10-03: a flat insert of one million edges runs at about 165 thousand edges per second where the old code reached 1.0 to 1.3 million, and the 3-hop median is about 27 µs where it was 10.5 µs. The README table now holds the figures for this release.

## 0.1.0rc1

First release candidate. The API may change before 0.1.0.

### Added

- Graph engine: sparse-set adjacency, k-hop traversal, common neighbors, and set intersection, in a Java kernel behind a native C ABI and a Python `Graph`.
- Durability: write-ahead log, checkpoints, and recovery. `sync` mode returns only after the write is on disk.
- String and UUID keys, mapped to dense ids and kept in a symbol table.
- Lakehouse writes: `LakeTable` with Parquet output (Snappy or uncompressed), plus sums and averages over the active buffer.
- Release wheels for Linux (glibc 2.34 or newer), macOS (11.0 or newer), and Windows (x86_64), for CPython 3.9 to 3.13.

### Fixed

- `Graph` and `LakeTable` calls are serialized with a lock, so threads can share one object.
- Each OS thread attaches its own GraalVM isolate thread. Before, all objects in a process shared one.

### Known limits

- One writer. A C handle must not be used from two threads at once.
- Recovery at 30.6 million edges takes about 22 seconds, nearly all of it loading the snapshot.
- A checkpoint pauses writes while it runs.
- Lake files are not compacted. Iceberg metadata and the Arrow export are not built yet.
- The Linux wheel smoke test runs on CPython 3.12 only. The other versions are built from the same library but not run in CI.
