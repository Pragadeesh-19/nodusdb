# Changelog

## Unreleased

### Added

- Log shipping. `Graph(path=..., shipping=Shipping.s3(...))` or `Shipping.directory(...)` copies the log to an object store as a signed, hash-linked chain, with snapshot references, epoch claims, retention and a backlog cap. The open refuses a directory that is behind the chain or lost unshipped records (`NodusWriterFencedError`) and a store that ignores conditional writes (`NodusUnsupportedError`).
- `durability="lake"` now waits until the write is in the bucket. `wait_shipped(token)` finishes a wait later, and a timeout raises `NodusShipTimeoutError` with the token of the applied write. Without shipping, `lake` still raises `NodusUnsupportedError`.
- `Graph.stats()` reports the shipper, the backlog, retention and the projection, and logs new shipping events to the `nodusdb` logger. `generate_signing_key` writes an Ed25519 key pair.
- An Iceberg v2 table `nodus_log` projected from the shipped log (`iceberg=True`), one row per tuple change with names instead of ids, readable by pyiceberg.
- C exports `nodus_open_durable_shipping`, `nodus_await_shipped`, `nodus_stats_json` and `nodus_signing_key_generate`.
- `python/benchmarks/shipping_ratio.py` compares `add_tuple` with and without shipping in one run. CI fails above a ratio of 3. It measures 1.01 on the development laptop.

### Fixed

- Every C-ABI call on one graph handle holds that handle's monitor. Before, concurrent calls could overwrite each other's query results, and concurrent batch writes could return wrong counts.
- Calls on a closed session fail with an error. Closing twice is safe.

### Known limits

- Calls on one handle run one at a time. Readers do not run in parallel within a handle yet.
- Shipping is one-way. Nothing restores a graph from the bucket yet, and the Iceberg manifest list grows by one entry per commit.
- A `lake` write waits 30 seconds at most through the C interface.

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
