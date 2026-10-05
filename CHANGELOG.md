# Changelog

## Unreleased

### Fixed

- Every C-ABI call on one graph handle holds that handle's monitor. Before, concurrent calls could overwrite each other's query results, and concurrent batch writes could return wrong counts.
- Calls on a closed session fail with an error. Closing twice is safe.

### Known limits

- Calls on one handle run one at a time. Readers do not run in parallel within a handle yet.

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
