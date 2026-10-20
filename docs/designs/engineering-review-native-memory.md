# Engineering review: native memory access in the native library

Status: decisions NM1 to NM10 accepted on 2026-10-20 and built on branch `perf/native-memory`. Choices made while
building are D247 to D252 in [`docs/decisions.md`](../decisions.md); where they differ from this document they win.

The 0.1.0 lake ingested 21,017 rows a second where the README claimed 846,137, flushed 250,000 rows in 9.9 s where it
claimed 0.265 s, and summed 1,000,000 rows in 922.6 ms where it claimed 4.0 ms (D246). The same Java code on a JVM
was fast. This review found why and decided how to fix it.

---

## 1. First principles

1. **Measure the primitive before changing the design.** A hypothesis about scope checks or arenas is cheap to test
   in a twenty-line program compiled the same way as the library.
2. **A memory bug must not crash the host.** The library runs inside someone else's process. An out-of-range read
   throws and becomes an error code; it never reads arbitrary memory.
3. **The output does not change.** A faster writer that writes different bytes is a different writer. Prove it is
   the same one.
4. **A regression that is invisible on the JVM needs a gate that runs the native library.**

## 2. What was found

Every probe below was a standalone program built with GraalVM CE 25.0.2 and the library's own flags
(`-O3 --no-fallback -H:+ForeignAPISupport`), timing 1,000,000 longs.

| Access | JVM | Native image |
|---|---:|---:|
| `MemorySegment.getAtIndex`, any arena (auto, global, shared, confined), a reinterpreted address, a heap segment | 1.7 ns | about 300 ns |
| A `VarHandle` built at image build time, with or without `-H:+ForeignAPISupport` | | 260 to 580 ns |
| Single-element `MemorySegment.copy` | 34 ns | 28 ns |
| `LongBuffer` view | 2.2 ns | 6.3 ns |
| Bulk `MemorySegment.copy` of 4,096 longs into a `long[]` | 6.9 ns | 2.0 ns |
| GraalVM `Pointer.readLong`, address hoisted | | 1.65 ns |
| Field `VarHandle` `setRelease` and `getAcquire` (the seqlock) | 1.6 ns | 1.4 ns |

Every segment accessor goes through a `VarHandle` path that native image does not compile, so each call costs
about 300 ns no matter where the memory lives. The graph kernel reads its adjacency through the same accessors in
`NativeLongArray` and `LowDegreeSlab`, which explains why the graph also slowed down when it moved to native
memory.

A second finding shaped the fix. Community Edition inlines only trivial methods (`MaxNodesInTrivialMethod=20`,
`MaxInvokesInTrivialMethod=1`, 40 nodes for a leaf with no calls). A checked accessor with a fallback call is never
trivial, so it stays a call: about 5.7 ns when the caller passes a cached address and size, and 20 to 25 ns when it
asks the segment for its address, size, liveness and kind on every call. A mode flag read from a class initialized
at run time also stops inlining. Raising the limits with build options measured 1.09 ns, but those options are
documented as being for testing only.

## 3. Decisions

| ID | Topic | Decision |
|---|---|---|
| NM1 | Scope | Fix the lake and the graph kernel in one change: they share the cause |
| NM2 | Approach | GraalVM `Pointer` reads and writes in the native library, `MemorySegment` accessors on the JVM, behind one class, `NativeMemory`. Rejected: bulk copies and buffer views everywhere (slower random access, 2 GB view limit), `sun.misc.Unsafe` (terminally deprecated and prints a warning to the user's stderr) |
| NM3 | Safety | The native path keeps its checks: bounds, a released or heap segment, and a closed arena all throw instead of reading memory |
| NM4 | Regression guard | A test scans the main sources and fails on a direct segment accessor anywhere but `NativeMemory` |
| NM5 | Native test | `nodus_selftest_memory` runs the accessor contract inside the native library, called from the Python suite |
| NM6 | Speed gate | `python/benchmarks/native_ratio.py` adds lake ingest, sum and flush ratios against the JVM |
| NM7 | Hot paths | Beyond the accessor: zero-copy flush slices when no row is a tombstone, INT32 pages written in bulk, whole-word page writes, and a columnar ingest path without per-row staging. The Parquet output must stay byte-identical |
| NM8 | Outside review | Skipped |
| NM9 | Unchecked reads | Deferred to TODOS.md |
| NM10 | Zero-copy Arrow export | Deferred to TODOS.md; its precondition (native columns) is now met |

## 4. Design

```
caller (DeltaMemTable, NativeKeyIndex, encoder, NativeLongArray, ...)
   │ NativeMemory.getLong(segment, address, size, offset)    address and size cached by the owner
   ├── JVM:    segment.get(JAVA_LONG, offset)                checked by the JDK
   └── native: bounds check ──► Pointer.readLong(address + offset)
                    │ out of range: IndexOutOfBoundsException, which the C layer turns into an error code
               Reference.reachabilityFence(segment)          keeps Arena.ofAuto memory alive during the read
```

- **Mode.** `NativeMemory` reads `org.graalvm.nativeimage.imagecode`, which native image sets at build time and at
  run time, and the native build initializes the class at build time so the flag is a constant. The JVM never loads
  the `Pointer` code, so a plain JDK without the GraalVM SDK runs it.
- **Cached addresses.** `NativeColumn` keeps its address and size next to its segment and refreshes them on growth
  and close; a closed column falls back to a 0-byte heap segment and throws. The kernel's `NativeLongArray` and
  `LowDegreeSlab` publish their chunks as one immutable record of segments, addresses and sizes, so a lock-free
  reader always sees a consistent set.
- **Sequential loops** (sums, statistics, selections, dictionary scans) copy 4,096 values at a time into a reused
  heap array with `MemorySegment.copy` and loop over the array, which native image compiles to plain loads.
- **Snappy** copies the page into a reused `byte[]` once and runs the match finder over heap arrays.
- **Sums** read a tombstone count kept by the memtable instead of scanning for tombstones. The chunk size is a
  multiple of four, so each value lands in the same accumulator as before and double sums are bit-identical.
- **Columnar ingest** reads straight from the caller's Arrow buffers through a `ColumnarCursor` built once per batch,
  sharing row preparation with the single-row path. `RowBuffer` is gone.

## 5. Verification

- The Java suite runs both the accessor contract (`NativeMemoryTest`) and the source guard (`AccessorGuardTest`).
- `ParquetGoldenOutputTest` writes 132,000 rows through both ingest paths, with overwrites, tombstones and new keys,
  under both codecs, and compares the SHA-256 of every data and delete file with hashes recorded from the writer
  before this change. Writing one wrong byte in the page sink fails it.
- `python/tests/test_native_memory.py` runs `nodus_selftest_memory` inside the native library.
- `native_ratio.py` runs in CI on Ubuntu, macOS and Windows.

## 6. Not in scope

- Unchecked reads in hot loops (NM9) and the Arrow export (NM10), both in TODOS.md.
- The vector API: native-image support for it is still unverified (D42).
- Re-running the 2.5-hour Pokec suite.
