# Architecture

This document describes how the engine is laid out in memory and on disk. The source files carry no
comments, so the structure and the reasons behind it are written here. Design decisions and their
rationale are in `docs/designs/`.

## Packages

```
io.nodusdb.kernel                    GraphKernel, the public engine; KeyKind; NodeIds
io.nodusdb.kernel.memory             off-heap allocation primitives and the memory budget
io.nodusdb.kernel.adjacency          per-node neighbor storage (slab, pool, sparse sets, node table)
io.nodusdb.kernel.traversal          k-hop traversal
io.nodusdb.kernel.symbols            string interner
io.nodusdb.kernel.index              on-heap index structures used as oracles and benchmarks
io.nodusdb.kernel.wal                write-ahead log, snapshot and recovery
io.nodusdb.capi                      the C interface exported by the native library
io.nodusdb.lake                      the lakehouse write path
```

## Chunked off-heap storage

Growth never moves existing memory, so a reader holding a segment stays valid while its arena is
reachable. Storage is a list of chunks, each twice the size of the one before it.

```
ChunkLayout, first = size of chunk 0 (a power of two)

  chunk 0   indices [0, first)                    size first
  chunk c   indices [first*2^(c-1), first*2^c)    size first*2^(c-1), c >= 1
```

`NativeLongArray` is a grow-only array of longs on this layout. Reads past the allocated range return
the default value, so a reader racing a writer never throws. A growth request charges the bytes of every
chunk it needs to the `MemoryBudget` before it allocates any, so a request that does not fit leaves the
array and the budget as they were.

`MemoryBudget` counts the native bytes of one kernel and refuses a request that would pass the limit.
Chunks are never returned, so the count only grows. One budget is shared by every structure of a
kernel. Only the writer charges, and the count is volatile so other threads can read it.

## Low-degree slab

A node with fewer than 16 neighbors keeps them in one fixed block of 16 longs (128 bytes), and every
block starts on a 64-byte boundary.

```
chunk 0   blocks [0, first)               size first
chunk 1   blocks [first, 2*first)         size first
chunk 2   blocks [2*first, 4*first)       size 2*first
chunk 3   blocks [4*first, 8*first)       size 4*first
```

Free blocks form an intrusive stack. A freed block's first long holds the next free block id plus one,
and zero ends the stack. Allocation flags live in a parallel byte per block.

## Block pool and sparse sets

`NativeBlockPool` hands out power-of-two blocks of longs with one free list per size class.

```
word:    ... [ header ] [ payload 0 .. 2^log-1 ] [ header ] [ payload ... ]
handle:  word index of payload 0, always a multiple of LINE_WORDS
```

The arena aligns every chunk to 64 bytes and chunk starts are multiples of `LINE_WORDS`, so every payload
begins on a cache line. A header holds `log` while the block is allocated and `~log` while it is free. A
freed block's first payload word holds the next free handle. A released block returns only to its own
size class, so classes cannot fragment one another. Readers call `logWordsOf`, which answers -1 for any
handle that is not a live block.

`NativeSparseSet` is a set over one pool block. For capacity C, a power of two, the payload is 2C words.

```
words [0, C)     dense keys, positions 0 .. degree-1 are live
words [C, 2C)    position table: 2C ints packed two per word, each holding position + 1
                 so that zeroed memory is an empty table
```

A table slot holds a position into the dense array. A lookup reads the position and compares the key
stored there, so each key is stored once. Deletion shifts later probe-chain entries back, so the table
holds no tombstones. The set holds no state: the degree lives with the caller and the handle names the
block. Capacity is padded up to `MIN_CAPACITY` so the payload fills whole cache lines.

## Node table and adjacency

`NodeTable` keeps one 64-bit slot per node. The high 32 bits hold the degree. The low 32 bits hold a slab
block id, or a pool handle when the top bit is set. A zero degree with no block is the empty slot.

`AdjacencyTable` combines the slab, the pool, the sparse sets and the node table for one direction.

```
degree < 16        keys live in one slab block                       (low-degree state)
degree >= 16       keys live in a sparse set from the pool           (high-degree state)
promote at 16, demote at 8: degrees 9 to 15 keep whichever tier the node already holds
```

Writes follow one order: validate, reserve memory, journal, mutate. `reserveForAdd` mirrors the three
places `add` allocates (the first slab block, promotion at degree 15, and growth of a full set), so a
refusal by the budget surfaces before the graph changes. Deletes never fail: a set demotes only when the
slab can supply a block, and otherwise stays a set at the lower degree.

## Open addressing

Tables are power-of-two sized and indexed by `mask = size - 1`. Deletion uses backward shift (Knuth,
TAOCP 6.4, Algorithm R). Walking the cluster behind a hole, an entry may move into the hole only when the
hole lies on that entry's probe path from its home slot. `OpenAddressing.canMoveInto` encodes the test
with wrap-around arithmetic. A test that ignores wrap-around moves entries too early or too late across
the end of the table.

`PositionIndex` stores dense positions only; the keys live in the owning set's dense array. A slot costs
4 bytes where a long key with an int value costs 12. Readers call `find` while a writer may be mid-update,
so `find` reads the table once into a local and bounds-checks every access. A torn read yields a wrong
answer that the seqlock discards, never an exception.

`IndexedSparseSet` is the on-heap form: a packed dense array with a `PositionIndex` sized at twice the
dense length and rebuilt whenever the array doubles, so its load stays at or below 0.5. It serves as the
oracle for the native set and as the baseline in the microbenchmarks.

## Concurrency

One writer, many readers. Every mutation runs between `beginWrite` and `endWrite`, which move a sequence
counter to odd and back to even. A reader records the counter, runs its query, and retries when the
counter changed or was odd. Readers take no lock.

Bulk load runs in two steps. `prepareBulkLoad` runs once on one thread and sizes every node. `loadBulkNode`
then fills nodes, and calls for distinct nodes may run in parallel. Neither step takes the write sequence,
so the kernel must not be shared with readers until every fill has returned.

## String interner

An append-only map between UTF-8 strings and dense ids `0 .. size-1`. Ids follow first-seen order and
never change.

```
slab      all string bytes
starts    id -> offset into the slab        lengths   id -> byte length
hashes    id -> hash of the string
buckets   open addressing over ids, storing id + 1
```

The bucket table grows once it is half full. Growth rebuilds buckets from the stored hashes, so no string
is rehashed.

## Write-ahead log and snapshot (version 1)

```
header    magic "NODU" | version | reserved | created
frame     op u8 | reserved | padding | crc32 | u i64 | v i64          24 bytes, big endian
```

Snapshot version 2 lists every node in id order, forward section then backward section:

```
header    magic "NODS" | version 2 | reserved | nodeCapacity | edgeCount | created
forward   for node in [0, nodeCapacity): degree (int) | neighbors (long x degree)
backward  same
trailer   CRC32 over every byte before it
```

Both sections are written from the live kernel, so the loader builds each node at its exact degree and
never scatters edges into place. Version 1 snapshots hold only the forward section and load edge by edge.

Symbol log, kept beside the graph log:

```
header   "NSYM" | version u8 | key kind u8 | reserved u16                      8 bytes
record   length u32 | crc32c u32 | utf8 bytes                                  repeated
```

Records are written in id order. A record is forced to disk before any edge that uses its id is written.
A torn final record is truncated, because no edge can depend on it.

## Lake write path

`DeltaMemTable` is a columnar write absorber keyed by a 64-bit key hash. Rows `0 .. rowCount-1` are always
dense.

```
keyHashes          [k0 k1 ... k(n-1) | free]     native, 64-byte aligned
rowKinds           INSERT or TOMBSTONE per row
longColumns[c]     values; doubles stored as raw bits
intColumns[c]      values
varCharOffsets[c]  offsets into varCharSlab        varCharLengths[c]   lengths
varCharSlab        [live bytes | dead bytes | free]
index              keyHash -> row, the only structure that maps keys to rows
```

`upsert` writes an INSERT row. `tombstone` marks a key as deleted without removing it, so the deletion can
be written to a delete file if an older committed row exists. `delete` removes the row outright with
swap-and-pop across every column.

`NativeKeyIndex` stores row positions only; keys live in the key column. Slots store `row + 1` so zeroed
memory is an empty table. Deletion uses backward shift and the load factor stays at or below one half.

`NativeColumn` is a growable byte buffer with power-of-two capacity whose first byte sits on a 64-byte
line. Growth copies into a new buffer, so a segment read before a growth must not be used after it. Memory
comes from GC-managed arenas, because GraalVM native image does not support closing shared arenas.

`ColumnChunkEncoder` writes one column chunk: an optional dictionary page, then one data page, each
compressed with the table's codec. A column is dictionary-encoded only when the dictionary and its indices
are smaller than the plain values and it has at most `MAX_DICTIONARY_VALUES` distinct entries.

`ColumnarRows` is the column-oriented input for `upsertColumns`. Each variable-width column follows Arrow's
layout: value i spans bytes `[offsets[i], offsets[i + 1])` of its data segment, and offsets are absolute
positions in that data so a slice of a larger buffer needs no copy.

`SnappyCompressor` compresses a native segment into a `NativeSink`. Its match table lives in native memory
and is reused across calls, with entries holding `position + 1` so a zeroed table means no candidate.
