# Architecture

This document describes how the engine is laid out in memory and on disk. The source files carry no
comments, so the structure and the reasons behind it are written here. Design decisions and their
rationale are in `docs/designs/`.

## Packages

```
io.nodusdb.kernel                    GraphKernel, the public engine; the record applier and write reservation
io.nodusdb.kernel.memory             off-heap allocation primitives and the memory budget
io.nodusdb.kernel.adjacency          per-node neighbor storage (slab, pool, sparse sets, node table), edge keys
io.nodusdb.kernel.traversal          k-hop traversal
io.nodusdb.kernel.symbols            the off-heap symbol table
io.nodusdb.kernel.catalog            the relation catalog a schema produces
io.nodusdb.kernel.concurrency        the seqlock
io.nodusdb.kernel.index              on-heap index structures used as oracles and benchmarks
io.nodusdb.error                     the error contract: one exception per code
io.nodusdb.log                       the typed log: segments, recovery, group commit
io.nodusdb.log.record                the record codec
io.nodusdb.log.io                    the file seam the crash simulator replaces
io.nodusdb.storage                   opening a directory, checkpoints, the format marker and the upgrade
io.nodusdb.storage.snapshot          snapshot version 3
io.nodusdb.storage.legacy            read-only readers for version 1 directories
io.nodusdb.authz                     the tuple store, transactions, tokens and the check evaluator
io.nodusdb.authz.schema              the schema language, relation ids and the compiled model
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

Writes follow one order: validate, reserve memory, journal, make durable, mutate. `accumulateHeadroom`
mirrors the places `add` allocates (the first slab block, promotion at degree 15, and growth of a full
set) and records, per size class, how many pool blocks a run of additions to one node may need. The
reservation subtracts the blocks already on the free lists, so a graph at its limit still accepts a write
that reuses freed memory. A refusal by the budget surfaces before anything is journaled. Deletes never
fail: a set demotes only when the slab can supply a block, and otherwise stays a set at the lower degree.

## Edge keys and the two table pairs

An edge key packs a relation, a subject relation and a node id into one 64-bit value.

```
 63  62           47 46            31 30                         0
+---+---------------+----------------+----------------------------+
| 0 | relation u16  | subj_rel u16   | id (31 bits)               |
+---+---------------+----------------+----------------------------+
```

A key with relation 0 and subject relation 0 equals its id, so an untyped graph needs no re-keying.
`EdgeTables` pairs an outgoing and an incoming `AdjacencyTable`. The kernel holds two pairs. DIRECT holds
tuples with a plain subject whose relation is not a tupleset relation, which a check answers with one probe.
INDIRECT holds userset tuples and the tuples of tupleset relations, and is created on the first such tuple.
The relation catalog decides which pair a tuple belongs to. Traversal masks keys to their ids.

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

One writer, many readers. Every mutation to memory runs between `beginWrite` and `endWrite`, which move a
sequence counter to odd and back to even. A reader records the counter, runs its query, and retries when
the counter changed or was odd. Readers take no lock.

A transaction passes through one writer monitor in this order:

```
validate and collect the headroom                       outside the seqlock
reserve memory                                          short seqlock section
append to the log                                       outside the seqlock
wait for the log to be durable (SYNC mode)              outside the seqlock
apply the records and publish the applied LSN           short seqlock section
```

The durability wait sits outside the seqlock, so a reader is never held up by an fsync, and a change becomes
visible only after it is durable. A failure while applying marks the kernel faulted, because the log and
the memory would disagree. Reservation makes that impossible for budget reasons.

A session handle in the C interface follows the same rule. Looking up a handle is lock-free, reads take no
monitor and keep their results in a buffer per thread. Writes, checkpoint and close serialize on the session.

Bulk load runs in two steps. `prepareBulkLoad` runs once on one thread and sizes every node. `loadBulkNode`
then fills nodes, and calls for distinct nodes may run in parallel. Neither step takes the write sequence,
so the kernel must not be shared with readers until every fill has returned.

## Symbol table

An append-only map between UTF-8 names and dense ids `0 .. size-1`, kept in native memory and charged to the
memory budget. Ids follow the order in which `SYMBOL` records were applied and never change.

```
entries   id -> slab location (chunk, offset) and (hash, length)
slab      chunks of name bytes, 4 KiB first, doubling to 1 MiB
buckets   open addressing over ids, storing id + 1, load at most 0.5
```

Lookups run under their own sequence so that a rebuild of the buckets is never seen half done. A lookup
reads the published count first and ignores any id at or beyond it, so a reader sees a name only after the
writer has finished storing it. A name is added only through a transaction, so the log and the table agree.

## Log (version 2)

One log of typed, variable-length records. Integers are big-endian and records are 8-byte aligned.

```
segment header   magic "NODU" | version 2 | flags | created | base_lsn | crc32c            32 bytes
record           len u32 | type u8 | flags u8 | reserved u16 | lsn u64 | payload | crc32c
```

| Type | Record | Meaning |
|---|---|---|
| 0x01 | GRAPH_CONFIG | claims the key kind, integer or string, once |
| 0x10, 0x11 | TUPLE_ADD, TUPLE_REMOVE | `(object, relation, subject_relation, subject)`; with the AUTOCOMMIT flag the record is its own transaction |
| 0x20 | SYMBOL | defines the next symbol id |
| 0x30 | SCHEMA | the canonical document, its digest and the relation id table |
| 0x40 | TXN_COMMIT | closes a transaction: its first LSN, record count and commit time |
| 0x50 | EPOCH | starts a writer tenure and names the last LSN of the one before |
| 0x60 | ERASE | reserved; replay refuses it until erasure exists |

A transaction is its records followed by a `TXN_COMMIT`; a reader changes state only at commit points.
LSNs start at 1 and rise by one per record. Every open of a durable graph starts a tenure with an
`[EPOCH, TXN_COMMIT]` transaction, and the pair `(epoch, lsn)` is the token a write returns. A write the
previous tenure acknowledged but never made durable lies beyond that tenure's handoff LSN, and a token that
names it raises `TokenLostException`.

Segments are 64 MiB files named by their base LSN. The writer forces a segment before it creates the next
and records the forced position in `log/FORCED`, which alternates between two checksummed slots. Recovery
scans from the oldest segment. A bad record below the forced mark is corruption and refuses to open. A bad
record at or above it is a torn tail: the log is cut back to the last commit point and later segments are
deleted. A failed fsync poisons the log, because retrying an fsync after a failure is not safe, and the
waiting write raises `IndeterminateOutcomeException`.

`SegmentedLog` runs a flusher thread. Appends copy into a buffer and group-commit: in SYNC mode a caller
waits until the flusher has forced its records, and one force covers every caller that arrived meanwhile.
`CrashExplorationTest` replays every prefix of the writes and forces through `SimulatedDisk`, including torn
writes at every byte, and requires recovery to return a prefix of what was acknowledged.

## Snapshot (version 3)

A snapshot is a complete image of the state at an LSN, so every log segment at or below it can be deleted.

```
header    magic "NODS" | version 3 | flags | lsn | epoch | last_commit_ts | node_capacity | sections | crc32c   48 bytes
section   kind u32 | length u64 | crc32c u32 | reserved u32 | body                                              repeated
```

Sections appear in a fixed order: config (key kind), schema (version, digest, relation ids and flags,
canonical document), symbols, the erasure salt, epoch history, then four adjacency sections (DIRECT
outgoing, DIRECT incoming, INDIRECT outgoing, INDIRECT incoming). An adjacency section lists every node's
degree and keys. The loader verifies every checksum, scans each adjacency section once to learn every node's
degree and offset, sizes the graph in one pass and fills the nodes in parallel. It checks every key: the id
must be inside the node capacity and any relation must be one the schema knows.

A checkpoint takes the writer monitor, forces the log, writes `snapshot.bin.tmp`, forces it, renames it over
`snapshot.bin`, rolls the log to a new segment and deletes the segments the snapshot covers. A crash after
the rename and before the deletion is safe, because recovery skips records at or below the snapshot LSN.

## Directory format and upgrade

```
FORMAT          "nodus-format 2", written last by creation and by upgrade
nodus.wal       a 16-byte tripwire carrying version 2, so a version 1 build refuses the directory
snapshot.bin    snapshot version 3
log/            segments and the forced mark
nodus.lock      the directory lock
pre-v2/         the original files, kept by upgrade until upgrade_cleanup()
```

`DirectoryFormat.detect` classifies a directory as new, legacy, current or an interrupted upgrade, and open
refuses the middle two. `GraphUpgrade` recovers the old graph through read-only readers, copies the old files
into `pre-v2/`, replaces `nodus.wal` with the tripwire, builds the new log and snapshot in `.upgrade/`,
installs them, writes `FORMAT`, removes the leftovers and then reopens the result and compares an edge
fingerprint and the symbol count with the original. Running it again after an interruption finishes the job.
The tests crash it after every step.

## Authorization layer

`SchemaParser` and `SchemaValidator` read and check the schema language. `SchemaCompiler` plans one `SCHEMA`
transaction: the symbols it needs and an append-only relation id table, where a removed relation keeps its
id with the retired flag and the id is never reused. `CompiledSchema` is the immutable model built from the
stored catalog, both when a schema is applied and when a graph opens. It numbers every relation and
permission as a definition.

`TupleStore` validates each tuple against the model, defines new names inside the same transaction and
returns the `(epoch, lsn)` token. `CheckEvaluator` answers a check as a search over `(node, definition)`
frames. A stored relation probes the DIRECT pair for the tuple and scans the node's INDIRECT keys for usersets
of that relation. A permission follows its terms: a computed term pushes the same node under another
definition, and an arrow term pushes each subject reached through a tupleset relation. The search keeps its
frames in a per-thread array and its visited frames in a generation-stamped set, so it allocates nothing after
warm-up and ends on cycles. It runs between `readStart` and `readStillValid` and retries when a write overlapped
it. When the depth limit stops it before it can decide it raises `CheckDepthException`, and a grant found on
another branch is still returned.

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
