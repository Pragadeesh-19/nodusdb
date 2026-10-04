# Lake write path: DeltaMemTable

`io.nodusdb.lake` holds the in-memory write absorber for open lakehouse tables. It takes keyed upserts and deletes, keeps every column densely packed, and answers point lookups without allocating on the heap.

## Layout

Each column is a separate primitive array. Rows `0 .. rowCount-1` are always dense: no holes, no tombstones.

```
keyHashes          [k0 k1 k2 ... k(n-1) | free ]
longColumns[c]     [v0 v1 v2 ... v(n-1) | free ]   doubles are stored as raw bits
intColumns[c]      [i0 i1 i2 ... i(n-1) | free ]
varCharOffsets[c]  [o0 o1 o2 ... o(n-1) | free ]   position in varCharSlab
varCharLengths[c]  [l0 l1 l2 ... l(n-1) | free ]
varCharSlab        [ live bytes ... | dead bytes | free ]
index              keyHash -> row
```

- Key resolution is one `LongIntIndex` lookup. The index is the only structure that maps keys to rows.
- Doubles travel as `Double.doubleToRawLongBits`, so NaN payloads and `-0.0` round-trip exactly. Read them with `doubleAt`.
- All row arrays share one capacity and double together.
- Variable-width values are UTF-8 bytes supplied by the caller. The kernel never sees a `String`.

## Operations

`upsert(keyHash, longs, ints, varCharBytes, varCharLengths)` returns `true` for an insert and `false` for an overwrite. Arguments are validated before anything mutates, so a rejected call leaves the table unchanged.

- Overwrite: the new variable-width bytes are appended to the slab, the row's offsets and lengths are repointed, and the old bytes are left as dead space.
- Insert: the row is appended at `rowCount`, and the key is registered in the index.

`delete(keyHash)` uses swap-and-pop across every column:

```
if the key is absent:           return false
R      = index.get(keyHash)
last   = rowCount - 1
if R < last:
    move keyHashes[last] to keyHashes[R]
    copy every long, int, offset, and length column from last to R
    index.put(movedKey, R)            // update the moved key in place
index.remove(keyHash)                 // backward-shift deletion, no tombstone
rowCount = last
return true
```

The index update comes before the removal. That keeps the only structural index change inside `remove`.

Row indices are valid only until the next mutation. A delete can move the last row into the vacated slot, so callers must not keep row numbers across writes.

## Slab reclamation

Overwrites and deletes leave dead bytes in the slab. When an append does not fit, the slab is rebuilt:

- The live bytes of every row are copied into a buffer of at least `2 * required` bytes, rounded up to a power of two. This keeps the rebuild cost amortized over the appends that follow it.
- The buffer is reused from a spare slot when it is large enough. Once the slab reaches its working size, rebuilds allocate nothing.
- Memory is at most about four times the live variable-width bytes, plus the spare buffer.

## Invariants

`assertInvariant()` runs after every mutation in the tests and checks:

- `index.size() == rowCount`
- `index.get(keyHashes[i]) == i` for every row
- every variable-width span lies inside `[0, slabUsed)`
- the tracked live-byte count equals the sum of all variable-width lengths

Duplicate keys are excluded by the second check. If every row maps to its own index, no two rows can share a key.

## Identity and limits

- The 64-bit key hash is the row identity. Two distinct primary keys with the same hash would merge into one row. The collision probability is about `n^2 / 2^65`, roughly `3e-8` at one million keys. The caller is responsible for supplying a well-mixed 64-bit hash.
- Row count is capped at `2^29`, and the slab at `2^30` bytes.
- Single writer. Concurrent reads during a write are not supported.

## Benchmark

`DeltaMemTableBench.sustainedUpsert` pre-sizes the table to one million rows and then streams in-place upserts. Growth never runs inside the timed window, so the steady-state allocation claim can be checked directly with `-prof gc`. Run:

```
java -jar bench/target/benchmarks.jar DeltaMemTableBench -wi 3 -i 5 -f 1 -prof gc
```

Results are machine-specific. Record them in `bench/baseline/` alongside the graph baselines before comparing across runs.
