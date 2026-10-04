# Lake write path

`io.nodusdb.lake` absorbs keyed upserts and deletes in memory, then commits them to an
Iceberg-shaped directory of Parquet files. Reads see every accepted write immediately, whether
the row is still in memory or already committed.

## Components

```
            upsert / delete                      get
                  |                               |
                  v                               v
   +-----------------------------------------------------------+
   | LakeTable (one lock guards the three buffer references)   |
   |                                                           |
   |  active  --freeze-->  frozen  --flusher-->  committed     |
   |    ^                    |                     |           |
   |    |                    |                     v           |
   |  spare <----clear-------+              manifest.txt       |
   |                                        data-N.parquet     |
   |                                        delete-N.parquet   |
   +-----------------------------------------------------------+
```

- `DeltaMemTable` is the columnar buffer. Each buffer holds a key-hash column, a row-kind
  column (`INSERT` or `TOMBSTONE`), one array per numeric column, and a shared byte slab for
  UTF-8 values. Keys resolve through `LongIntIndex`.
- `LakeTable` owns three buffer references: `active` takes writes, `frozen` is being flushed,
  and `spare` is the buffer that will become the next `active`.
- `ParquetWriter` and `ParquetReader` read and write Parquet directly. They depend only on the
  JDK, so the native image needs no Hadoop.
- `Manifest` records the committed files in order and is replaced atomically.

## Lifecycle

1. A write goes into `active`. When `active` reaches `maxRows` or `maxSlabBytes`, and no buffer
   is pending, `active` becomes `frozen`, `spare` becomes `active`, and a flush job is queued.
   Writers continue into the new `active` without waiting.
2. The flusher writes the `INSERT` rows to `data-N.parquet` and the `TOMBSTONE` keys to
   `delete-N.parquet`. It then appends an entry to the manifest and replaces `manifest.txt`
   atomically.
3. The flusher clears the frozen buffer, keeping its grown arrays, and returns it as `spare`.
   Steady state therefore reuses the same arrays and allocates nothing on the ingestion thread.

`flush()` drains both buffers synchronously. `LakeTable.Config.flushIntervalMillis` runs the
same drain on a timer, so a time-based commit needs no caller thread.

### Backpressure

Writers never wait while a flush is pending, unless `active` has reached twice its threshold.
At that point the flush is at least two buffers behind, and writers wait for it. Memory
therefore stays bounded at roughly three buffers plus the slab spares. Under sustained overload,
throughput is set by the flush rate, not the ingestion rate. On `windows-dev`, a 65,536-row
flush takes about 70 ms.

If a flush fails, the frozen buffer is kept and `lastFlushFailure()` reports the cause. Writers
blocked behind it fail with the same cause. The next `flush()` or timer tick retries the same
buffer, so no rows are lost.

## Deletes and tombstones

`delete(key)` never removes a row from `active`. It marks the row `TOMBSTONE` in place, or
appends a tombstone if the key is not present. The tombstone is written to a delete file on the
next flush.

The reason is correctness. A key can exist in an earlier committed file, and a removed
in-memory row would bring that older version back. The cost is that a delete of a key that was
never committed still writes a tombstone. Compaction can drop those later.

A later upsert of the same key turns the tombstone back into an `INSERT`.

## Reads

`get(key)` checks the buffers under the lock, then the committed files without it:

1. `active`: an `INSERT` returns the row; a `TOMBSTONE` returns absent.
2. `frozen`, if a flush is pending: same rule.
3. Committed files, newest first. Within each entry the data file is checked first, then the
   delete file. A key found in a delete file is absent, and older files are not consulted.

Only the in-memory checks are O(1). Each committed lookup reads and decodes the whole file that
it checks, so cost grows with the number of files and their size. Caching decoded key columns
is the next step. The sub-microsecond target applies to in-memory hits only, and it has not been
measured.

## Parquet output

Files use the standard layout: `PAR1`, one row group, one data page per column, and a Thrift
compact footer. Each column is PLAIN-encoded and GZIP-compressed at level 1 (`BEST_SPEED`).

| Schema type | Parquet type | Notes |
|---|---|---|
| `INT64` | INT64 | |
| `DOUBLE` | DOUBLE | stored as raw IEEE 754 bits, so NaN payloads and `-0.0` survive |
| `INT32` | INT32 | |
| `UTF8` | BYTE_ARRAY, converted type UTF8 | |

Every column is `REQUIRED`, so no definition levels are written. The key column is always
`key_hash` (INT64).

The `data-N` and `delete-N` files are verified against PyArrow: a 100,000-row fixture, mixed
integers, doubles including NaN payloads, and UTF-8 strings, reads back bit for bit. DuckDB has
not been checked.

## Identity and limits

- The 64-bit key hash is the row identity. Two keys with the same hash merge into one row. The
  collision probability is about `n^2 / 2^65`, roughly `3e-8` at one million keys.
- Row count is capped at `2^29` per buffer and the slab at `2^30` bytes.
- Writes and reads are serialized by one lock. Writers do not run concurrently with each other.
- Committed lookups decode whole files; there is no compaction yet.
- Iceberg metadata (table JSON and Avro manifests) is not written. The manifest is local and
  Iceberg-shaped only.

## C ABI

`LakeCApi` exports these functions. Each catches runtime exceptions and returns a sentinel.

| Export | Behaviour |
|---|---|
| `nodus_lake_open(schema, path, maxRows, maxSlabBytes, flushIntervalMillis)` | Returns a handle, or null |
| `nodus_lake_close(handle)` | Flushes, then releases the table. Returns false and keeps the handle on failure |
| `nodus_lake_upsert(handle, key, longs, ints, varBytes, varByteCount, varLengths)` | |
| `nodus_lake_upsert_batch(handle, keys, rows, longs, ints, varBytes, varByteCount, varLengths)` | Returns the rows applied, or -1 with nothing applied. Rejects a batch whose lengths exceed the supplied bytes. Capacity errors mid-batch can leave a prefix applied |
| `nodus_lake_delete(handle, key)` | |
| `nodus_lake_flush(handle)` | |
| `nodus_lake_get(handle, key, outLongs, outInts, outVarBytes, outVarCapacity, outVarLengths)` | 0 absent, 1 found, 2 buffer too small (retry larger), -1 error |

Schema specs are `name:TYPE` pairs separated by commas, with `TYPE` one of `INT64`, `DOUBLE`,
`INT32`, `UTF8`. Writers pass longs in the order of `INT64` and `DOUBLE` fields, ints in the
order of `INT32` fields, and UTF-8 bytes and lengths in the order of `UTF8` fields.

The `table_flush(handle, target_parquet_path)` signature in the design sketch became
`nodus_lake_flush(handle)`. Files are written into the table directory, and the directory is
passed at open time.

Not built: the Arrow C Data Interface exporter. The columns live on the Java heap, so the GC can
move them under native code. Zero-copy export needs off-heap storage first, so the flush path
copies at the boundary instead.

## Python

```python
from nodusdb import LakeTable

schema = {"amount": "int64", "score": "float64", "status": "int32", "label": "utf8"}

with LakeTable("/data/orders", schema, flush_rows=1 << 18, flush_interval=5.0) as table:
    table.upsert("order-1042", {"amount": 9900, "score": 0.5, "status": 1, "label": "paid"})
    table.upsert_from((i, {"amount": i, "score": 1.0, "status": 0, "label": ""}) for i in range(100_000))
    table.delete("order-1041")
    print(table.get("order-1042"))
    table.flush()
```

- `key_hash(key)` maps `int` keys to themselves and hashes `str` and `bytes` keys with BLAKE2b
  to 64 bits.
- `flush_rows` sets the freeze threshold. `flush_interval` sets the timer, in seconds.
- Both triggers run inside Java. A Python thread would call into the native isolate from a
  second OS thread, which GraalVM does not allow for the isolate thread the caller created.

The native tests in `python/tests/test_lake.py` skip when `libnodusdb` lacks the `nodus_lake_*`
exports. They run after a native rebuild.

## Known gaps

- The native library has not been rebuilt, so the C ABI and Python layer have not been run.
  That needs GraalVM CE 22 with `native-image` on the path.
- `get` on committed data is O(file). A key-column cache would fix it.
- The ingestion thread is not allocation-free on every cycle. It allocates about 100 bytes at
  each freeze, which measures out to about 0.0014 B/op.
- Writes block once two buffers are full and the flush is still pending, so sustained
  overload shows up as latency.
- No compaction. Tombstones and superseded rows accumulate in the committed files.

## Benchmarks

`LakeTableBench.upsertWhileFlushing` runs with `maxRows = 65536`, so flushes are continuous
during the timed window. Results are recorded in `bench/baseline/lake-flush-windows-dev.json`.
They are specific to that machine.

```
java -jar bench/target/benchmarks.jar LakeTableBench -wi 3 -i 5 -f 1 -prof gc
```

The ingestion thread allocates 0.0014 B/op after warm-up, measured with its own allocation
counter. The `-prof gc` figures count the flusher too, so they are not the ingestion number.
