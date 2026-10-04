# Lake write path

`io.nodusdb.lake` takes keyed upserts and deletes, holds them in memory, and commits them as
Parquet files in a directory that follows the Iceberg layout. Every accepted write is visible to
reads straight away, whether the row is still in memory or already committed.

## Shape of the system

```
upsert, delete                                get
      |                                         |
      v                                         v
+-----------------------------------------------------------+
| LakeTable: one lock guards the buffer references          |
|                                                           |
|  active --freeze--> frozen --flush--> committed           |
|    ^                   |                 |                |
|    |                   v                 v                |
|  spare <---clear-------+        manifest.txt              |
|                                 data-N.parquet            |
|                                 delete-N.parquet          |
+-----------------------------------------------------------+
```

- `DeltaMemTable` is one buffer. It holds a key-hash column, a row-kind column (`INSERT` or
  `TOMBSTONE`), one array per numeric column, and one byte slab for UTF-8 text. Keys resolve
  through `LongIntIndex`.
- `LakeTable` holds three buffer references. `active` takes writes. `frozen` is being flushed.
  `spare` becomes the next `active`.
- `ParquetWriter` and `ParquetReader` read and write Parquet with no dependency beyond the JDK.
- `Manifest` lists the committed files in order and is replaced atomically.

## Lifecycle

1. Writes go into `active`. When `active` reaches `maxRows` or `maxSlabBytes`, and nothing is
   pending, it becomes `frozen`. `spare` becomes the new `active`, and a flush job is queued.
   Writers do not wait.
2. The flusher writes the `INSERT` rows to `data-N.parquet` and the `TOMBSTONE` keys to
   `delete-N.parquet`. It appends an entry to the manifest and replaces `manifest.txt` in one
   atomic step.
3. The flusher clears the frozen buffer and returns it as `spare`. The buffer keeps its grown
   arrays, so the next cycle allocates nothing on the ingestion thread.

`flush()` drains both buffers before it returns. `LakeTable.Config.flushIntervalMillis` runs the
same drain on a timer, so time-based commits do not need a caller thread.

### Backpressure

Writers do not wait for a pending flush until `active` holds twice its threshold. At that point
the flush is at least two buffers behind, so writers wait for it. Memory stays near three buffers
plus the slab spares. Under sustained overload the flush rate sets the ingestion rate. On
`windows-dev`, a 65,536-row flush takes about 70 ms.

A failed flush keeps its buffer and records the cause in `lastFlushFailure()`. Writers blocked
behind it fail with that cause. The next `flush()` or timer tick retries the same buffer, so no
rows are lost.

## Deletes and tombstones

`delete(key)` never removes a row from `active`. It marks the row `TOMBSTONE`, or appends a
tombstone when the key is not in the buffer. The next flush writes it to a delete file.

Removing the row would be cheaper, but it would be wrong. An earlier committed file can hold an
older version of the same key, and removing the in-memory row would bring that version back. The
cost is that a delete of a key which was never committed still writes a tombstone. Compaction
would drop those later.

An upsert of a tombstoned key turns the row back into an `INSERT`.

## Reads

`get(key)` checks the buffers under the lock, then the committed files outside it:

1. `active`. An `INSERT` returns the row. A `TOMBSTONE` returns absent.
2. `frozen`, when a flush is pending. Same rule.
3. Committed files, newest first. Within an entry the data file is checked before the delete
   file. A key found in a delete file is absent, and older files are not checked.

The in-memory checks are constant time. A committed lookup reads and decodes each file it
checks, so its cost grows with the number and size of the files. A cache of decoded key columns
is the planned fix. The sub-microsecond target applies to in-memory hits. It has not been measured.

## Parquet output

Files have the standard layout: the `PAR1` magic, one row group, one data page per column, and a
Thrift compact footer. Each column is PLAIN-encoded and GZIP-compressed at level 1.

| Schema type | Parquet type | Notes |
|---|---|---|
| `INT64` | INT64 | |
| `DOUBLE` | DOUBLE | raw IEEE 754 bits, so NaN payloads and `-0.0` survive |
| `INT32` | INT32 | |
| `UTF8` | BYTE_ARRAY with converted type UTF8 | |

All columns are `REQUIRED`, so no definition levels are written. The key column is always
`key_hash`, an INT64.

Checked against PyArrow on a 100,000-row file with mixed integers, doubles (including NaN
payloads), and UTF-8 strings: every value matched bit for bit. That check runs on a developer
machine. The CI image does not install `pyarrow`, so the test skips there. DuckDB reads the output and
returns the same revenue total as the source on the 1,000,000-row taxi run.

## Identity and limits

- The 64-bit key hash is the row identity. Two keys with the same hash become one row. The
  collision probability is about `n^2 / 2^65`, roughly `3e-8` at one million keys.
- Each buffer holds up to `2^29` rows, and a slab holds up to `2^30` bytes.
- Writes and reads share one lock. Writers do not run in parallel.
- There is no compaction. Superseded rows and tombstones stay in the files.
- Iceberg table metadata (JSON and Avro manifests) is not written. The manifest has the Iceberg
  shape and nothing more.

## C ABI

`LakeCApi` exports the functions below. Each one catches runtime exceptions and returns a
sentinel.

| Export | Behaviour |
|---|---|
| `nodus_lake_open(schema, path, maxRows, maxSlabBytes, flushIntervalMillis)` | Returns a handle, or null |
| `nodus_lake_close(handle)` | Flushes and releases the table. On failure it returns false and keeps the handle, so the call can be retried |
| `nodus_lake_upsert(handle, key, longs, ints, varBytes, varByteCount, varLengths)` | Returns false on error |
| `nodus_lake_upsert_batch(handle, keys, rows, longs, ints, varBytes, varByteCount, varLengths)` | Returns the rows applied, or -1 with nothing applied. A batch whose lengths exceed the supplied bytes is rejected. A capacity error partway through can leave a prefix applied |
| `nodus_lake_delete(handle, key)` | Returns false on error |
| `nodus_lake_flush(handle)` | Returns false on error |
| `nodus_lake_get(handle, key, outLongs, outInts, outVarBytes, outVarCapacity, outVarLengths)` | 0 absent, 1 found, 2 buffer too small (retry with more room), -1 error |

Schema specs are comma-separated `name:TYPE` pairs. `TYPE` is `INT64`, `DOUBLE`, `INT32`, or
`UTF8`. Longs are passed in the order of the `INT64` and `DOUBLE` fields, ints in the order of
the `INT32` fields, and UTF-8 bytes and lengths in the order of the `UTF8` fields.

The design sketch had `table_flush(handle, target_parquet_path)`. The export is
`nodus_lake_flush(handle)` instead. Files go into the table directory, which is fixed at open.

### What CI covers

The `ci` workflow builds the native library on Ubuntu, macOS (arm64), and Windows, then runs the
Python lake tests against it. Those tests call `open`, `upsert`, `get`, `delete`, `flush`,
`close`, and the timed flush, and they check the rejection of an invalid schema. The batch export
and the retry path of `get` are not covered. Only `upsert_from` calls the batch export, and only
the PyArrow test uses it, which skips in CI.

Not built: the Arrow C Data Interface exporter. The columns live on the Java heap, and the GC can
move them while native code holds a pointer. Zero-copy export needs off-heap storage first, so
the flush copies at the boundary instead.

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

- `key_hash(key)` returns an `int` key unchanged. It hashes `str` and `bytes` keys with BLAKE2b
  into 64 bits.
- `flush_rows` sets the freeze threshold. `flush_interval` sets the timer, in seconds.
- Both triggers run inside Java. A Python thread would call into the native isolate from a second
  OS thread, which GraalVM does not allow for a thread the caller did not attach.

## Known gaps

- The Arrow export is not built. See the C ABI section.
- `nodus_lake_upsert_batch` and the buffer-retry path of `get` have no CI coverage.
- Committed reads decode whole files. A key-column cache would remove that cost.
- The ingestion thread is not allocation-free on every cycle. Each freeze allocates about 100
  bytes, which works out to about 0.0014 B/op.
- Once two buffers are full and a flush is still pending, writers block. Sustained overload
  therefore shows up as latency.
- No compaction. Tombstones and superseded rows accumulate.
- DuckDB scans this output 2.5 to 3 times slower than its own `COPY` output. See the multi-engine results.

## Benchmarks

`LakeTableBench.upsertWhileFlushing` uses `maxRows = 65536`, so flushes run for the whole timed
window. The result is recorded in `bench/baseline/lake-flush-windows-dev.json` and applies to that
machine only.

```
java -jar bench/target/benchmarks.jar LakeTableBench -wi 3 -i 5 -f 1 -prof gc
```

The ingestion thread allocates 0.0014 B/op after warm-up, measured with that thread's own
allocation counter. The `-prof gc` numbers include the flusher, so they are not the ingestion
figure.

## Multi-engine results

These run on `windows-dev` against the first 1,000,000 rows of the NYC TLC Yellow Taxi file for
January 2024. The file's SHA-256 is checked before use. Rows are keyed by row index. The raw
output is in `bench/baseline/lake-heavyweights-windows-dev.json`, and the script is
`python/benchmarks/benchmark_lake_heavyweights.py`. Each engine and workload runs in its own
process.

- **NodusDB lake**: this module, with 250,000-row buffers. W4 also uses a 5,000-row configuration.
- **DuckDB**: an in-memory table, written with `COPY` to Parquet.
- **PyArrow**: a Python dict of rows, written with `pq.write_table`.
- **SQLite**: in memory, dumped with the backup API.

### W1, ingest 1,000,000 rows

| Engine | Rows/s | Seconds | Peak RSS (MB) |
|---|---:|---:|---:|
| NodusDB lake | 47,210 | 21.2 | 663 |
| DuckDB | 167,364 | 6.0 | 458 |
| PyArrow | 112,510 | 8.9 | 375 |
| SQLite | 94,969 | 10.5 | 590 |

### W2, 500,000 upserts and 100,000 deletes

| Engine | Upserts/s | Deletes/s | Peak RSS (MB) |
|---|---:|---:|---:|
| NodusDB lake | 37,253 | 334,790 | 724 |
| DuckDB | 857 | 1,031 | 500 |
| PyArrow (Python dict) | 398,114 | 790,855 | 713 |
| SQLite | 259,463 | 276,078 | 662 |

### W3, flush a 250,000-row buffer

| Engine | Seconds | Rows/s | Output | Files |
|---|---:|---:|---:|---:|
| NodusDB lake | 0.367 | 681,443 | 3.35 MB Parquet | 1 |
| DuckDB | 1.135 | 220,331 | 3.66 MB Parquet | 1 |
| PyArrow | 0.194 | 1,290,500 | 3.96 MB Parquet | 1 |
| SQLite | 0.124 | 2,010,307 | 11.85 MB database | 1 |

SQLite writes a database file, not Parquet, so W4 leaves it out.

### W4, DuckDB aggregate over each engine's output

Every source gives the same totals: revenue 27,486,629.10 across 1,000,000 trips.

| Source | Files | Bytes | Median (ms) | p99 (ms) |
|---|---:|---:|---:|---:|
| NodusDB, 250,000-row files | 4 | 13.17 MB | 42.3 | 67.5 |
| NodusDB, 5,000-row files | 108 | 13.51 MB | 34.6 | 37.3 |
| DuckDB `COPY` | 4 | 14.47 MB | 13.6 | 27.2 |
| PyArrow | 4 | 15.51 MB | 17.0 | 18.6 |

### What the numbers show

- **Deletes are cheap.** A delete updates an in-memory index entry and records a tombstone, so
  100,000 deletes take 0.30 s.
- **Upserts are the weak path.** NodusDB upserts at 37,253 rows/s, about 7 times slower than
  SQLite's in-memory upsert. I have not profiled it. The native call per row and the shared lock
  are the first suspects.
- **Scans are slower on NodusDB's files.** This writer emits PLAIN pages only. DuckDB's own
  `COPY` output is different, so the encoding is the first suspect. I have not tested that.
- **File count did not hurt at this size.** The 108 streaming files scanned faster than the four
  large files, 34.6 ms against 42.3 ms. This is one 13 MB dataset. A table with many more files may
  behave differently, and this suite did not test that.

### Caveats

- W1 and W2 pass each row from Python into the native library, so their figures include the
  binding cost.
- PyArrow's W2 figures come from a Python dict with no storage behind it. They show a ceiling for
  an in-memory map, not a storage engine.
- The 5,000-row configuration produced 108 files. The setting implies about 200, and I have not
  checked why.
- Peak RSS includes the Python process and the loaded source table, so compare it as a relative
  figure.
- Each engine ran once, on one laptop.
