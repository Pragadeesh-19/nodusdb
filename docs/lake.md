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

Files have the standard layout: the `PAR1` magic, row groups of 122,880 rows (DuckDB's default
size), one column chunk per column per row group, and a Thrift compact footer. A column chunk is
one data page, preceded by a dictionary page when the column is dictionary-encoded. Each page is
compressed with the table's codec: Snappy by default, or none.

A column is dictionary-encoded when it has at most 65,536 distinct values and the dictionary plus
its indices are smaller than the plain values. Dictionary indices use the RLE/bit-packed hybrid,
written as bit-packed runs. Key columns are never dictionary-encoded, because keys are unique
within a buffer.

Every column chunk records its min, max, and null count, both in the column metadata and in its
data page header. Doubles record no statistics when the chunk holds a NaN or a zero, because
their ordering is not safe to publish. The footer declares type-defined column orders, which
PyArrow needs before it trusts min and max. Numeric columns also write the deprecated min and max
fields, which DuckDB reads. Byte-array columns do not, because the deprecated fields are only
valid under signed comparison.

| Schema type | Parquet type | Notes |
|---|---|---|
| `INT64` | INT64 | |
| `DOUBLE` | DOUBLE | raw IEEE 754 bits, so NaN payloads and `-0.0` survive |
| `INT32` | INT32 | |
| `UTF8` | BYTE_ARRAY with converted type UTF8 | |

All columns are `REQUIRED`, so no definition levels are written. The key column is always
`key_hash`, an INT64.

Checked against PyArrow on a 100,000-row file with mixed integers, doubles (including NaN
payloads), and UTF-8 strings: every value matched bit for bit. The Python suite also reads
300,000-row files with PyArrow and checks the codec, encodings, row groups, and statistics. Both
checks run on a developer machine. The CI image does not install `pyarrow`, so those tests skip
there. DuckDB reads the output and returns the same totals as the source on the 1,000,000-row taxi
run. DuckDB shows min and max for numeric columns of these files. It shows none for text columns,
which it also does not show for PyArrow's own files.

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
| `nodus_lake_open(schema, path, maxRows, maxSlabBytes, flushIntervalMillis, codec)` | Returns a handle, or null. `codec` is 0 for uncompressed and 1 for Snappy. Any other value returns null |
| `nodus_lake_close(handle)` | Flushes and releases the table. On failure it returns false and keeps the handle, so the call can be retried |
| `nodus_lake_upsert(handle, key, longs, ints, varBytes, varByteCount, varLengths)` | Returns false on error |
| `nodus_lake_upsert_batch(handle, keys, rows, longs, ints, varBytes, varByteCount, varLengths)` | Returns the rows applied, or -1 with nothing applied. A batch whose lengths exceed the supplied bytes is rejected. A capacity error partway through can leave a prefix applied |
| `nodus_lake_delete(handle, key)` | Returns false on error |
| `nodus_lake_flush(handle)` | Returns false on error |
| `nodus_lake_get(handle, key, outLongs, outInts, outVarBytes, outVarCapacity, outVarLengths)` | 0 absent, 1 found, 2 buffer too small (retry with more room), -1 error |
| `nodus_lake_upsert_columns(handle, rows, keys, longCount, longAddresses, intCount, intAddresses, varCharCount, varOffsetAddresses, varDataAddresses)` | Returns the rows applied, or -1 with nothing applied. Each column is read in place from the address given for it. Variable-width columns use Arrow's layout: 32-bit offsets into a byte buffer. Nulls are not accepted. The layout is validated before any row is applied |
| `nodus_lake_sum(handle, field, out)` | 1 with the sum of the rows in the active buffer, -1 error. Integer sums are exact below 2^53 |
| `nodus_lake_avg(handle, field, out)` | 1 with the average, 0 when the active buffer holds no live rows, -1 error |

Schema specs are comma-separated `name:TYPE` pairs. `TYPE` is `INT64`, `DOUBLE`, `INT32`, or
`UTF8`. Longs are passed in the order of the `INT64` and `DOUBLE` fields, ints in the order of
the `INT32` fields, and UTF-8 bytes and lengths in the order of the `UTF8` fields.

The design sketch had `table_flush(handle, target_parquet_path)`. The export is
`nodus_lake_flush(handle)` instead. Files go into the table directory, which is fixed at open.

The aggregate exports take a field index, counting from zero in schema order. They only accept
numeric fields.

The columnar entry point reads the address tables as arrays of 64-bit values. The caller keeps the
memory alive until the call returns. The columns of a `pyarrow` batch satisfy that, because the
Python wrapper holds the batch for the duration of the call.

### What CI covers

The `ci` workflow builds the native library on Ubuntu, macOS (arm64), and Windows, then runs the
Python lake tests against it. Those tests call `open`, `upsert`, `get`, `delete`, `flush`,
`close`, and the timed flush, and they check the rejection of an invalid schema. The Python suite
also covers the columnar entry point, the aggregates, sliced arrays, null rejection, and the
uncompressed codec. The retry path of `get` is still not covered.

The tests that read files back with PyArrow skip in CI, because the CI image does not install
`pyarrow`. The Java suite covers the Snappy codec, the dictionary index codec, the row-group and
statistics layout, and the columnar upsert against a row-by-row oracle, so those paths run in CI
without PyArrow.

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
    print(table.sum("amount"), table.average("score"))
    table.flush()
```

For batches, pass Arrow data. `upsert_table` takes a `pyarrow` table or record batch and the name
of its `int64` key column. `upsert_columns` takes the key array and a mapping from column name to
array. Text must be Arrow `string`, which uses 32-bit offsets, so `large_string` is rejected. Nulls
are rejected, and so is any column whose length differs from the key's.

- `key_hash(key)` returns an `int` key unchanged. It hashes `str` and `bytes` keys with BLAKE2b
  into 64 bits.
- `flush_rows` sets the freeze threshold. `flush_interval` sets the timer, in seconds.
- `compression` is `"snappy"` by default, or `"none"`.
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
- Single-call upserts from Python are slow. Each call crosses ctypes and builds a row dictionary,
  which the columnar path avoids. The W2 results show the difference.
- Snappy is pure Java and is most of the remaining flush time. The uncompressed setting removes it,
  at the cost of larger files.
- The clean W4 layout's p99 is higher than DuckDB's own files. The cause is not isolated.
- The native aggregate covers only the active buffer. Committed files are not part of it.

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
output is in `bench/baseline/lake-heavyweights-windows-dev-v2.json`, and the script is
`python/benchmarks/benchmark_lake_heavyweights.py`. Each engine and workload runs in its own
process.

- **NodusDB**: this module. Snappy is the default codec. `nodus-lake-uncompressed` is the same
  table with `compression="none"`, and `nodus-lake-batch` is W2 with the columnar path.
- **DuckDB**: an in-memory table, written with `COPY` to Parquet.
- **PyArrow**: a Python dict of rows, written with `pq.write_table`.
- **SQLite**: in memory, dumped with the backup API.

### W1, ingest 1,000,000 rows

| Engine | Rows/s | Seconds | Peak RSS (MB) |
|---|---:|---:|---:|
| NodusDB | 846,137 | 1.18 | 442 |
| DuckDB | 164,793 | 6.07 | 448 |
| PyArrow | 114,582 | 8.73 | 364 |
| SQLite | 95,113 | 10.51 | 591 |

### W2, 500,000 upserts and 100,000 deletes

| Engine | Upserts/s | Deletes/s | Peak RSS (MB) |
|---|---:|---:|---:|
| NodusDB, one call per row | 39,335 | 467,542 | 684 |
| NodusDB, columnar batch | 1,763,882 | 455,458 | 497 |
| DuckDB | 960 | 930 | 512 |
| PyArrow (Python dict) | 393,661 | 755,709 | 730 |
| SQLite | 253,594 | 269,022 | 661 |

The columnar batch builds its update rows with Arrow compute before the timed window. The other
engines build theirs before their timed windows too. Its deletes still use one call per key, so
the delete figures for both NodusDB rows measure the same path.

### W3, flush 250,000 rows

| Engine | Seconds | Rows/s | Files | Size (MB) |
|---|---:|---:|---:|---:|
| NodusDB, Snappy | 0.265 | 941,658 | 1 | 3.95 |
| NodusDB, uncompressed | 0.187 | 1,339,509 | 1 | 5.40 |
| DuckDB | 1.143 | 218,807 | 1 | 3.66 |
| PyArrow | 0.187 | 1,335,888 | 1 | 3.96 |
| SQLite | 0.123 | 2,026,186 | 1 | 11.85 (database file) |

### W4, DuckDB aggregate over each source

Every source gives the same totals: revenue 27,486,629.10 across 1,000,000 trips. The first
five rows are DuckDB's grouped query over the files. The last two are an ungrouped sum and
average over the same column, so they are not comparable to the grouped rows.

| Source | Files | Size (MB) | Median (ms) | p99 (ms) |
|---|---:|---:|---:|---:|
| NodusDB, 250,000-row flushes | 3 | 15.71 | 12.8 | 35.9 |
| NodusDB, 5,000-row flushes | 101 | 16.97 | 25.5 | 27.0 |
| DuckDB `COPY` | 4 | 14.47 | 14.0 | 15.3 |
| PyArrow | 4 | 15.51 | 17.7 | 21.0 |
| NodusDB in memory, ungrouped sum and average (no files) | 0 | 0 | 4.0 | 6.6 |
| DuckDB `COPY`, ungrouped sum and average | 4 | 14.47 | 7.7 | 8.4 |

The clean NodusDB layout wrote three files, not four. Ingest is now faster than the flush, so the
buffer grows past its freeze threshold before it freezes. The 5,000-row layout produced 101 files,
not the 200 that setting implies.

### Change from the previous run

| Measure | Before | After |
|---|---:|---:|
| W1 ingest, rows/s | 47,210 | 846,137 |
| W2 upserts/s, one call per row | 37,253 | 39,335 |
| W2 upserts/s, columnar batch | not measured | 1,763,882 |
| W2 deletes/s | 334,790 | 467,542 |
| W3 flush, Snappy | 0.367 s | 0.265 s |
| W3 flush, no compression | not measured | 0.187 s |
| W4 grouped scan of NodusDB files, median | 42.3 ms | 12.8 ms |

### What the numbers show

- **Ingest is no longer Python-bound.** Arrow columns go to the native library by buffer address.
  The C side reads them in place, and it allocates nothing per row.
- **Flush is now encoding- and codec-bound.** The writer copies column arrays directly, without a
  per-value method call. Snappy is pure Java, and it is most of the remaining flush time. With
  compression off, the flush ties PyArrow at 0.187 s, and the files are 36% larger.
- **Scans are now competitive.** NodusDB's files have dictionary-encoded low-cardinality columns,
  Snappy pages, and 122,880-row row groups. DuckDB scans one row group per task. The earlier layout
  wrote one row group per file, which gave DuckDB only four tasks for four files. The probes that
  measured each property are not in the repository.
- **Single-call upserts remain slow.** Each call crosses ctypes and rebuilds a row dict. I have not
  profiled that path.
- **Tails are worse than DuckDB's.** The clean layout's p99 is 35.9 ms, against 15.3 ms. The
  scan-side cause is not isolated.

### Caveats

- W1 and the single-call W2 path pass each row from Python. The columnar batch does not.
- PyArrow's W2 figures come from a Python dict with no storage behind it. They show a ceiling for
  an in-memory map, not a storage engine.
- The native aggregate covers only the rows in the active buffer. It does not read committed files.
- Peak RSS includes the Python process and the loaded source table, so compare it as a relative
  figure.
- Each engine ran once, on one laptop.

### Reproducing

```
python python/benchmarks/benchmark_lake_heavyweights.py run --out bench/baseline/lake-heavyweights-windows-dev-v2.json
```

The run takes about 20 minutes on `windows-dev`.
