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
  `TOMBSTONE`), one native column per numeric field, and one native byte slab for UTF-8 text. Keys resolve
  through `NativeKeyIndex`.
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
- Iceberg table metadata (JSON and Avro manifests) is not written for a `LakeTable`. Its manifest has the
  Iceberg shape and nothing more. The shipped graph log is different: with `iceberg=True` it is projected into
  a real Iceberg v2 table, `nodus_log`, written by the same Parquet writer through its `ColumnSource`
  interface. See the Shipping section of `architecture.md`.

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

Not built: the Arrow C Data Interface exporter. The columns now live in native memory, which is the
precondition D26 set for a zero-copy export, but the exporter itself has not been written.

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
- The native write path is far slower than it was before the columns moved to native memory (about 40 times on ingest
  and on the flush). See the first lake item in TODOS.md.
- Single-call upserts from Python are slow. Each call crosses ctypes and builds a row dictionary,
  which the columnar path avoids. The W2 results show the difference.
- Snappy is pure Java. Turning it off cut the 2026-10-20 flush from 9.9 s to 6.1 s,
  at the cost of files 36% larger.
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

The multi-engine suite (W1 to W4 on the NYC taxi file) lives in [benchmarks.md](benchmarks.md#lake). It holds the run on the 0.1.0 library (2026-10-20) and the earlier run (2026-10-04). The two differ a great deal on the write side, because the columns moved from Java arrays to native memory between them. The 2026-10-04 write-side figures do not describe this code. The open investigation is the first lake item in [TODOS.md](../TODOS.md).
