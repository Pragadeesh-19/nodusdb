# The lake engine

A guide to using `nodusdb.LakeTable`, called Spillway in the design documents. It covers what the table does, how to write to it and read from it, what it leaves on disk and how to read that with other tools, and how to get the speed the benchmarks show. For the internals, see [lake.md](lake.md). The runnable versions of this page are [`examples/04_lake_table.py`](../examples/04_lake_table.py) and [`examples/06_read_lake_with_duckdb.py`](../examples/06_read_lake_with_duckdb.py).

## The problem it solves

Open table formats such as Apache Iceberg and DuckLake store data as immutable Parquet files. Immutable files are what make them easy to read from many engines, and they are also the trouble. A writer that commits one upsert at a time makes one file per commit. A writer that commits every few seconds makes thousands of small files a day, and every query has to open all of them.

The usual fix is a buffer in front of the table. That is what `LakeTable` is. It takes one upsert or delete at a time, holds the rows in columns in memory, and writes them as a few large Parquet files. Reads see every accepted write straight away, whether the row is still in memory or already on disk.

Use it when:

- Records arrive one at a time or in small batches and you want them in Parquet, readable by DuckDB, Spark, PyArrow or Iceberg tooling.
- You update and delete by key, and the latest version of a key should win.
- The writer is a single Python process.

Do not use it as a general database. It has no query language, no secondary indexes, no compaction yet and no multi-writer mode.

## A first table

```python
import nodusdb

schema = {"amount": "int64", "score": "float64", "status": "int32", "label": "utf8"}

with nodusdb.LakeTable("/data/orders", schema, flush_rows=100_000) as table:
    table.upsert("order-101", {"amount": 5000, "score": 0.95, "status": 1, "label": "cleared"})
    table.delete("order-099")
    print(table.get("order-101"))      # {'amount': 5000, 'score': 0.95, 'status': 1, 'label': 'cleared'}
    print(table.get("order-099"))      # None
    table.flush()                      # data-*.parquet and delete-*.parquet appear in /data/orders
```

Leaving the `with` block flushes whatever is still in memory and releases the table. A `LakeTable` owns native memory, so close it as you would a file. A table that is garbage collected while open is released by a finalizer and a `ResourceWarning` names the leak.

### Schema

A schema is a dict from column name to type. There are four types:

| Type | Python value | Stored as |
|---|---|---|
| `int64` | `int` | Parquet INT64 |
| `float64` | `float` | Parquet DOUBLE, as raw IEEE 754 bits, so NaN payloads and `-0.0` survive |
| `int32` | `int` | Parquet INT32 |
| `utf8` | `str` | Parquet BYTE_ARRAY (UTF8) |

Every column is required. There are no nulls. Every row must supply every column, and an unknown column raises `ValueError`.

### Keys

A row is identified by a key you choose. An `int` key is used as is and must fit in 64 bits. A `str` or `bytes` key is hashed with BLAKE2b to 64 bits (`nodusdb.key_hash(key)` shows you the value). The Parquet files hold only this 64-bit number, in a column called `key_hash`. They do not hold your original string.

The 64-bit hash *is* the identity. Two different keys with the same hash become one row. The chance is about n² / 2⁶⁵, roughly 3 in 100 million at a million keys. If you can use integer ids, do, and keep the original string as a column.

## Writing

```python
table.upsert(key, row)                    # one row
table.upsert_from(pairs)                  # an iterable of (key, row), sent in batches
table.delete(key)
```

An upsert of a key that is already in memory overwrites the row. A delete writes a **tombstone**: it marks the key as deleted in memory and, at the next flush, records it in a delete file. It cannot simply drop the row, because an older committed file may hold a version of that key, and dropping the in-memory row would bring that version back. A later upsert of a deleted key makes it live again.

### Arrow batches

For volume, hand over Arrow data. The native library reads the Arrow buffers in place, with no Python object per row:

```python
import pyarrow as pa

batch = pa.table({
    "id": pa.array(ids, pa.int64()),
    "amount": pa.array(amounts, pa.int64()),
    "score": pa.array(scores, pa.float64()),
    "status": pa.array(statuses, pa.int32()),
    "label": pa.array(labels, pa.string()),
})
table.upsert_table(batch, key="id")        # returns the number of rows applied
# or, with the pieces separately:
table.upsert_columns(batch.column("id"), {"amount": ..., "score": ..., "status": ..., "label": ...})
```

The rules: the key column is `int64`, each column has the Arrow type that matches the schema, text is Arrow `string` (32-bit offsets, so `large_string` is rejected), there are no nulls, and every column has the same length. The layout is checked for the whole batch before any row is applied, so a bad batch leaves the table unchanged.

## Reading

```python
table.get(key)                  # the row as a dict, or None
table.sum("amount")             # a float
table.average("score")          # a float, or None when there are no live rows
```

`get` looks in the active buffer, then the buffer being flushed, then the committed files from newest to oldest. A key found in a delete file is absent, and older files are not checked. The in-memory checks take constant time. A lookup that reaches committed files reads and decodes each file it checks, so its cost grows with the number and size of the files. A cache for that is the next planned step. Do not use `get` as a bulk read path.

`sum` and `average` cover **only the rows in the active buffer**. They do not read committed files and they do not see rows already frozen for a flush. They exist for cheap running totals over recent data. For anything else, query the files.

## Committing

A buffer is written out when it reaches a row count, a byte count or a time.

```python
nodusdb.LakeTable(
    path, schema,
    flush_rows=1 << 20,         # freeze the buffer at this many rows (default 1,048,576)
    max_slab_bytes=1 << 26,     # or this many bytes of text (default 64 MiB)
    flush_interval=5.0,         # also commit every 5 seconds; default is no timer
    compression="snappy",       # "snappy" (default) or "none"
)
```

All three triggers run inside the native library, on a background thread, so your writing thread does not wait.

The lifecycle:

1. Writes go into the *active* buffer. When it reaches a trigger, it is frozen and a clean buffer takes over. Writers do not wait.
2. A background thread writes the frozen buffer's live rows to `data-N.parquet` and its tombstones to `delete-N.parquet`, then adds an entry to the manifest and replaces `manifest.txt` in one atomic rename.
3. The frozen buffer is cleared and reused as the next spare, keeping its grown columns, so the writing thread stops allocating.

`flush()` drains everything and returns when it is on disk. If a flush fails, the buffer is kept, writers blocked behind it fail with the cause, and the next `flush()` or timer tick retries the same buffer, so no rows are lost.

**Backpressure.** If writes outrun the flush until the active buffer holds twice its trigger, writers wait for the flush. Memory stays near three buffers. Under sustained overload the flush rate sets the ingest rate, and you see it as latency in `upsert`.

## What it leaves on disk

```
/data/orders/
  manifest.txt
  data-00000000.parquet        rows written by flush 0
  data-00000001.parquet        rows written by flush 1
  delete-00000001.parquet      keys deleted by flush 1
```

`manifest.txt` lists the entries in order:

```
nodus-lake-manifest 1
schema amount:INT64;label:UTF8;
entry 0 data-00000000.parquet -
entry 1 data-00000001.parquet delete-00000001.parquet
```

The files are standard Parquet v1: row groups of 122,880 rows, Snappy pages, dictionary encoding on low-cardinality columns, and min, max and null-count statistics on every column chunk. PyArrow, DuckDB and Spark read them. The key column is always called `key_hash`. The Parquet output matches PyArrow bit for bit on a 100,000-row file of mixed integers, doubles (including NaN payloads) and UTF-8 text.

### Reading the files from another engine

There is no compaction yet, so the files hold every version of a key that was ever flushed, and the delete files hold every deletion. A tool that reads `data-*.parquet` with a plain `SELECT` sees superseded rows. To get the current state, take the newest version of each key and drop keys deleted at or after it. In DuckDB:

```sql
WITH versions AS (
    SELECT *, regexp_extract(filename, 'data-([0-9]+)', 1)::INT AS entry
    FROM read_parquet('/data/orders/data-*.parquet', filename = true)
),
newest AS (
    SELECT * FROM versions
    QUALIFY row_number() OVER (PARTITION BY key_hash ORDER BY entry DESC) = 1
),
removed AS (
    SELECT key_hash, regexp_extract(filename, 'delete-([0-9]+)', 1)::INT AS entry
    FROM read_parquet('/data/orders/delete-*.parquet', filename = true)
)
SELECT n.key_hash, n.amount, n.label
FROM newest n
WHERE NOT EXISTS (SELECT 1 FROM removed r WHERE r.key_hash = n.key_hash AND r.entry >= n.entry)
```

[`examples/06_read_lake_with_duckdb.py`](../examples/06_read_lake_with_duckdb.py) runs exactly this against a table that has an overwrite, a delete and a string key, and prints the same answer that `table.get` gives. If your workload only ever inserts new keys, a plain scan is already correct.

## Getting the speed

1. **Use Arrow batches for volume.** A single `upsert` crosses the Python-to-native boundary and builds a row dict for every call. `upsert_table` and `upsert_columns` skip both. In the 2026-10-20 run an Arrow batch was 1.5 times faster than single calls (25,092 against 16,632 rows/s), a much smaller gap than the 45 times measured on 2026-10-04 (1,763,882 against 39,335), because the native write path itself is now the bottleneck.
2. **Pick the flush size for your readers.** Larger buffers mean fewer, larger files, which scan faster. A layout of 250,000-row flushes scanned in about half the time of a layout of 5,000-row flushes in the benchmark (13.4 ms against 25.6 ms), because each file is a separate scan task for the reader.
3. **Compression is a trade.** Snappy is the default and matches DuckDB. `compression="none"` takes about 38% less time to flush (6.1 s against 9.9 s in the 2026-10-20 run) and makes files about 36% larger (5.40 MB against 3.95 MB).
4. **Keep an eye on `flush()` latency.** If writers stall, the flush is the bottleneck, and the fix is a faster disk or a larger `flush_rows`, not more writer threads. Writes and reads on one table share one lock.

## Numbers

**Read this before you choose it for a write-heavy job: the write path in 0.1.0 is slow.** On 2026-10-20, against the library the 0.1.0 wheel carries, on the first 1,000,000 rows of the NYC taxi file:

| Measure | NodusDB | DuckDB | PyArrow | SQLite |
|---|---:|---:|---:|---:|
| Ingest, rows/s | 21,017 | 147,314 | 547,340 | 285,608 |
| Upserts/s, one call per row | 16,632 | did not finish | 329,166 | 243,167 |
| Upserts/s, one Arrow batch | 25,092 | | | |
| Deletes/s | 97,090 | did not finish | 710,651 | 259,898 |
| Flush of 250,000 rows | 9.9 s | 1.1 s | 0.19 s | 0.09 s |
| Grouped scan by DuckDB of the files each wrote, median | 13.4 ms | 15.0 ms | 17.8 ms | |

What holds up: the files are small (3.95 MB for the flush, against 3.66 MB for DuckDB and 3.96 MB for PyArrow), and DuckDB scans them as fast as its own. What does not: the write side is the slowest of the four by a wide margin, and the native `sum` over 1,000,000 rows takes 923 ms. An earlier run on 2026-10-04 measured 846,137 rows per second for ingest and a 0.265 s flush, but that was an earlier implementation with columns in Java arrays. The columns moved to native memory the next day, and the loss is somewhere in that move. [Benchmarks](benchmarks.md#lake) has both runs and what is known so far. The fix is the first lake item in [TODOS.md](../TODOS.md).

The table is one run per cell on a laptop CPU with a Windows disk.

## Limits

- **No compaction.** Superseded rows and tombstones stay in the files. A delete of a key that was never committed still writes a tombstone.
- **Reads that reach committed data decode whole files.** A key-column cache is the planned fix.
- **One writer.** Writes and reads share one lock.
- **Native aggregates cover the active buffer only.**
- **No nulls.** All columns are required.
- **Iceberg metadata is not written for a `LakeTable`.** The manifest has the Iceberg shape and is local. The shipped graph log, by contrast, is projected into a real Iceberg v2 table when you set `iceberg=True` on `Shipping`.
- **No zero-copy Arrow export** yet. The flush copies at the boundary.
- **A buffer holds at most 2²⁹ rows and 2³⁰ bytes of text.**
