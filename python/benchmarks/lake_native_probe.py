import tempfile
import time
from pathlib import Path

import pyarrow as pa

import nodusdb

ROWS = 200_000
schema = {"amount": "int64", "score": "float64", "status": "int32", "label": "utf8"}
ids = list(range(ROWS))
batch = pa.table({
    "id": pa.array(ids, pa.int64()),
    "amount": pa.array([i * 3 for i in ids], pa.int64()),
    "score": pa.array([i / 7 for i in ids], pa.float64()),
    "status": pa.array([i % 3 for i in ids], pa.int32()),
    "label": pa.array(["paid" if i % 2 else "open" for i in ids], pa.string()),
})

directory = Path(tempfile.mkdtemp(prefix="nodus-speed-")) / "t"
with nodusdb.LakeTable(directory, schema, flush_rows=1 << 22) as table:
    start = time.perf_counter()
    table.upsert_table(batch, key="id")
    columnar = time.perf_counter() - start
    print(f"upsert_table {ROWS} rows: {columnar:.3f} s, {ROWS / columnar:,.0f} rows/s")

    start = time.perf_counter()
    table.sum("amount")
    print(f"sum over {ROWS} rows: {(time.perf_counter() - start) * 1000:.1f} ms")

    start = time.perf_counter()
    for i in range(2000):
        table.upsert(i, {"amount": i, "score": 1.0, "status": 0, "label": "x"})
    single = time.perf_counter() - start
    print(f"2000 single upserts: {single / 2000 * 1e6:.1f} us each")

    start = time.perf_counter()
    table.flush()
    flush = time.perf_counter() - start
    print(f"flush {ROWS} rows: {flush:.3f} s")
