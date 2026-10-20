import tempfile
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

import nodusdb

schema = {"amount": "int64", "score": "float64", "status": "int32", "label": "utf8"}
directory = Path(tempfile.mkdtemp(prefix="nodus-example-")) / "orders"

with nodusdb.LakeTable(directory, schema, flush_rows=100_000) as table:
    table.upsert("order-101", {"amount": 5000, "score": 0.95, "status": 1, "label": "cleared"})
    table.upsert("order-102", {"amount": 1200, "score": 0.40, "status": 0, "label": "pending"})
    table.delete("order-099")
    print("get:", table.get("order-101"))
    print("absent:", table.get("order-099"))

    ids = list(range(1_000, 51_000))
    batch = pa.table({
        "id": pa.array(ids, pa.int64()),
        "amount": pa.array([i * 3 for i in ids], pa.int64()),
        "score": pa.array([i / 7 for i in ids], pa.float64()),
        "status": pa.array([i % 3 for i in ids], pa.int32()),
        "label": pa.array(["paid" if i % 2 else "open" for i in ids], pa.string()),
    })
    print("rows applied:", table.upsert_table(batch, key="id"))
    print("sum of amount in memory:", table.sum("amount"))
    print("average score in memory:", round(table.average("score"), 4))

    table.delete(1_000)
    print("deleted key:", table.get(1_000))
    table.flush()

print("files:", sorted(entry.name for entry in directory.iterdir()))
data = pq.read_table(directory / sorted(p.name for p in directory.glob("data-*.parquet"))[0])
print("parquet rows:", data.num_rows, "columns:", data.column_names)
