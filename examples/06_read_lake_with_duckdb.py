import tempfile
from pathlib import Path

import duckdb

import nodusdb

schema = {"amount": "int64", "label": "utf8"}
directory = Path(tempfile.mkdtemp(prefix="nodus-example-")) / "orders"

with nodusdb.LakeTable(directory, schema) as table:
    table.upsert(1, {"amount": 10, "label": "first"})
    table.upsert(2, {"amount": 20, "label": "second"})
    table.flush()
    table.upsert(1, {"amount": 11, "label": "first, revised"})
    table.delete(2)
    table.upsert("order-7", {"amount": 70, "label": "third"})
    table.flush()

    print("through the table:", table.get(1), table.get(2))

data = (directory / "data-*.parquet").as_posix()
deletes = (directory / "delete-*.parquet").as_posix()

current = duckdb.sql(f"""
    WITH versions AS (
        SELECT *, regexp_extract(filename, 'data-([0-9]+)', 1)::INT AS entry
        FROM read_parquet('{data}', filename = true)
    ),
    newest AS (
        SELECT * FROM versions
        QUALIFY row_number() OVER (PARTITION BY key_hash ORDER BY entry DESC) = 1
    ),
    removed AS (
        SELECT key_hash, regexp_extract(filename, 'delete-([0-9]+)', 1)::INT AS entry
        FROM read_parquet('{deletes}', filename = true)
    )
    SELECT n.key_hash, n.amount, n.label
    FROM newest n
    WHERE NOT EXISTS (SELECT 1 FROM removed r WHERE r.key_hash = n.key_hash AND r.entry >= n.entry)
    ORDER BY n.amount
""").fetchall()

print("through SQL over the files:")
for row in current:
    print(" ", row)
