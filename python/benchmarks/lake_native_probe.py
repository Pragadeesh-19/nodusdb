"""Time the lake's pieces through the native library: Arrow ingest, the in-memory sum, single upserts and a flush.

Run it with NODUSDB_LIBRARY pointing at the library under test. --compression none separates the Snappy cost from
the encoding cost of a flush."""

import argparse
import tempfile
import time
from pathlib import Path

import pyarrow as pa

import nodusdb

SCHEMA = {"amount": "int64", "score": "float64", "status": "int32", "label": "utf8"}


def batch(rows):
    ids = list(range(rows))
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "amount": pa.array([i * 3 for i in ids], pa.int64()),
        "score": pa.array([i / 7 for i in ids], pa.float64()),
        "status": pa.array([i % 3 for i in ids], pa.int32()),
        "label": pa.array(["paid" if i % 2 else "open" for i in ids], pa.string()),
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=200_000)
    parser.add_argument("--compression", default="snappy", choices=["snappy", "none"])
    arguments = parser.parse_args()
    rows = arguments.rows
    data = batch(rows)
    directory = Path(tempfile.mkdtemp(prefix="nodus-speed-")) / "t"
    with nodusdb.LakeTable(directory, SCHEMA, flush_rows=1 << 22, compression=arguments.compression) as table:
        start = time.perf_counter()
        table.upsert_table(data, key="id")
        elapsed = time.perf_counter() - start
        print(f"upsert_table {rows} rows: {elapsed:.3f} s, {rows / elapsed:,.0f} rows/s")

        start = time.perf_counter()
        table.sum("amount")
        print(f"sum over {rows} rows: {(time.perf_counter() - start) * 1000:.1f} ms")

        start = time.perf_counter()
        for i in range(2000):
            table.upsert(i, {"amount": i, "score": 1.0, "status": 0, "label": "x"})
        print(f"2000 single upserts: {(time.perf_counter() - start) / 2000 * 1e6:.1f} us each")

        start = time.perf_counter()
        table.flush()
        print(f"flush {rows} rows ({arguments.compression}): {time.perf_counter() - start:.3f} s")


if __name__ == "__main__":
    main()
