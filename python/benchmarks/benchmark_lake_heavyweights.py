"""Lakehouse engine comparison on real NYC TLC Yellow Taxi trips.

NodusDB LakeTable (Spillway) against DuckDB, PyArrow, and SQLite. Each (workload, engine) pair runs
in its own process, so RSS belongs to one engine. Workload 4 queries the Parquet output with DuckDB.

    python benchmark_lake_heavyweights.py run --out bench/baseline/lake-heavyweights-windows-dev.json
"""

import argparse
import json
import random
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _heavy_common import (  # noqa: E402
    CACHE_DIRECTORY,
    Stopwatch,
    ensure_download,
    file_sha256,
    latency_summary,
    markdown_table,
    optional_import,
    peak_rss_mb,
    current_rss_mb,
    write_json,
)

TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
TAXI_SHA256 = "c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510"
ROWS = 1_000_000
UPDATES = 500_000
DELETES = 100_000
FLUSH_ROWS = 250_000
STREAMING_FLUSH_ROWS = 5_000
FRESHNESS_REPEATS = 7
WORKLOADS = ["W1", "W2", "W3", "W4"]
ENGINES = ["nodus-lake", "nodus-lake-batch", "nodus-lake-uncompressed", "duckdb", "pyarrow", "sqlite"]
BASE_ENGINES = ["nodus-lake", "duckdb", "pyarrow", "sqlite"]
ENGINES_BY_WORKLOAD = {
    "W1": BASE_ENGINES,
    "W2": ["nodus-lake", "nodus-lake-batch", "duckdb", "pyarrow", "sqlite"],
    "W3": ["nodus-lake", "nodus-lake-uncompressed", "duckdb", "pyarrow", "sqlite"],
    "W4": BASE_ENGINES,
}
UNGROUPED_QUERY = ("SELECT count(*) AS trips, sum(total_amount) AS revenue, avg(trip_distance) AS distance "
                   "FROM read_parquet('{pattern}')")

SOURCE_COLUMNS = ["VendorID", "tpep_pickup_datetime", "trip_distance", "fare_amount", "total_amount",
                  "payment_type", "store_and_fwd_flag"]
SCHEMA = {"vendor_id": "int32", "pickup_us": "int64", "trip_distance": "float64",
          "fare_amount": "float64", "total_amount": "float64", "payment_type": "int32", "flag": "utf8"}


def load_taxi():
    path = ensure_download(TAXI_URL, CACHE_DIRECTORY / "taxi" / "yellow_tripdata_2024-01.parquet", TAXI_SHA256)
    table = pq.read_table(path, columns=SOURCE_COLUMNS).slice(0, ROWS)
    pickup = table.column("tpep_pickup_datetime").cast(pa.timestamp("us")).cast(pa.int64())
    flag = table.column("store_and_fwd_flag").fill_null("N").cast(pa.string())
    columns = {
        "id": pa.array(range(table.num_rows), pa.int64()),
        "vendor_id": table.column("VendorID").cast(pa.int32()),
        "pickup_us": pickup,
        "trip_distance": table.column("trip_distance").cast(pa.float64()),
        "fare_amount": table.column("fare_amount").cast(pa.float64()),
        "total_amount": table.column("total_amount").cast(pa.float64()),
        "payment_type": table.column("payment_type").cast(pa.int32()),
        "flag": flag,
    }
    return pa.table(columns), path


class NodusLake:
    name = "nodus-lake"

    def __init__(self, directory, flush_rows, compression="snappy"):
        from nodusdb import LakeTable

        self.directory = Path(directory)
        self.table = LakeTable(str(self.directory), SCHEMA, flush_rows=flush_rows, compression=compression)

    def ingest(self, data):
        self.table.upsert_table(data, "id")

    def update(self, key, row):
        self.table.upsert(key, row)

    def delete(self, key):
        self.table.delete(key)

    def flush(self):
        self.table.flush()

    def output_files(self):
        return sorted(self.directory.glob("data-*.parquet"))

    def close(self):
        self.table.close()


def _update_table(data, keys):
    import pyarrow.compute as pc

    rows = data.take(pa.array(keys, pa.int64()))
    payment = rows.column("payment_type")
    return pa.table({
        "id": rows.column("id"),
        "vendor_id": rows.column("vendor_id"),
        "pickup_us": rows.column("pickup_us"),
        "trip_distance": rows.column("trip_distance"),
        "fare_amount": pc.round(pc.multiply(rows.column("fare_amount"), 1.05), 2),
        "total_amount": rows.column("total_amount"),
        "payment_type": pc.add(pc.subtract(payment, pc.multiply(pc.divide(payment, 6), 6)), 1).cast(pa.int32()),
        "flag": rows.column("flag"),
    })


class DuckDBLake:
    name = "duckdb"

    def __init__(self, directory):
        duckdb = optional_import("duckdb")
        if duckdb is None:
            raise ImportError("duckdb")
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.connection = duckdb.connect(":memory:")
        self.connection.execute(
            "CREATE TABLE taxi (id BIGINT PRIMARY KEY, vendor_id INTEGER, pickup_us BIGINT, trip_distance DOUBLE, "
            "fare_amount DOUBLE, total_amount DOUBLE, payment_type INTEGER, flag VARCHAR)")
        self.parts = 0

    def ingest(self, data, period=FLUSH_ROWS):
        for start in range(0, data.num_rows, period):
            chunk = data.slice(start, period)
            self.connection.register("incoming", chunk)
            self.connection.execute("INSERT INTO taxi SELECT * FROM incoming")
            self.connection.unregister("incoming")
            self.write_part()

    def write_part(self):
        self.parts += 1
        target = self.directory / f"part-{self.parts:04d}.parquet"
        self.connection.execute(f"COPY taxi TO '{target.as_posix()}' (FORMAT PARQUET)")
        self.connection.execute("DELETE FROM taxi")

    def load_resident(self, data):
        self.connection.register("incoming", data)
        self.connection.execute("INSERT INTO taxi SELECT * FROM incoming")
        self.connection.unregister("incoming")

    def update(self, rows):
        self.connection.executemany(
            "UPDATE taxi SET fare_amount = ?, payment_type = ? WHERE id = ?", rows)

    def delete(self, keys):
        self.connection.executemany("DELETE FROM taxi WHERE id = ?", [(key,) for key in keys])

    def output_files(self):
        return sorted(self.directory.glob("part-*.parquet"))

    def close(self):
        self.connection.close()


class PyArrowLake:
    name = "pyarrow"

    def __init__(self, directory):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.parts = 0
        self.state = {}

    def ingest(self, data, period=FLUSH_ROWS):
        names = list(SCHEMA)
        for start in range(0, data.num_rows, period):
            chunk = data.slice(start, period)
            columns = {name: chunk.column(name).to_pylist() for name in ["id"] + names}
            self.write_part(pa.table({
                "id": pa.array(columns["id"], pa.int64()),
                **{name: pa.array(columns[name], data.schema.field(name).type) for name in names}}))

    def write_part(self, table):
        self.parts += 1
        pq.write_table(table, self.directory / f"part-{self.parts:04d}.parquet", compression="snappy")

    def load_state(self, data):
        names = list(SCHEMA)
        columns = [data.column(name).to_pylist() for name in names]
        for key, values in zip(data.column("id").to_pylist(), zip(*columns)):
            self.state[key] = values

    def update(self, key, values):
        self.state[key] = values

    def delete(self, key):
        self.state.pop(key, None)

    def output_files(self):
        return sorted(self.directory.glob("part-*.parquet"))

    def close(self):
        return None


class SQLiteLake:
    name = "sqlite"

    def __init__(self, directory):
        import sqlite3

        self.directory = Path(directory)
        self.connection = sqlite3.connect(":memory:")
        self.connection.execute(
            "CREATE TABLE taxi (id INTEGER PRIMARY KEY, vendor_id INTEGER, pickup_us INTEGER, "
            "trip_distance REAL, fare_amount REAL, total_amount REAL, payment_type INTEGER, flag TEXT)")

    def ingest(self, data):
        names = ["id"] + list(SCHEMA)
        columns = [data.column(name).to_pylist() for name in names]
        rows = zip(*columns)
        for batch in _batches(rows, 50_000):
            with self.connection:
                self.connection.executemany(
                    "INSERT INTO taxi (id, vendor_id, pickup_us, trip_distance, fare_amount, total_amount, "
                    "payment_type, flag) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", batch)

    def update(self, rows):
        with self.connection:
            self.connection.executemany(
                "UPDATE taxi SET fare_amount = ?, payment_type = ? WHERE id = ?", rows)

    def delete(self, keys):
        with self.connection:
            self.connection.executemany("DELETE FROM taxi WHERE id = ?", [(key,) for key in keys])

    def dump(self, target):
        import sqlite3

        target = Path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.unlink()
        destination = sqlite3.connect(str(target))
        self.connection.backup(destination)
        destination.close()
        return target

    def close(self):
        self.connection.close()


def _batches(iterator, size):
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def workload_w1(engine_name, workdir, data):
    workdir = Path(workdir)
    metrics = {"rows": data.num_rows}
    if engine_name == "nodus-lake":
        engine = NodusLake(workdir / "out", FLUSH_ROWS)
        with Stopwatch() as timer:
            engine.ingest(data)
            engine.flush()
        engine.close()
    elif engine_name == "duckdb":
        engine = DuckDBLake(workdir / "out")
        with Stopwatch() as timer:
            engine.ingest(data)
        engine.close()
    elif engine_name == "pyarrow":
        engine = PyArrowLake(workdir / "out")
        with Stopwatch() as timer:
            engine.ingest(data)
    elif engine_name == "sqlite":
        engine = SQLiteLake(workdir / "out")
        with Stopwatch() as timer:
            engine.ingest(data)
        engine.close()
    metrics["ingest_seconds"] = timer.seconds
    metrics["rows_per_second"] = data.num_rows / timer.seconds
    metrics["peak_rss_mb"] = peak_rss_mb()
    return metrics


def workload_w2(engine_name, workdir, data):
    rng = random.Random(23)
    keys = list(range(data.num_rows))
    update_keys = rng.sample(keys, UPDATES)
    updated = set(update_keys)
    delete_keys = rng.sample([key for key in keys if key not in updated], DELETES)
    metrics = {"updates": UPDATES, "deletes": DELETES}
    rss = [current_rss_mb()]
    if engine_name == "nodus-lake":
        engine = NodusLake(Path(workdir) / "out", FLUSH_ROWS)
        engine.ingest(data)
        columns = {name: data.column(name).to_pylist() for name in SCHEMA}
        with Stopwatch() as updating:
            for position, key in enumerate(update_keys):
                row = {name: columns[name][key] for name in SCHEMA}
                row["fare_amount"] = round(row["fare_amount"] * 1.05, 2)
                row["payment_type"] = (row["payment_type"] % 6) + 1
                engine.update(key, row)
                if position % 50_000 == 0:
                    rss.append(current_rss_mb())
        with Stopwatch() as deleting:
            for key in delete_keys:
                engine.delete(key)
        engine.close()
    elif engine_name == "nodus-lake-batch":
        engine = NodusLake(Path(workdir) / "out", FLUSH_ROWS)
        engine.ingest(data)
        updates = _update_table(data, update_keys)
        with Stopwatch() as updating:
            engine.table.upsert_table(updates, "id")
        with Stopwatch() as deleting:
            for key in delete_keys:
                engine.delete(key)
        engine.close()
    elif engine_name == "duckdb":
        engine = DuckDBLake(Path(workdir) / "out")
        engine.load_resident(data)
        values = [(round(v * 1.05, 2), (p % 6) + 1, k) for k, v, p in zip(
            update_keys, data.column("fare_amount").take(pa.array(update_keys)).to_pylist(),
            data.column("payment_type").take(pa.array(update_keys)).to_pylist())]
        with Stopwatch() as updating:
            engine.update(values)
            rss.append(current_rss_mb())
        with Stopwatch() as deleting:
            engine.delete(delete_keys)
        engine.close()
    elif engine_name == "pyarrow":
        engine = PyArrowLake(Path(workdir) / "out")
        engine.load_state(data)
        with Stopwatch() as updating:
            for position, key in enumerate(update_keys):
                values = list(engine.state[key])
                values[3] = round(values[3] * 1.05, 2)
                values[5] = (values[5] % 6) + 1
                engine.update(key, tuple(values))
                if position % 50_000 == 0:
                    rss.append(current_rss_mb())
        with Stopwatch() as deleting:
            for key in delete_keys:
                engine.delete(key)
    elif engine_name == "sqlite":
        engine = SQLiteLake(Path(workdir) / "out")
        engine.ingest(data)
        values = [(round(f * 1.05, 2), (p % 6) + 1, k) for k, f, p in zip(
            update_keys, data.column("fare_amount").take(pa.array(update_keys)).to_pylist(),
            data.column("payment_type").take(pa.array(update_keys)).to_pylist())]
        with Stopwatch() as updating:
            engine.update(values)
            rss.append(current_rss_mb())
        with Stopwatch() as deleting:
            engine.delete(delete_keys)
        engine.close()
    metrics["update_seconds"] = updating.seconds
    metrics["upserts_per_second"] = UPDATES / updating.seconds
    metrics["delete_seconds"] = deleting.seconds
    metrics["deletes_per_second"] = DELETES / deleting.seconds
    metrics["rss_mb_samples"] = [round(value, 1) for value in rss]
    metrics["rss_mb_min"] = min(rss)
    metrics["rss_mb_max"] = max(rss)
    metrics["peak_rss_mb"] = peak_rss_mb()
    return metrics


def workload_w3(engine_name, workdir, data):
    workdir = Path(workdir)
    buffer = data.slice(0, FLUSH_ROWS)
    metrics = {"buffer_rows": buffer.num_rows}
    if engine_name.startswith("nodus-lake"):
        codec = "none" if engine_name == "nodus-lake-uncompressed" else "snappy"
        engine = NodusLake(workdir / "flush", 1 << 20, compression=codec)
        engine.ingest(buffer)
        with Stopwatch() as flushing:
            engine.flush()
        engine.close()
        output = engine.output_files()
    elif engine_name == "duckdb":
        engine = DuckDBLake(workdir / "flush")
        engine.load_resident(buffer)
        with Stopwatch() as flushing:
            engine.write_part()
        engine.close()
        output = engine.output_files()
    elif engine_name == "pyarrow":
        engine = PyArrowLake(workdir / "flush")
        with Stopwatch() as flushing:
            table = pa.table({"id": buffer.column("id"), **{name: buffer.column(name) for name in SCHEMA}})
            engine.write_part(table)
        output = engine.output_files()
    elif engine_name == "sqlite":
        engine = SQLiteLake(workdir / "flush")
        engine.ingest(buffer)
        with Stopwatch() as flushing:
            target = engine.dump(workdir / "flush" / "taxi.sqlite")
        engine.close()
        output = [target]
    metrics["flush_seconds"] = flushing.seconds
    metrics["flush_rows_per_second"] = buffer.num_rows / flushing.seconds
    metrics["output_files"] = len(output)
    metrics["output_bytes"] = sum(path.stat().st_size for path in output)
    metrics["peak_rss_mb"] = peak_rss_mb()
    return metrics


def workload_w4(engine_name, workdir, data):
    import duckdb

    workdir = Path(workdir)
    sources = {}
    native_table = None
    if engine_name == "nodus-lake":
        clean = NodusLake(workdir / "clean", FLUSH_ROWS)
        clean.ingest(data)
        clean.flush()
        clean.close()
        sources["nodus clean (250k-row files)"] = workdir / "clean" / "data-*.parquet"
        streaming = NodusLake(workdir / "streaming", STREAMING_FLUSH_ROWS)
        streaming.ingest(data)
        streaming.flush()
        streaming.close()
        sources["nodus streaming (5k-row files)"] = workdir / "streaming" / "data-*.parquet"
        native_table = NodusLake(workdir / "memory", 1 << 20)
        native_table.ingest(data)
    elif engine_name == "duckdb":
        engine = DuckDBLake(workdir / "parts")
        engine.ingest(data)
        engine.close()
        sources["duckdb COPY parts"] = workdir / "parts" / "part-*.parquet"
    elif engine_name == "pyarrow":
        engine = PyArrowLake(workdir / "parts")
        engine.ingest(data)
        sources["pyarrow parts"] = workdir / "parts" / "part-*.parquet"
    elif engine_name == "sqlite":
        return {"n/a": "SQLite writes a database file, not Parquet, so DuckDB cannot read its output as Parquet"}
    query = ("SELECT vendor_id, count(*) AS trips, sum(total_amount) AS revenue, avg(trip_distance) AS distance "
             "FROM read_parquet('{pattern}') GROUP BY 1 ORDER BY 1")
    results = {}
    for label, pattern in sources.items():
        files = sorted(pattern.parent.glob(pattern.name))
        connection = duckdb.connect(":memory:")
        latencies = []
        answer = None
        for _ in range(FRESHNESS_REPEATS):
            started = time.perf_counter()
            answer = connection.execute(query.format(pattern=pattern.as_posix())).fetchall()
            latencies.append((time.perf_counter() - started) * 1e3)
        connection.close()
        summary = latency_summary(latencies)
        results[label] = {
            "files": len(files),
            "bytes": sum(path.stat().st_size for path in files),
            "median_ms": summary["median_us"],
            "p99_ms": summary["p99_us"],
            "revenue_total": round(sum(row[2] for row in answer), 2),
            "trips_total": sum(row[1] for row in answer),
        }
    if native_table is not None:
        results["nodus in-memory native, ungrouped (no files)"] = _time_native(native_table.table)
        native_table.close()
    if engine_name == "duckdb":
        results["duckdb COPY parts, ungrouped"] = _time_ungrouped(sources["duckdb COPY parts"])
    return {"sources": results, "peak_rss_mb": peak_rss_mb()}


def _time_native(table):
    latencies = []
    revenue = None
    for _ in range(FRESHNESS_REPEATS):
        started = time.perf_counter()
        revenue = table.sum("total_amount")
        table.average("trip_distance")
        latencies.append((time.perf_counter() - started) * 1e3)
    summary = latency_summary(latencies)
    return {"files": 0, "bytes": 0, "median_ms": summary["median_us"], "p99_ms": summary["p99_us"],
            "revenue_total": round(revenue, 2), "trips_total": None}


def _time_ungrouped(pattern):
    import duckdb

    files = sorted(pattern.parent.glob(pattern.name))
    connection = duckdb.connect(":memory:")
    latencies = []
    answer = None
    for _ in range(FRESHNESS_REPEATS):
        started = time.perf_counter()
        answer = connection.execute(UNGROUPED_QUERY.format(pattern=pattern.as_posix())).fetchall()
        latencies.append((time.perf_counter() - started) * 1e3)
    connection.close()
    summary = latency_summary(latencies)
    trips, revenue, _ = answer[0]
    return {"files": len(files), "bytes": sum(path.stat().st_size for path in files),
            "median_ms": summary["median_us"], "p99_ms": summary["p99_us"],
            "revenue_total": round(revenue, 2), "trips_total": trips}


WORKLOAD_FUNCTIONS = {"W1": workload_w1, "W2": workload_w2, "W3": workload_w3, "W4": workload_w4}


def worker(arguments):
    started = time.perf_counter()
    result = {"workload": arguments.workload, "engine": arguments.engine}
    try:
        data, source = load_taxi()
        result["source"] = {"path": str(source), "sha256": file_sha256(source), "rows": data.num_rows}
        workdir = Path(arguments.workdir)
        if workdir.exists():
            shutil.rmtree(workdir)
        workdir.mkdir(parents=True)
        result["status"] = "ok"
        result["metrics"] = WORKLOAD_FUNCTIONS[arguments.workload](arguments.engine, workdir, data)
    except ImportError as error:
        result.update({"status": "unavailable", "reason": f"missing package: {error}"})
    except Exception as error:
        result.update({"status": "error", "reason": f"{type(error).__name__}: {error}"})
    result["wall_seconds"] = time.perf_counter() - started
    write_json(arguments.result, result)


def orchestrate(arguments):
    combined = {"dataset": "NYC TLC Yellow Taxi, January 2024, first 1,000,000 rows", "runs": []}
    scratch = Path(arguments.scratch)
    for workload in arguments.workloads.split(","):
        engines = arguments.engines.split(",") if arguments.engines else ENGINES_BY_WORKLOAD[workload]
        for engine in engines:
            result_file = scratch / f"{workload}-{engine}.json"
            if result_file.exists():
                result_file.unlink()
            command = [sys.executable, str(Path(__file__).resolve()), "worker", "--workload", workload,
                       "--engine", engine, "--workdir", str(scratch / f"{workload}-{engine}"),
                       "--result", str(result_file)]
            print(f"running {workload} / {engine}", flush=True)
            try:
                subprocess.run(command, timeout=arguments.timeout_seconds, check=False)
                run = json.loads(result_file.read_text(encoding="utf-8"))
            except subprocess.TimeoutExpired:
                run = {"workload": workload, "engine": engine, "status": "timeout"}
            except (OSError, ValueError) as error:
                run = {"workload": workload, "engine": engine, "status": "error", "reason": str(error)}
            combined["runs"].append(run)
            print(f"  {workload}/{engine}: {run.get('status')}", flush=True)
            write_json(arguments.out, combined)
    print(render_tables(combined))


def render_tables(combined):
    runs = combined["runs"]
    lines = []
    w1 = [[r["engine"], f"{r['metrics']['rows_per_second']:,.0f}", f"{r['metrics']['ingest_seconds']:.2f}",
           f"{r['metrics']['peak_rss_mb']:,.0f}"] for r in runs if r["workload"] == "W1" and r.get("status") == "ok"]
    lines.append("### Workload 1: streaming ingestion of 1,000,000 taxi trips")
    lines.append(markdown_table(["engine", "rows/s", "seconds", "peak RSS MB"], w1))
    w2 = [[r["engine"], f"{r['metrics']['upserts_per_second']:,.0f}", f"{r['metrics']['deletes_per_second']:,.0f}",
           f"{r['metrics']['rss_mb_min']:,.0f} to {r['metrics']['rss_mb_max']:,.0f}", f"{r['metrics']['peak_rss_mb']:,.0f}"]
          for r in runs if r["workload"] == "W2" and r.get("status") == "ok"]
    lines.append("### Workload 2: 500,000 keyed updates and 100,000 deletes")
    lines.append(markdown_table(["engine", "upserts/s", "deletes/s", "RSS range MB", "peak RSS MB"], w2))
    w3 = [[r["engine"], f"{r['metrics']['flush_seconds']:.3f}", f"{r['metrics']['flush_rows_per_second']:,.0f}",
           r["metrics"]["output_files"], f"{r['metrics']['output_bytes'] / 1e6:,.2f}"]
          for r in runs if r["workload"] == "W3" and r.get("status") == "ok"]
    lines.append("### Workload 3: flush of a 250,000-row buffer")
    lines.append(markdown_table(["engine", "flush seconds", "rows/s", "files", "size MB"], w3))
    w4 = []
    for r in runs:
        if r["workload"] == "W4" and r.get("status") == "ok":
            for label, source in r["metrics"].get("sources", {}).items():
                w4.append([label, source["files"], f"{source['bytes'] / 1e6:,.2f}",
                           f"{source['median_ms']:,.1f}", f"{source['p99_ms']:,.1f}",
                           f"{source['revenue_total']:,.2f}"])
    lines.append("### Workload 4: DuckDB aggregate over each source (latency in ms)")
    lines.append(markdown_table(["source", "files", "size MB", "median ms", "p99 ms", "revenue total"], w4))
    failures = [f"{r['workload']}/{r['engine']}: {r.get('status')} {r.get('reason', '')}"
                for r in runs if r.get("status") not in ("ok", None) and "sources" not in r]
    if failures:
        lines.append("Runs that did not complete:")
        lines.extend(f"- {entry}" for entry in failures)
    return "\n\n".join(lines)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--workloads", default=",".join(WORKLOADS))
    run.add_argument("--engines", default=None)
    run.add_argument("--out", required=True)
    run.add_argument("--scratch", default=str(CACHE_DIRECTORY / "lake-runs"))
    run.add_argument("--timeout-seconds", type=int, default=5400)
    work = subparsers.add_parser("worker")
    work.add_argument("--workload", required=True, choices=WORKLOADS)
    work.add_argument("--engine", required=True, choices=ENGINES)
    work.add_argument("--workdir", required=True)
    work.add_argument("--result", required=True)
    return parser


if __name__ == "__main__":
    parsed = build_parser().parse_args()
    if parsed.command == "run":
        orchestrate(parsed)
    else:
        worker(parsed)
