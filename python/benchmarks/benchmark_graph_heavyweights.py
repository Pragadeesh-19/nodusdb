"""Graph engine comparison: NodusDB against Kùzu, DuckDB, SQLite, igraph, and NetworkX.

The orchestrator runs one worker process per (workload, engine) pair, so every RSS figure
belongs to a single engine. A worker that exceeds its budget or crashes is recorded and the
suite continues.

    python benchmark_graph_heavyweights.py run --out bench/baseline/graph-heavyweights-windows-dev.json
"""

import argparse
import hashlib
import json
import os
import random
import shutil
import subprocess
import sys
import time
from array import array
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pcsv
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from _heavy_common import (  # noqa: E402
    CACHE_DIRECTORY,
    Stopwatch,
    current_rss_mb,
    directory_bytes,
    ensure_download,
    latency_summary,
    markdown_table,
    optional_import,
    peak_rss_mb,
    write_json,
)

POKEC_URL = "https://snap.stanford.edu/data/soc-pokec-relationships.txt.gz"
POKEC_NODES = 1_700_000
CHUNK = 1 << 20
HUB_COUNT = 50
MEDIAN_COUNT = 100
NETWORKX_EDGE_LIMIT = 3_000_000
WORKLOAD_B_NODES = 50_000
WORKLOAD_B_PRELOAD = 200_000
WORKLOAD_B_OPS = 100_000
WORKLOAD_B_INSERT_SHARE, WORKLOAD_B_DELETE_SHARE = 0.60, 0.30
WORKLOAD_C_USERS, WORKLOAD_C_GROUPS_PER_TIER, WORKLOAD_C_RESOURCES = 10_000, 500, 50_000
WORKLOAD_C_TIERS = 4
WORKLOAD_C_CHECKS = 10_000
REACH_DEPTH = 5
ENGINE_ORDER = ["nodus-memory", "nodus-durable-async", "kuzu", "duckdb", "sqlite", "igraph", "networkx"]
WORKLOADS = ["A", "B", "C"]

KHOP_SQL = """
WITH RECURSIVE reach(node, depth) AS (
    SELECT CAST(? AS BIGINT), 0
    UNION
    SELECT e.dst, r.depth + 1 FROM reach r JOIN edges e ON e.src = r.node WHERE r.depth < ?
)
SELECT count(DISTINCT node) - 1 FROM reach
"""

REACH_SQL = """
WITH RECURSIVE reach(node, depth) AS (
    SELECT CAST(? AS BIGINT), 0
    UNION
    SELECT e.dst, r.depth + 1 FROM reach r JOIN edges e ON e.src = r.node WHERE r.depth < ?
)
SELECT count(*) FROM reach WHERE node = ?
"""


def load_pokec():
    gz = ensure_download(POKEC_URL, CACHE_DIRECTORY / "soc-pokec-relationships.txt.gz")
    parquet = CACHE_DIRECTORY / "soc-pokec-edges.parquet"
    if not parquet.exists():
        table = pcsv.read_csv(
            gz,
            read_options=pcsv.ReadOptions(column_names=["src", "dst"]),
            parse_options=pcsv.ParseOptions(delimiter="\t"),
            convert_options=pcsv.ConvertOptions(column_types={"src": pa.int64(), "dst": pa.int64()}),
        )
        pq.write_table(table, parquet)
    return pq.read_table(parquet)


def chunks_of(table, size=CHUNK):
    src = table.column("src")
    dst = table.column("dst")
    for start in range(0, table.num_rows, size):
        stop = min(start + size, table.num_rows)
        yield src.slice(start, stop - start).to_pylist(), dst.slice(start, stop - start).to_pylist()


def out_degrees(table):
    counted = table.group_by("src").aggregate([("dst", "count")])
    return dict(zip(counted.column("src").to_pylist(), counted.column("dst_count").to_pylist()))


class Engine:
    name = "base"

    def __init__(self, workdir):
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)

    def bulk_load(self, table):
        raise NotImplementedError

    def khop(self, start, depth):
        raise NotImplementedError

    def reachable(self, start, depth, target):
        return target in self.reach_list(start, depth)

    def reach_list(self, start, depth):
        raise NotImplementedError

    def add(self, u, v):
        raise NotImplementedError

    def remove(self, u, v):
        raise NotImplementedError

    def checkpoint(self):
        return None

    def footprint_bytes(self):
        return None

    def close(self):
        return None


class NodusEngine(Engine):
    def __init__(self, workdir, durable):
        super().__init__(workdir)
        from nodusdb import Graph

        self.durable = durable
        self.name = "nodus-durable-async" if durable else "nodus-memory"
        self.directory = self.workdir / "nodus-graph"
        self.graph = Graph(path=str(self.directory), sync_mode="async") if durable else Graph()

    def bulk_load(self, table):
        for src, dst in chunks_of(table):
            flat = array("q", [0]) * (2 * len(src))
            flat[0::2] = array("q", src)
            flat[1::2] = array("q", dst)
            self.graph.add_edges_from(flat)

    def khop(self, start, depth):
        return len(self.graph.khop(start, depth))

    def reach_list(self, start, depth):
        return self.graph.khop(start, depth)

    def add(self, u, v):
        self.graph.add_edge(u, v)

    def remove(self, u, v):
        self.graph.remove_edge(u, v)

    def checkpoint(self):
        if self.durable:
            self.graph.checkpoint()

    def footprint_bytes(self):
        return directory_bytes(self.directory) if self.durable else None

    def close(self):
        self.graph.close()


class KuzuEngine(Engine):
    name = "kuzu"

    def __init__(self, workdir):
        super().__init__(workdir)
        kuzu = optional_import("kuzu")
        if kuzu is None:
            raise ImportError("kuzu")
        self.path = self.workdir / "kuzu.db"
        self.database = kuzu.Database(str(self.path))
        self.connection = kuzu.Connection(self.database)
        self.connection.execute("CREATE NODE TABLE Node(id INT64, PRIMARY KEY (id))")
        self.connection.execute("CREATE REL TABLE Rel(FROM Node TO Node)")

    def bulk_load(self, table):
        import pyarrow.compute as compute

        node_ids = compute.unique(pa.concat_arrays([table.column("src").combine_chunks(),
                                                    table.column("dst").combine_chunks()]))
        nodes_csv = self.workdir / "kuzu-nodes.csv"
        rels_csv = self.workdir / "kuzu-rels.csv"
        pcsv.write_csv(pa.table({"id": node_ids}), nodes_csv)
        pcsv.write_csv(pa.table({"from": table.column("src"), "to": table.column("dst")}), rels_csv)
        self.connection.execute(f"COPY Node FROM '{nodes_csv.as_posix()}' (HEADER=true)")
        self.connection.execute(f"COPY Rel FROM '{rels_csv.as_posix()}' (HEADER=true)")

    def khop(self, start, depth):
        result = self.connection.execute(
            f"MATCH (a:Node {{id: $id}})-[*1..{depth}]->(b:Node) RETURN count(DISTINCT b.id)", {"id": start})
        return result.get_next()[0]

    def reach_list(self, start, depth):
        result = self.connection.execute(
            f"MATCH (a:Node {{id: $id}})-[*1..{depth}]->(b:Node) RETURN DISTINCT b.id", {"id": start})
        return _column_values(result)

    def reachable(self, start, depth, target):
        result = self.connection.execute(
            f"MATCH (a:Node {{id: $u}})-[*1..{depth}]->(b:Node {{id: $v}}) RETURN count(*)",
            {"u": start, "v": target})
        return result.get_next()[0] > 0

    def add(self, u, v):
        self.connection.execute(
            "MATCH (a:Node {id: $u}), (b:Node {id: $v}) CREATE (a)-[:Rel]->(b)", {"u": u, "v": v})

    def remove(self, u, v):
        self.connection.execute(
            "MATCH (a:Node {id: $u})-[r:Rel]->(b:Node {id: $v}) DELETE r", {"u": u, "v": v})

    def footprint_bytes(self):
        return directory_bytes(self.path)

    def close(self):
        self.connection.close()
        self.database.close()


def _column_values(result):
    values = []
    while result.has_next():
        values.append(result.get_next()[0])
    return values


class DuckDBEngine(Engine):
    name = "duckdb"

    def __init__(self, workdir):
        super().__init__(workdir)
        duckdb = optional_import("duckdb")
        if duckdb is None:
            raise ImportError("duckdb")
        self.path = self.workdir / "graph.duckdb"
        self.connection = duckdb.connect(str(self.path))
        self.connection.execute("CREATE TABLE edges(src BIGINT, dst BIGINT, PRIMARY KEY (src, dst))")

    def bulk_load(self, table):
        self.connection.register("incoming", table)
        self.connection.execute("INSERT INTO edges SELECT src, dst FROM incoming")
        self.connection.unregister("incoming")

    def khop(self, start, depth):
        return self.connection.execute(KHOP_SQL, [start, depth]).fetchone()[0]

    def reach_list(self, start, depth):
        rows = self.connection.execute(
            "WITH RECURSIVE reach(node, depth) AS (SELECT CAST(? AS BIGINT), 0 UNION "
            "SELECT e.dst, r.depth + 1 FROM reach r JOIN edges e ON e.src = r.node WHERE r.depth < ?) "
            "SELECT DISTINCT node FROM reach WHERE node != ?", [start, depth, start]).fetchall()
        return [row[0] for row in rows]

    def reachable(self, start, depth, target):
        return self.connection.execute(REACH_SQL, [start, depth, target]).fetchone()[0] > 0

    def add(self, u, v):
        self.connection.execute("INSERT INTO edges VALUES (?, ?)", [u, v])

    def remove(self, u, v):
        self.connection.execute("DELETE FROM edges WHERE src = ? AND dst = ?", [u, v])

    def footprint_bytes(self):
        return directory_bytes(self.path)

    def close(self):
        self.connection.close()


class SQLiteEngine(Engine):
    name = "sqlite"

    def __init__(self, workdir):
        super().__init__(workdir)
        import sqlite3

        self.path = self.workdir / "graph.sqlite"
        self.connection = sqlite3.connect(str(self.path))
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=NORMAL")
        self.connection.execute(
            "CREATE TABLE edges (src INTEGER NOT NULL, dst INTEGER NOT NULL, PRIMARY KEY (src, dst)) WITHOUT ROWID")
        self.connection.execute("CREATE INDEX edges_dst ON edges (dst)")

    def bulk_load(self, table):
        for src, dst in chunks_of(table):
            with self.connection:
                self.connection.executemany("INSERT INTO edges (src, dst) VALUES (?, ?)", zip(src, dst))

    def khop(self, start, depth):
        return self.connection.execute(KHOP_SQL, (start, depth)).fetchone()[0]

    def reach_list(self, start, depth):
        rows = self.connection.execute(
            "WITH RECURSIVE reach(node, depth) AS (SELECT ?, 0 UNION "
            "SELECT e.dst, r.depth + 1 FROM reach r JOIN edges e ON e.src = r.node WHERE r.depth < ?) "
            "SELECT DISTINCT node FROM reach WHERE node != ?", (start, depth, start)).fetchall()
        return [row[0] for row in rows]

    def reachable(self, start, depth, target):
        return self.connection.execute(REACH_SQL, (start, depth, target)).fetchone()[0] > 0

    def add(self, u, v):
        with self.connection:
            self.connection.execute("INSERT INTO edges (src, dst) VALUES (?, ?)", (u, v))

    def remove(self, u, v):
        with self.connection:
            self.connection.execute("DELETE FROM edges WHERE src = ? AND dst = ?", (u, v))

    def footprint_bytes(self):
        return directory_bytes(self.path)

    def close(self):
        self.connection.close()


class IgraphEngine(Engine):
    name = "igraph"

    def __init__(self, workdir):
        super().__init__(workdir)
        igraph = optional_import("igraph")
        if igraph is None:
            raise ImportError("igraph")
        self.igraph = igraph
        self.graph = igraph.Graph(n=POKEC_NODES, directed=True)

    def bulk_load(self, table):
        for src, dst in chunks_of(table):
            self.graph.add_edges(list(zip(src, dst)))

    def khop(self, start, depth):
        return len(self.graph.neighborhood(vertices=[start], order=depth, mode="out")[0]) - 1

    def reach_list(self, start, depth):
        return self.graph.neighborhood(vertices=[start], order=depth, mode="out")[0][1:]

    def add(self, u, v):
        self.graph.add_edges([(u, v)])

    def remove(self, u, v):
        edge = self.graph.get_eid(u, v, directed=True, error=False)
        if edge >= 0:
            self.graph.delete_edges([edge])


class NetworkXEngine(Engine):
    name = "networkx"

    def __init__(self, workdir):
        super().__init__(workdir)
        nx = optional_import("networkx")
        if nx is None:
            raise ImportError("networkx")
        self.nx = nx
        self.graph = nx.DiGraph()

    def bulk_load(self, table):
        for src, dst in chunks_of(table):
            self.graph.add_edges_from(zip(src, dst))

    def khop(self, start, depth):
        if start not in self.graph:
            return 0
        reached = self.nx.single_source_shortest_path_length(self.graph, start, cutoff=depth)
        return len(reached) - 1

    def reach_list(self, start, depth):
        if start not in self.graph:
            return []
        reached = self.nx.single_source_shortest_path_length(self.graph, start, cutoff=depth)
        reached.pop(start, None)
        return list(reached)

    def add(self, u, v):
        self.graph.add_edge(u, v)

    def remove(self, u, v):
        if self.graph.has_edge(u, v):
            self.graph.remove_edge(u, v)


ENGINE_FACTORIES = {
    "nodus-memory": lambda workdir: NodusEngine(workdir, durable=False),
    "nodus-durable-async": lambda workdir: NodusEngine(workdir, durable=True),
    "kuzu": KuzuEngine,
    "duckdb": DuckDBEngine,
    "sqlite": SQLiteEngine,
    "igraph": IgraphEngine,
    "networkx": NetworkXEngine,
}


def workload_a(engine_name, workdir, budget_seconds):
    table = load_pokec()
    edges = table.num_rows
    if engine_name == "networkx" and edges > NETWORKX_EDGE_LIMIT:
        return {"status": "skipped", "reason": f"networkx skipped above {NETWORKX_EDGE_LIMIT:,} edges"}
    engine = ENGINE_FACTORIES[engine_name](workdir)
    metrics = {"edges": edges}
    with Stopwatch() as ingest:
        engine.bulk_load(table)
    metrics["ingest_seconds"] = ingest.seconds
    metrics["ingest_edges_per_second"] = edges / ingest.seconds
    metrics["peak_rss_mb_after_ingest"] = peak_rss_mb()
    engine.checkpoint()
    metrics["footprint_bytes_after_ingest"] = engine.footprint_bytes()

    degrees = out_degrees(table)
    ranked = sorted(degrees.items(), key=lambda item: (-item[1], item[0]))
    hubs = [node for node, _ in ranked[:HUB_COUNT]]
    candidates = sorted(node for node, degree in degrees.items() if 5 <= degree <= 20)
    rng = random.Random(2026)
    medians = rng.sample(candidates, min(MEDIAN_COUNT, len(candidates)))
    metrics["hub_degrees"] = [degrees[node] for node in hubs]

    deadline = time.perf_counter() + budget_seconds
    for label, starts in (("hub", hubs), ("median", medians)):
        for depth in (2, 3):
            micro = []
            truncated = False
            for start in starts:
                if time.perf_counter() > deadline:
                    truncated = True
                    break
                started = time.perf_counter()
                engine.khop(start, depth)
                micro.append((time.perf_counter() - started) * 1e6)
            metrics[f"{label}_{depth}hop"] = {**latency_summary(micro), "truncated": truncated,
                                              "planned": len(starts)}
    metrics["peak_rss_mb"] = peak_rss_mb()
    engine.close()
    metrics["footprint_bytes_after_close"] = None
    return {"status": "ok", "metrics": metrics}


def workload_b(engine_name, workdir, budget_seconds):
    table = load_pokec()
    degrees = out_degrees(table)
    top = {node for node, _ in sorted(degrees.items(), key=lambda item: (-item[1], item[0]))[:WORKLOAD_B_NODES]}
    members = pa.array(sorted(top), pa.int64())
    keep = pc.and_(pc.is_in(table.column("src"), value_set=members),
                   pc.is_in(table.column("dst"), value_set=members))
    subgraph = table.filter(keep)
    rng = random.Random(7)
    order = list(range(subgraph.num_rows))
    rng.shuffle(order)
    subgraph = subgraph.take(pa.array(order, pa.int64()))
    src = subgraph.column("src").to_pylist()
    dst = subgraph.column("dst").to_pylist()
    preload = pa.table({"src": pa.array(src[:WORKLOAD_B_PRELOAD], pa.int64()),
                        "dst": pa.array(dst[:WORKLOAD_B_PRELOAD], pa.int64())})
    pool = list(zip(src[WORKLOAD_B_PRELOAD:], dst[WORKLOAD_B_PRELOAD:]))
    present = list(zip(preload.column("src").to_pylist(), preload.column("dst").to_pylist()))
    position = {edge: index for index, edge in enumerate(present)}
    nodes = sorted({node for edge in present for node in edge})

    engine = ENGINE_FACTORIES[engine_name](workdir)
    with Stopwatch() as preloading:
        engine.bulk_load(preload)
    schedule_rng = random.Random(11)
    inserts = int(WORKLOAD_B_OPS * WORKLOAD_B_INSERT_SHARE)
    deletes = int(WORKLOAD_B_OPS * WORKLOAD_B_DELETE_SHARE)
    queries = WORKLOAD_B_OPS - inserts - deletes
    kinds = ["insert"] * inserts + ["delete"] * deletes + ["query"] * queries
    schedule_rng.shuffle(kinds)

    latency = {"insert": [], "delete": [], "query2": [], "query3": []}
    deleted = []
    query_index = 0
    deadline = time.perf_counter() + budget_seconds
    executed = 0
    with Stopwatch() as churn:
        for kind in kinds:
            if time.perf_counter() > deadline:
                break
            if kind == "insert":
                if pool:
                    u, v = pool.pop()
                elif deleted:
                    u, v = deleted.pop()
                else:
                    continue
                started = time.perf_counter()
                engine.add(u, v)
                latency["insert"].append((time.perf_counter() - started) * 1e6)
                position[(u, v)] = len(present)
                present.append((u, v))
            elif kind == "delete":
                if not present:
                    continue
                index = schedule_rng.randrange(len(present))
                edge = present[index]
                last = present.pop()
                if index < len(present):
                    present[index] = last
                    position[last] = index
                del position[edge]
                started = time.perf_counter()
                engine.remove(*edge)
                latency["delete"].append((time.perf_counter() - started) * 1e6)
                deleted.append(edge)
            else:
                start = nodes[query_index % len(nodes)]
                depth = 2 if query_index % 2 == 0 else 3
                query_index += 1
                started = time.perf_counter()
                engine.khop(start, depth)
                latency[f"query{depth}"].append((time.perf_counter() - started) * 1e6)
            executed += 1
    metrics = {
        "preload_edges": preload.num_rows,
        "preload_seconds": preloading.seconds,
        "operations_planned": WORKLOAD_B_OPS,
        "operations_executed": executed,
        "churn_seconds": churn.seconds,
        "operations_per_second": executed / churn.seconds,
        "latency": {name: latency_summary(values) for name, values in latency.items()},
        "peak_rss_mb": peak_rss_mb(),
        "current_rss_mb": current_rss_mb(),
    }
    engine.close()
    return {"status": "ok", "metrics": metrics}


def rebac_graph(seed):
    rng = random.Random(seed)
    tiers = [[] for _ in range(WORKLOAD_C_TIERS)]
    next_id = WORKLOAD_C_USERS
    for tier in range(WORKLOAD_C_TIERS):
        for _ in range(WORKLOAD_C_GROUPS_PER_TIER):
            tiers[tier].append(next_id)
            next_id += 1
    resources = list(range(next_id, next_id + WORKLOAD_C_RESOURCES))
    membership = {user: set(rng.sample(tiers[0], 3)) for user in range(WORKLOAD_C_USERS)}
    parent_of = {}
    for tier in range(WORKLOAD_C_TIERS - 1):
        for group in tiers[tier]:
            parent_of[group] = rng.choice(tiers[tier + 1])
    resources_of = {group: [] for group in tiers[-1]}
    attachments = []
    for resource in resources:
        group = rng.choice(tiers[-1])
        resources_of[group].append(resource)
        attachments.append((group, resource))
    hierarchy = [(group, parent) for group, parent in parent_of.items()]
    return membership, parent_of, resources_of, hierarchy, attachments, tiers[0], resources


def model_reachable_resources(user, membership, parent_of, resources_of):
    reached = []
    for group in membership[user]:
        current = group
        while current in parent_of:
            current = parent_of[current]
        reached.extend(resources_of[current])
    return reached


def workload_c(engine_name, workdir, budget_seconds):
    membership, parent_of, resources_of, hierarchy, attachments, first_tier, resources = rebac_graph(seed=13)
    rows = []
    for user, groups in membership.items():
        rows.extend((user, group) for group in groups)
    rows.extend(hierarchy)
    rows.extend(attachments)
    table = pa.table({"src": pa.array([u for u, _ in rows], pa.int64()),
                      "dst": pa.array([v for _, v in rows], pa.int64())})
    engine = ENGINE_FACTORIES[engine_name](workdir)
    with Stopwatch() as loading:
        engine.bulk_load(table)

    rng = random.Random(17)
    results = []
    update_latency, check_latency = [], []
    deadline = time.perf_counter() + budget_seconds
    executed = 0
    for _ in range(WORKLOAD_C_CHECKS):
        if time.perf_counter() > deadline:
            break
        user = rng.randrange(WORKLOAD_C_USERS)
        current = sorted(membership[user])
        leaving = current[rng.randrange(len(current))]
        joining = rng.choice(first_tier)
        if joining not in membership[user]:
            started = time.perf_counter()
            engine.remove(user, leaving)
            engine.add(user, joining)
            update_latency.append((time.perf_counter() - started) * 1e6)
            membership[user].discard(leaving)
            membership[user].add(joining)
        reachable = model_reachable_resources(user, membership, parent_of, resources_of)
        if rng.random() < 0.5 and reachable:
            target = reachable[rng.randrange(len(reachable))]
        else:
            target = rng.choice(resources)
        started = time.perf_counter()
        answer = engine.reachable(user, REACH_DEPTH, target)
        check_latency.append((time.perf_counter() - started) * 1e6)
        results.append("1" if answer else "0")
        executed += 1
    digest = hashlib.sha256("".join(results).encode("ascii")).hexdigest()
    metrics = {
        "users": WORKLOAD_C_USERS,
        "groups": WORKLOAD_C_GROUPS_PER_TIER * WORKLOAD_C_TIERS,
        "resources": WORKLOAD_C_RESOURCES,
        "edges_loaded": table.num_rows,
        "load_seconds": loading.seconds,
        "checks_planned": WORKLOAD_C_CHECKS,
        "checks_executed": executed,
        "positive_answers": results.count("1"),
        "answer_digest": digest,
        "membership_updates": len(update_latency),
        "check": latency_summary(check_latency),
        "update": latency_summary(update_latency),
        "peak_rss_mb": peak_rss_mb(),
    }
    engine.close()
    return {"status": "ok", "metrics": metrics}


WORKLOAD_FUNCTIONS = {"A": workload_a, "B": workload_b, "C": workload_c}


def worker(arguments):
    started = time.perf_counter()
    result = {"workload": arguments.workload, "engine": arguments.engine}
    workdir = Path(arguments.workdir)
    if workdir.exists():
        shutil.rmtree(workdir)
    try:
        if arguments.engine not in ENGINE_FACTORIES:
            raise ValueError(f"unknown engine {arguments.engine}")
        outcome = WORKLOAD_FUNCTIONS[arguments.workload](arguments.engine, workdir, arguments.budget_seconds)
        result.update(outcome)
    except ImportError as error:
        result.update({"status": "unavailable", "reason": f"missing package: {error}"})
    except Exception as error:
        result.update({"status": "error", "reason": f"{type(error).__name__}: {error}"})
    result["wall_seconds"] = time.perf_counter() - started
    write_json(arguments.result, result)


def orchestrate(arguments):
    workloads = arguments.workloads.split(",")
    engines = arguments.engines.split(",")
    combined = {"machine": os.environ.get("NODUS_BENCH_MACHINE", "windows-dev"), "runs": []}
    scratch = Path(arguments.scratch)
    for workload in workloads:
        for engine in engines:
            result_file = scratch / f"{workload}-{engine}.json"
            if result_file.exists():
                result_file.unlink()
            command = [sys.executable, str(Path(__file__).resolve()), "worker", "--workload", workload,
                       "--engine", engine, "--workdir", str(scratch / f"{workload}-{engine}"),
                       "--result", str(result_file), "--budget-seconds", str(arguments.budget_seconds)]
            print(f"running workload {workload} / {engine}", flush=True)
            try:
                subprocess.run(command, timeout=arguments.timeout_seconds, check=False)
                run = json.loads(result_file.read_text(encoding="utf-8"))
            except subprocess.TimeoutExpired:
                run = {"workload": workload, "engine": engine, "status": "timeout",
                       "reason": f"exceeded {arguments.timeout_seconds} seconds"}
            except (OSError, ValueError) as error:
                run = {"workload": workload, "engine": engine, "status": "error",
                       "reason": f"worker produced no result: {error}"}
            combined["runs"].append(run)
            print(f"  {workload}/{engine}: {run.get('status')}", flush=True)
            write_json(arguments.out, combined)
    print(render_tables(combined))


def render_tables(combined):
    lines = []
    runs = combined["runs"]
    a_rows, b_rows, c_rows = [], [], []
    for run in runs:
        if run["workload"] == "A" and run.get("status") == "ok":
            m = run["metrics"]
            a_rows.append([
                run["engine"], f"{m['ingest_edges_per_second']:,.0f}",
                f"{m['ingest_seconds']:.1f}", f"{m['peak_rss_mb']:,.0f}",
                "n/a" if m["footprint_bytes_after_ingest"] is None else f"{m['footprint_bytes_after_ingest'] / 1e6:,.1f}",
                f"{m['hub_2hop']['median_us']:,.0f} / {m['hub_2hop']['p99_us']:,.0f}" if m['hub_2hop']['count'] else "n/a",
                f"{m['hub_3hop']['median_us']:,.0f} / {m['hub_3hop']['p99_us']:,.0f}" if m['hub_3hop']['count'] else "n/a",
                f"{m['median_2hop']['median_us']:,.0f} / {m['median_2hop']['p99_us']:,.0f}" if m['median_2hop']['count'] else "n/a",
                f"{m['median_3hop']['median_us']:,.0f} / {m['median_3hop']['p99_us']:,.0f}" if m['median_3hop']['count'] else "n/a",
            ])
        elif run["workload"] == "B" and run.get("status") == "ok":
            m = run["metrics"]
            lat = m["latency"]
            b_rows.append([run["engine"], f"{m['operations_per_second']:,.0f}", f"{m['peak_rss_mb']:,.0f}",
                           f"{lat['insert']['median_us'] or 0:,.1f}", f"{lat['delete']['median_us'] or 0:,.1f}",
                           f"{lat['query2']['median_us'] or 0:,.1f} / {lat['query3']['median_us'] or 0:,.1f}"])
        elif run["workload"] == "C" and run.get("status") == "ok":
            m = run["metrics"]
            c_rows.append([run["engine"], f"{m['check']['median_us'] or 0:,.1f}",
                           f"{m['check']['p99_us'] or 0:,.1f}", m["checks_executed"], m["answer_digest"][:12]])
    lines.append("### Workload A: Pokec full graph")
    lines.append(markdown_table(["engine", "ingest edges/s", "ingest s", "peak RSS MB", "footprint MB",
                                 "hub 2-hop med/p99 us", "hub 3-hop med/p99 us",
                                 "median 2-hop med/p99 us", "median 3-hop med/p99 us"], a_rows))
    lines.append("### Workload B: churn on a real Pokec subgraph")
    lines.append(markdown_table(["engine", "ops/s", "peak RSS MB", "insert med us", "delete med us",
                                 "2-hop / 3-hop med us"], b_rows))
    lines.append("### Workload C: ReBAC reachability under writes")
    lines.append(markdown_table(["engine", "check med us", "check p99 us", "checks", "answer digest"], c_rows))
    skipped = [f"{r['workload']}/{r['engine']}: {r.get('status')} {r.get('reason', '')}"
               for r in runs if r.get("status") != "ok"]
    if skipped:
        lines.append("Runs that did not complete:")
        lines.extend(f"- {entry}" for entry in skipped)
    return "\n\n".join(lines)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest="command", required=True)
    run = subparsers.add_parser("run")
    run.add_argument("--workloads", default=",".join(WORKLOADS))
    run.add_argument("--engines", default=",".join(ENGINE_ORDER))
    run.add_argument("--out", required=True)
    run.add_argument("--scratch", default=str(CACHE_DIRECTORY / "graph-runs"))
    run.add_argument("--timeout-seconds", type=int, default=5400)
    run.add_argument("--budget-seconds", type=int, default=1200)
    work = subparsers.add_parser("worker")
    work.add_argument("--workload", required=True, choices=WORKLOADS)
    work.add_argument("--engine", required=True)
    work.add_argument("--workdir", required=True)
    work.add_argument("--result", required=True)
    work.add_argument("--budget-seconds", type=int, default=1200)
    return parser


if __name__ == "__main__":
    parsed = build_parser().parse_args()
    if parsed.command == "run":
        orchestrate(parsed)
    else:
        worker(parsed)
