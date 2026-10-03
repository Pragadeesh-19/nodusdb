import argparse
import random
import sqlite3
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import networkx as nx  # noqa: E402

from nodusdb import Graph  # noqa: E402

SINGLE = "single"
BATCH = "batch"
MODES = (SINGLE, BATCH)


def generate_unique_edges(count, nodes, rng):
    edges = set()
    while len(edges) < count:
        u = rng.randrange(nodes)
        v = rng.randrange(nodes)
        if u != v:
            edges.add((u, v))
    edge_list = list(edges)
    rng.shuffle(edge_list)
    return edge_list


class NodusBackend:
    name = "nodusdb"

    def __init__(self):
        self.graph = Graph()

    def insert_single(self, edges):
        add = self.graph.add_edge
        for u, v in edges:
            add(u, v)

    def insert_batch(self, edges):
        self.graph.add_edges_from(edges)

    def delete_single(self, edges):
        remove = self.graph.remove_edge
        for u, v in edges:
            remove(u, v)

    def delete_batch(self, edges):
        self.graph.remove_edges_from(edges)

    def khop(self, start, depth):
        return self.graph.khop(start, depth)

    def common_neighbors(self, u, v):
        return self.graph.common_neighbors(u, v)

    def close(self):
        self.graph.close()


class NetworkxBackend:
    name = "networkx"

    def __init__(self):
        self.graph = nx.DiGraph()

    def insert_single(self, edges):
        add = self.graph.add_edge
        for u, v in edges:
            add(u, v)

    def insert_batch(self, edges):
        self.graph.add_edges_from(edges)

    def delete_single(self, edges):
        remove = self.graph.remove_edge
        for u, v in edges:
            remove(u, v)

    def delete_batch(self, edges):
        self.graph.remove_edges_from(edges)

    def khop(self, start, depth):
        if start not in self.graph:
            return []
        reached = nx.single_source_shortest_path_length(self.graph, start, cutoff=depth)
        reached.pop(start, None)
        return list(reached)

    def common_neighbors(self, u, v):
        if u not in self.graph or v not in self.graph:
            return []
        return list(set(self.graph.successors(u)) & set(self.graph.successors(v)))

    def close(self):
        self.graph = None


KHOP_SQL = """
WITH RECURSIVE reach(node, depth) AS (
    SELECT ?, 0
    UNION
    SELECT e.v, r.depth + 1 FROM reach r JOIN edges e ON e.u = r.node WHERE r.depth < ?
)
SELECT DISTINCT node FROM reach WHERE node != ?
"""


class SqliteBackend:
    name = "sqlite3"

    def __init__(self):
        self.db = sqlite3.connect(":memory:")
        self.db.execute("CREATE TABLE edges (u INTEGER NOT NULL, v INTEGER NOT NULL, PRIMARY KEY (u, v)) WITHOUT ROWID")
        self.db.execute("CREATE INDEX edges_v ON edges (v)")

    def insert_single(self, edges):
        with self.db:
            for u, v in edges:
                self.db.execute("INSERT INTO edges (u, v) VALUES (?, ?)", (u, v))

    def insert_batch(self, edges):
        with self.db:
            self.db.executemany("INSERT INTO edges (u, v) VALUES (?, ?)", edges)

    def delete_single(self, edges):
        with self.db:
            for u, v in edges:
                self.db.execute("DELETE FROM edges WHERE u = ? AND v = ?", (u, v))

    def delete_batch(self, edges):
        with self.db:
            self.db.executemany("DELETE FROM edges WHERE u = ? AND v = ?", edges)

    def khop(self, start, depth):
        rows = self.db.execute(KHOP_SQL, (start, depth, start)).fetchall()
        return [row[0] for row in rows]

    def common_neighbors(self, u, v):
        rows = self.db.execute(
            "SELECT v FROM edges WHERE u = ? INTERSECT SELECT v FROM edges WHERE u = ?", (u, v)
        ).fetchall()
        return [row[0] for row in rows]

    def close(self):
        self.db.close()


BACKENDS = {"nodusdb": NodusBackend, "networkx": NetworkxBackend, "sqlite3": SqliteBackend}


def timed(function, *args):
    start = time.perf_counter()
    result = function(*args)
    return time.perf_counter() - start, result


def latency_summary(samples):
    return statistics.median(samples) * 1e6, statistics.mean(samples) * 1e6


def run_configuration(backend_class, mode, inserts, deletes, starts, pairs, depths):
    backend = backend_class()
    try:
        insert = backend.insert_batch if mode == BATCH else backend.insert_single
        delete = backend.delete_batch if mode == BATCH else backend.delete_single
        insert_seconds, _ = timed(insert, inserts)
        delete_seconds, _ = timed(delete, deletes)
        metrics = {
            "insert_per_second": len(inserts) / insert_seconds,
            "delete_per_second": len(deletes) / delete_seconds,
        }
        for depth in depths:
            samples = [timed(backend.khop, start, depth)[0] for start in starts]
            metrics[f"khop{depth}_median_us"], metrics[f"khop{depth}_mean_us"] = latency_summary(samples)
        samples = [timed(backend.common_neighbors, u, v)[0] for u, v in pairs]
        metrics["common_median_us"], metrics["common_mean_us"] = latency_summary(samples)
        return metrics
    finally:
        backend.close()


def verify_agreement(rng, nodes=2_000, edge_count=8_000, queries=50):
    edges = generate_unique_edges(edge_count, nodes, rng)
    removed = rng.sample(edges, len(edges) // 4)
    configurations = {}
    for name, backend_class in BACKENDS.items():
        for mode in MODES:
            backend = backend_class()
            insert = backend.insert_batch if mode == BATCH else backend.insert_single
            delete = backend.delete_batch if mode == BATCH else backend.delete_single
            insert(edges)
            delete(removed)
            configurations[f"{name}/{mode}"] = backend
    try:
        for _ in range(queries):
            start = rng.randrange(nodes)
            depth = rng.choice((2, 3))
            khop_answers = {label: frozenset(b.khop(start, depth)) for label, b in configurations.items()}
            if len(set(khop_answers.values())) != 1:
                raise AssertionError(f"khop disagreement start={start} depth={depth}: {khop_answers}")
            u, v = rng.randrange(nodes), rng.randrange(nodes)
            common_answers = {label: frozenset(b.common_neighbors(u, v)) for label, b in configurations.items()}
            if len(set(common_answers.values())) != 1:
                raise AssertionError(f"common disagreement u={u} v={v}: {common_answers}")
    finally:
        for backend in configurations.values():
            backend.close()
    print(f"cross-check passed: {queries} khop and {queries} common-neighbor queries agree "
          f"across {len(configurations)} backend/mode configurations")


def format_table(results):
    columns = [
        ("insert_per_second", "insert/s", "{:,.0f}"),
        ("delete_per_second", "delete/s", "{:,.0f}"),
        ("khop2_median_us", "2-hop med us", "{:,.1f}"),
        ("khop3_median_us", "3-hop med us", "{:,.1f}"),
        ("khop3_mean_us", "3-hop mean us", "{:,.1f}"),
        ("common_median_us", "common med us", "{:,.1f}"),
    ]
    header = f"{'configuration':<20}" + "".join(f"{label:>16}" for _, label, _ in columns)
    lines = [header, "-" * len(header)]
    for label, metrics in results:
        row = f"{label:<20}" + "".join(f"{fmt.format(metrics[key]):>16}" for key, _, fmt in columns)
        lines.append(row)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Compare nodusdb with networkx and sqlite3.")
    parser.add_argument("--edges", type=int, default=1_000_000)
    parser.add_argument("--deletes", type=int, default=500_000)
    parser.add_argument("--nodes", type=int, default=200_000)
    parser.add_argument("--queries", type=int, default=200)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--backends", default="nodusdb,networkx,sqlite3")
    parser.add_argument("--modes", default="single,batch")
    parser.add_argument("--skip-verify", action="store_true")
    arguments = parser.parse_args()

    selected = [name.strip() for name in arguments.backends.split(",") if name.strip()]
    modes = [mode.strip() for mode in arguments.modes.split(",") if mode.strip()]
    rng = random.Random(arguments.seed)

    if not arguments.skip_verify:
        verify_agreement(rng)

    inserts = generate_unique_edges(arguments.edges, arguments.nodes, rng)
    deletes = rng.sample(inserts, min(arguments.deletes, len(inserts)))
    starts = [rng.randrange(arguments.nodes) for _ in range(arguments.queries)]
    pairs = [(rng.randrange(arguments.nodes), rng.randrange(arguments.nodes)) for _ in range(arguments.queries)]

    results = []
    for name in selected:
        for mode in modes:
            label = f"{name}/{mode}"
            print(f"running {label} ...", flush=True)
            metrics = run_configuration(BACKENDS[name], mode, inserts, deletes, starts, pairs, depths=(2, 3))
            results.append((label, metrics))

    print()
    print(f"edges={len(inserts):,} deletes={len(deletes):,} nodes={arguments.nodes:,} queries={arguments.queries}")
    print(format_table(results))


if __name__ == "__main__":
    main()
