import tempfile
from pathlib import Path

import nodusdb

directory = Path(tempfile.mkdtemp(prefix="nodus-example-")) / "graph"

with nodusdb.Graph(path=directory, sync_mode="sync") as g:
    g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])
    g.checkpoint()
    g.add_edge(4, 6)

print("files:", sorted(entry.name for entry in directory.iterdir()))

with nodusdb.Graph(path=directory) as g:
    print("reachable after restart:", g.khop(1, 4))
    print("has 4 -> 6:", g.has_edge(4, 6))

with nodusdb.Graph(max_memory_mb=1) as small:
    try:
        small.add_edges_from([(i, i + 1) for i in range(10_000_000)])
    except nodusdb.NodusMemoryError as error:
        print("memory limit:", type(error).__name__)
    print("still queryable:", small.degree(0))
