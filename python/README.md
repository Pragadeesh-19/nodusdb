# nodusdb

An embedded relationship graph engine for Python. It runs inside your process: a native library holds the graph in
memory, so there is no server to run and no Java to install.

- A graph with fast edge inserts and deletes, k-hop traversal and set intersection.
- Durability with a write-ahead log and snapshots.
- Typed relationship tuples, schemas and permission checks, including checks through nested groups.
- A signed log shipped to object storage, with followers, restore and takeover.
- Erasure, change watching and time travel (`as_of`).
- A lakehouse write path that batches upserts and deletes into Parquet.

## Install

```sh
pip install nodusdb
```

The wheel carries its own native library. It needs Python 3.10 or newer and one of:

| Platform | Requirement |
|---|---|
| Linux x86_64 or arm64 | glibc 2.34 or newer (Ubuntu 22.04 and later, Debian 12, RHEL 9) |
| macOS x86_64 or arm64 | macOS 11 or newer |
| Windows x86_64 | none |

Alpine (musl) and older Linux distributions are not supported yet; pip reports "no matching distribution" there.

## A first graph

```python
import nodusdb

with nodusdb.Graph() as g:
    g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])

    print("Reachable nodes:", g.khop(start=1, max_depth=3))
    print("Common neighbors:", g.common_neighbors(3, 5))
```

```
Reachable nodes: [2, 5, 3, 4]
Common neighbors: [4]
```

`nodusdb.__version__` reports the installed version.

## Status

This is a beta release. One writer and many readers share a graph: reads run in parallel and writes on one handle run one at a time.
The [README](https://github.com/Pragadeesh-19/nodusdb#readme) covers each feature, the
[limits](https://github.com/Pragadeesh-19/nodusdb#limits) and the
[compatibility promise](https://github.com/Pragadeesh-19/nodusdb#compatibility) for 0.1.x. Changes are listed in the
[changelog](https://github.com/Pragadeesh-19/nodusdb/blob/main/CHANGELOG.md).

## License

Apache License 2.0. The native library is compiled with GraalVM Community Edition Native Image; its runtime
license is in the `licenses` folder of the installed distribution, as described in `NOTICE`.
