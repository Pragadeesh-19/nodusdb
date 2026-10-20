# The graph engine

A guide to using `nodusdb.Graph`. It covers what the graph is, every call you need to build and query one, how to keep it on disk, and how to get the speed the benchmarks show. Every example on this page is in [`examples/`](../examples) and runs as part of the test suite.

## What it is

`Graph` is a directed graph held in memory, one set of neighbors per node, in both directions. You add and remove edges, and ask questions that start from a node: who does it point at, who points at it, what is within *k* hops, what do two nodes have in common. Each call goes straight into a native library inside your process. There is no server and no query language.

Use it when the graph changes constantly and the questions have to be answered in microseconds:

- An agent's working memory, where facts are written and dropped all the time.
- Live access control, where group membership changes and every request needs a yes or no.
- Session or presence graphs, where edges live for minutes.

Do not use it for whole-graph analytics such as PageRank or connected components. Columnar engines are built for scans that read every edge, and this one is built for touching a few.

## Install

```sh
pip install nodusdb
```

You need Python 3.10 or newer on Linux (x86_64 or arm64, glibc 2.34 or newer), macOS 11 or newer (Intel or Apple silicon) or Windows x86_64. The wheel carries the native library. There is no JVM to install.

## Edges and queries

```python
from array import array
import nodusdb

with nodusdb.Graph() as g:
    g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])

    g.has_edge(1, 2)               # True
    g.degree(1)                    # 2, edges leaving node 1
    g.in_degree(4)                 # 2, edges arriving at node 4
    g.khop(start=1, max_depth=3)   # [2, 5, 3, 4]
    g.common_neighbors(3, 5)       # [4]

    g.remove_edge(3, 4)
    g.khop(start=1, max_depth=3)   # [2, 5, 3, 4]: 4 is still reachable through 5

    g.add_edges_from(array("q", [10, 11, 11, 12, 12, 13]))   # flat buffer, the fastest input
```

What each call does:

| Call | Returns |
|---|---|
| `add_edge(u, v)` | `True` if the edge is new, `False` if it was already there |
| `remove_edge(u, v)` | `True` if it removed an edge, `False` if there was none |
| `add_edges_from(edges)` | the number of edges that were new. Takes a list of pairs or a flat `array('q')` of `u, v, u, v, ...` |
| `remove_edges_from(edges)` | the number removed |
| `has_edge(u, v)` | `bool` |
| `degree(u)`, `in_degree(v)` | counts. A node the graph has never seen has degree 0 |
| `khop(start, max_depth)` | every node reachable within `max_depth` hops, in breadth-first order. It leaves out the start node, and `max_depth=0` gives `[]` |
| `common_neighbors(u, v)` | the nodes both `u` and `v` point at |

A few rules that are easy to trip over:

- Adding an edge that exists, or removing one that does not, does nothing. It writes nothing to the log of a durable graph either.
- Edges are directed. `add_edge(1, 2)` does not make `2 -> 1`.
- A self-loop is allowed. `khop` leaves the start node out, so it never reports the loop.
- `khop` is safe on cycles. Each node appears once.
- Integer ids run from 0 to 2,147,483,638. Anything outside raises `ValueError`.
- A batch is checked as a whole before any edge is applied, so one bad id leaves the graph as it was.

## Names instead of numbers

A node can be a string or a `uuid.UUID`:

```python
with nodusdb.Graph() as g:
    g.add_edge("user:alice", "role:admin")
    g.add_edge("user:bob", "role:admin")
    g.khop("user:alice", 1)                          # ['role:admin']
    g.common_neighbors("user:alice", "user:bob")     # ['role:admin']
```

Strings are mapped once to dense integers by a symbol table, and the engine only ever sees integers. A graph uses one kind of key. The first key written claims the graph, and a durable graph remembers the claim across restarts. Mixing kinds raises `TypeError`. A read never adds a name: asking about a name the graph has not seen answers "absent".

On a durable graph a new name is forced to disk before any edge that uses it, so a crash cannot leave an edge pointing at a name that was lost.

## Keeping it on disk

Give the graph a directory and it becomes durable. It recovers whatever the directory holds when it opens, and every change is logged.

```python
with nodusdb.Graph(path="/data/graph", sync_mode="sync") as g:
    g.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])
    g.checkpoint()
    g.add_edge(4, 6)

with nodusdb.Graph(path="/data/graph") as g:
    g.khop(1, 4)                  # [2, 5, 3, 4, 6]
```

| Setting | Meaning |
|---|---|
| `sync_mode="async"` (default) | Writes go to the log in 10 ms batches. A killed process loses only what was still in the buffer. A power cut can lose the last 10 ms. |
| `sync_mode="sync"` | A single-edge call returns after its record is on disk. Every acknowledged write survives a kill or a power cut. |

Batch calls group-commit in either mode: the log waits for the disk once per batch, not once per edge. That is why a synchronous batch load runs at nearly the asynchronous rate (1.17 million edges per second against 1.04 to 1.11 million in the Pokec run), while a synchronous single call is disk-bound (about 3.6 ms on the test laptop, 203 calls per second). Load in batches.

`checkpoint()` writes a full snapshot and trims the log. Leaving the `with` block checkpoints and releases the directory lock, so a second handle or process cannot open the same directory at once. On the 30.6-million-edge Pokec graph a checkpoint takes about 9 seconds and recovery takes 2.8 seconds in a fresh process. [Operations](operations.md) covers what survives which failure, and how recovery treats a torn log.

## Limiting memory

```python
with nodusdb.Graph(max_memory_mb=256) as g:
    try:
        g.add_edges_from(edges)
    except nodusdb.NodusMemoryError:
        pass     # the graph holds what fit, and every query still works
```

The limit counts the graph's native memory: adjacency storage and the symbol table. A write that would pass it raises `NodusMemoryError` before anything changes, and on a durable graph the rejected write never reaches the log. Removals always work, and a write that needs no new storage still works. `add_edges_from` applies edges in order and stops at the first that does not fit, so the ones before it stay.

Storage grows in doubling chunks, so the limit can trip while part of the budget is unused. A large node id needs a large node table, and that counts too.

## Threads

One writer, many readers. Reads take no lock and never block the writer. A reader that overlaps a write discards the result and retries, so a query never returns a half-applied change. Writes, checkpoint and close on one `Graph` run one at a time.

## Errors

Every exception derives from `NodusError`. The ones you meet with plain graphs:

| Exception | When |
|---|---|
| `ValueError` | An id or depth is out of range |
| `TypeError` | You mixed integer and string keys, or passed the wrong type |
| `NodusMemoryError` | A write would pass `max_memory_mb`. It is also a built-in `MemoryError` |
| `NodusCorruptLogError` | The log is damaged in the part known to be on disk |
| `NodusUpgradeRequiredError` | The directory came from 0.1.0rc1. Run `nodusdb.upgrade(path)` |
| `NodusIndeterminateError` | A disk flush failed, so one write may or may not have survived. Reopen the graph to find out |

The permission, shipping and history features add more, and each is described where it is used.

## Getting the speed

The numbers in the README come from these habits:

1. **Batch.** Each Python call into the native library costs a few microseconds. `add_edges_from` with a flat `array('q')` is the fastest input form, then a list of tuples, then single calls. On the 1-million-edge benchmark the three run at 162,689, 132,281 and 71,962 inserts per second.
2. **Close what you open.** A `Graph` owns native memory. Use `with`, or call `close()`, which is safe to call twice. A graph that is garbage collected while open is released by a finalizer, and a `ResourceWarning` names the leak. A durable graph holds a directory lock until it is closed.
3. **Read from many threads, write from one.** Reads scale across threads on one handle. Writes do not.
4. **Size the result buffer.** `khop` writes into a buffer of 65,536 entries per thread and grows it when a result is larger. If you routinely return millions of nodes, pass `result_capacity=` to `Graph`.

## What to read next

- [Permissions](permissions.md) turns the same engine into a relationship-based access check.
- [Operations](operations.md) covers shipping the log to a bucket, followers, erasure, change feeds and time travel.
- [How it works](how-it-works.md) explains why the numbers look the way they do.
- [Benchmarks](benchmarks.md) has every table with its method.
