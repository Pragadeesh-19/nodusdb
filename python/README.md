# nodusdb

Zanzibar-style permissions that run inside your Python process, on a log that is also your lake.

NodusDB keeps the relationship graph in your process's memory, so a permission check that must reflect every write so
far is a function call: 9 microseconds at the median over 560,497 relationships, where SpiceDB and OpenFGA take 4.5 ms
and 3.6 ms over the network ([the benchmark](https://github.com/Pragadeesh-19/nodusdb#the-numbers)). Every write also
goes into one signed log in your own bucket, which followers replicate from, which projects into an Iceberg table you
can query with SQL, and which `as_of` and `erase` read.

- Typed relationship tuples, a schema language and permission checks through nested groups and parent links.
- Tokens, so a check can refuse to answer from a state older than a given write.
- Durability with a write-ahead log and snapshots.
- A signed, hash-linked log shipped to S3, with followers, restore and takeover.
- An Iceberg v2 table of every change, readable by DuckDB, Spark and pyiceberg.
- Time travel (`as_of`), erasure across every copy, and a change feed.
- A general graph engine and a Parquet write buffer (`LakeTable`) on the same native-memory core.

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

## A first check

```python
import nodusdb

SCHEMA = """
schema 1
type user
type group { relation member: user | group#member }
type document {
  relation viewer: user | group#member
  permission view = viewer
}
"""

with nodusdb.Graph() as g:
    g.apply_schema(SCHEMA)
    g.add_tuple("document:readme", "viewer", "group:staff", "member")
    token = g.add_tuple("group:staff", "member", "user:alice")

    print(g.check("document:readme", "view", "user:alice", at_least=token))   # True
    print(g.check("document:readme", "view", "user:bob"))                     # False
```

`nodusdb.__version__` reports the installed version.

## Status

This is a beta release. One writer and many readers share a graph: reads run in parallel and writes on one handle run
one at a time. The [README](https://github.com/Pragadeesh-19/nodusdb#readme) covers each feature, the
[benchmarks](https://github.com/Pragadeesh-19/nodusdb#the-numbers), the
[limits](https://github.com/Pragadeesh-19/nodusdb#limits) and the
[compatibility promise](https://github.com/Pragadeesh-19/nodusdb#compatibility) for 0.1.x. Changes are listed in the
[changelog](https://github.com/Pragadeesh-19/nodusdb/blob/main/CHANGELOG.md).

## License

Apache License 2.0. The native library is compiled with GraalVM Community Edition Native Image; its runtime
license is in the `licenses` folder of the installed distribution, as described in `NOTICE`.
