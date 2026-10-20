# Documentation

Start with the [README](../README.md) for what NodusDB is and a five-minute tour. This folder holds the rest.

## Using it

| Guide | What it covers |
|---|---|
| [The graph engine](graph.md) | Edges, k-hop queries, common neighbors, string keys, durability, the memory limit, and how to get the speed |
| [Permissions](permissions.md) | The schema language, tuples, checks, tokens, nested groups and the reachability index |
| [The lake engine](lake-guide.md) | `LakeTable`: upserts and deletes into Parquet, Arrow batches, the files it writes, and how to read them from DuckDB |
| [Operations](operations.md) | Crash recovery, shipping the log to a bucket, followers and takeover, erasure, change feeds, time travel, upgrading |
| [Benchmarks](benchmarks.md) | Every measurement, how it was taken, what it shows and what it does not |

Runnable versions of the guides are in [`examples/`](../examples).

## Understanding it

| Page | What it covers |
|---|---|
| [How it works](how-it-works.md) | The engine from first principles: sets, hash tables, tiers, the seqlock, the log, columns |
| [Architecture](architecture.md) | Packages and byte layouts: native memory, the log and snapshot formats, the signed chain, the lake buffer |
| [The lake design](lake.md) | The lake write path in detail: lifecycle, tombstones, Parquet layout, the C interface |
| [Decisions](decisions.md) | Every design decision with its evidence, numbered |
| [Engineering reviews](designs) | The reviews behind each build step |

## Working on it

| Page | What it covers |
|---|---|
| [CONTRIBUTING](../CONTRIBUTING.md) | How to help, the rules the code follows, how a change is reviewed |
| [Building and testing](building.md) | Toolchain, native build, test suites, benchmarks, release wheels |
| [TODOS](../TODOS.md) | Open work, each item with its reason and size |
| [CHANGELOG](../CHANGELOG.md) | What changed in each release |
