# Contributing to NodusDB

Thank you for looking. This project is small enough that one person can hold all of it in their head, and big enough that it needs more people. This page tells you how to build it, how to run the tests, what the code looks like and what we ask of a change. If anything here is wrong or missing, a pull request that fixes it is a good first contribution.

## Start here

1. Read the [README](README.md) to see what the project does and [How it works](docs/how-it-works.md) to see why it is built this way. Twenty minutes with the second one will save you hours in the code.
2. Run the examples in [`examples/`](examples). Each is a short, runnable script.
3. Pick something to work on. [TODOS.md](TODOS.md) lists every open item with what it is, why it matters, how big it is and what it depends on. Items marked `Effort: S` are the smallest. If you want to work on something that is not listed, open an issue first and describe it, so nobody builds the same thing twice.

Good places to begin:

- **Wheels for more platforms.** Alpine (musl), older Linux and Windows on ARM are missing. The workflow is in `.github/workflows/wheels.yml`.
- **Cloud role credentials** for the S3 client (instance profiles, web identity). The credential seam exists and the providers are open.
- **A Prometheus or OpenTelemetry adapter** over `stats()`.
- **Performance.** Graph inserts recovered five of the six to eight times they lost when the graph moved to native memory, but are still below the build at commit `0b05475` (810,031 against 1.06 to 1.30 million flat inserts a second). Where the rest goes is not measured yet. See "The in-memory write path is slower" in TODOS.md.
- **Docs and examples.** If you got stuck on something, someone else will.

## Set up

You need:

- JDK 22 and Maven 3.9 or newer. The Java sources compile for release 22.
- Python 3.10 or newer.
- For the native library only: GraalVM Community Edition with `native-image` (25.0.2 is what CI and the wheels use), and on Windows Visual Studio 2022 with the C++ build tools.

The Java tests do not need GraalVM. They run on any JDK 22 or newer.

```sh
git clone https://github.com/Pragadeesh-19/nodusdb
cd nodusdb
mvn test                                   # the Java suite, assertions on
```

To run the Python tests you need the native library, which `mvn -Pnative` builds. [docs/building.md](docs/building.md) walks through it, including the lookup order for the library and the one trap (a stale copy in `python/nodusdb/bin/`).

```sh
GRAALVM_HOME=/path/to/graalvm-community mvn -Pnative -pl core package -DskipTests
pip install pytest pyarrow
python -m pytest python/tests -ra
```

If you only want to try the Python side, `pip install nodusdb` gives you the published wheel, and `NODUSDB_LIBRARY` points the source tree at any copy of the library.

## The shape of the code

The Java packages are listed in [docs/architecture.md](docs/architecture.md), with the byte layouts. In short:

| Path | What it holds |
|---|---|
| `core/src/main/java/io/nodusdb/kernel` | The graph engine: native memory, adjacency, traversal, the seqlock, the reachability closure |
| `.../log`, `.../storage` | The write-ahead log, snapshots, recovery, the directory format and the upgrade |
| `.../authz` | The schema language, tuples, tokens and the check evaluator |
| `.../objectstore`, `.../chain`, `.../ship`, `.../replica`, `.../erasure`, `.../watch` | Object storage, the signed chain, shipping, followers, erasure and the change feed |
| `.../iceberg`, `.../avro`, `.../projection` | The Iceberg v2 projection of the log |
| `.../lake` | The lake write path and the Parquet writer |
| `.../capi` | The C interface the native library exports |
| `python/nodusdb` | The Python wrapper |
| `bench` | JMH microbenchmarks. `bench/baseline` holds recorded results |
| `python/benchmarks` | End-to-end comparisons against other engines |
| `docs` | Guides, architecture, and the decision log |

## What the code looks like

These rules are the reason the engine is fast and the reason it is readable. A change that breaks one should say why.

**On the hot path, allocate nothing.** Queries, edge inserts and deletes, and lake upserts must not call `new`, box a number, use a `java.util` collection, use a stream or build a string. Allocation belongs in constructors and in growth methods, and those are marked. A test reads the thread's allocated-bytes counter across a primed window and requires zero, and JMH runs with `-prof gc`.

**Touch native memory only through `NativeMemory`.** In the native library a `MemorySegment` accessor such as `getAtIndex` costs about 300 ns, against under 2 ns on the JVM, so a direct call is fast in every Java test and slow for every user. `AccessorGuardTest` fails the build on one. Pass the address and size the owner caches (see `NativeColumn`), and in a loop over many values copy a chunk into a heap array with `MemorySegment.copy` first. [architecture.md](docs/architecture.md#reading-and-writing-native-memory) explains why.

**Primitives and arrays.** Use `int` for degrees, positions and offsets, and `long` for node ids. Keep data in parallel primitive arrays or native segments, in place of arrays of objects. Use power-of-two sizes with masks, and never `/` or `%` on a hot path.

**No tombstones.** Deletion is swap-and-pop plus backward-shift in the index. A tombstone in a hash table slows every later probe.

**Failures are values on the hot path.** No logging, string formatting or exceptions in a loop. Contract violations throw. The C interface catches everything and returns a code, so an exception never crosses into the host process.

**No comments in the source.** The Java and Python files carry none. The design, the layouts and the reasons live in `docs/`, where they can be reviewed as prose. If code needs a comment to be understood, give the thing a better name or add a paragraph to [architecture.md](docs/architecture.md). Docstrings on public Python API are the exception.

**Explain a decision in the decision log.** When you settle a question that someone could reasonably have settled the other way, add a row to [docs/decisions.md](docs/decisions.md): the topic, what you chose, the evidence and who decided. Use the next free number.

## Tests

Every change needs tests, and the tests in this repository are strict about the edges.

- **Every branch in a hot method gets a case.** Empty, one element, exactly 16 (the promotion boundary), deleting the last element, deleting the only element, a duplicate add, and deleting an absent key.
- **Data structures get a differential test.** Run a long seeded sequence of random operations against both the structure and a plain `HashSet` or `HashMap`, compare after every operation, and call the structure's invariant check. Put the seed in `@ValueSource`, and put `seed=` and `op=` in every assertion message so a failure can be replayed by pinning that seed. The hash table also has a mode that draws keys from a colliding set, because random keys rarely exercise backward shift.
- **Mutation-check new tests.** Break the code the test is meant to guard and watch it fail. A test that passes either way proves nothing. Several tests in the repository say in the decision log that they were mutation-checked.
- **Crash behaviour goes through the simulator.** `CrashExplorationTest` replays every prefix of a run's writes and forces through a simulated disk. New durable formats should join it.
- **Never keep a thread idle with a long interval in a test.** Build the state by construction, or wait on an observable condition with a deadline.
- **Python tests live in `python/tests`.** They run against the real native library. A test that needs `pyarrow` skips without it, and `NODUSDB_REQUIRE_PYARROW=1` makes the skip a failure, as CI does.

### The slow-machine gate

CI runs the Java tests with three or four cores and a slow disk. A 16-core laptop hides tests that assume a background thread is idle or has finished. Before you push a change that touches threads, shipping, followers, the change feed or any test that waits, run the suite pinned to two cores with busy loops on them. On Windows:

```powershell
$env:JAVA_HOME = "C:\path\to\graalvm-jdk-25"
powershell -File scripts\contention.ps1 "mvn -B -ntp -pl core test"
powershell -File scripts\contention.ps1 "python -m pytest python/tests -q" -Cores 2 -Spinners 4
```

On Linux or macOS, pin the test process to two cores with `taskset` or an equivalent and run a few busy loops on the same cores. `scripts/contention.ps1` shows the shape.

## Performance changes

The project does not make a performance claim without a measurement.

- Run the relevant JMH benchmark before and after, on the same machine, with `-prof gc`. A run with `-f 1` is a smoke test, not a baseline. Record the machine.
- A regression of more than 10% against the recorded baseline in `bench/baseline` fails review.
- Cite the result in the pull request and in the doc or comment that makes the claim. If you can only say "I expect", write "expected".
- Numbers from your laptop are not comparable with numbers from `windows-dev`. Compare two runs on your own machine.

```sh
mvn -q -pl bench -am package -DskipTests
java -jar bench/target/benchmarks.jar IntersectionBench -wi 5 -i 10 -f 2 -prof gc
```

The end-to-end comparison against NetworkX and SQLite is `python python/benchmarks/compare.py`. Run it with the library the wheels carry, not a development build, or you will measure the toolchain. A local build with GraalVM CE 23 was about 130 times slower than CE 22 on `add_edge` because memory-segment access was not optimized. Time a new toolchain before you trust it.

## Making a change

1. Fork the repository and create a branch from `main`. Name it for what it does: `fix/torn-tail-off-by-one`, `feat/instance-profile-credentials`.
2. Make the change with its tests. Keep the diff to one idea.
3. Run `mvn test` and the Python suite. Run the slow-machine gate if the change touches threads or anything that waits.
4. Commit with a message in the form `type(scope): summary`, where the type is `feat`, `fix`, `perf`, `test`, `docs`, `refactor`, `build`, `ci`, `bench` or `chore`. Say what changed and why in the body when it is not obvious. Look at `git log` for the style.
5. Open a pull request. Describe the problem, what you changed, how you tested it and any measurement. CI builds the native library on Ubuntu, macOS (arm64) and Windows, and runs both suites on each.
6. Expect review comments on correctness first, then names, then style. We ask for changes to be pushed as new commits while the review is open, and never force-pushed over it.

Changes to a storage format (the log, the snapshot, the chain, the Iceberg projection) need extra care. The 0.1.x promise is that a directory written by any 0.1.x release opens in every later one, and the golden directory under `core/src/test/resources` pins it. If your change needs a format change, say so in the issue before you write it.

## Reporting a problem

Open an issue with:

- The output of `python -c "import nodusdb; print(nodusdb.__version__)"` and your platform.
- A script that reproduces it, as short as you can make it. For a durable graph, say whether `sync_mode` was `sync` or `async`.
- What you expected and what happened, with the full traceback.

If you think you have found a way to read or corrupt another user's data, or a bypass of the signed chain, do not put the details in a public issue. Open an issue that says you have a security report and nothing else, and a maintainer will arrange a private channel.

## License

NodusDB is licensed under the [Apache License, Version 2.0](LICENSE). By submitting a contribution you agree that it is licensed under the same terms.
