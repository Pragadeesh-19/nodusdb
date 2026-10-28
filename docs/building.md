# Building and testing

How to build NodusDB from source, run every test suite, and reproduce the benchmarks. If you only want to use it, `pip install nodusdb` is enough. See the [README](../README.md).

## What you need

- **JDK 22 and Maven 3.9 or newer.** The Java sources compile for release 22, and the Java tests run on any JDK 22 or newer.
- **Python 3.10 or newer.** The tests ran on 3.12.
- **GraalVM Community Edition with `native-image`**, for the native library only. CI and the wheels use 25.0.2 (25.0.1 for the macOS Intel wheel, because 25.0.2 has no macOS Intel build). 22.0.2 also works.
- **On Windows, Visual Studio 2022 with the C++ build tools.**

The toolchain changes the speed of the library. On the development laptop 25.0.2 and 22.0.2 agree within 5%, and 25.0.1 is up to 10% slower on a lookup. Oracle GraalVM 25 took about a third less time per call on the same code, but its free license allows redistributing the compiled library only without a fee for products that bundle it, so the wheels, which are Apache-2.0, use the Community Edition. A local build with GraalVM CE 23 ran `add_edge` in about 1.2 ms where CE 22.0.2 took 9 microseconds, because memory-segment access was not optimized. Check a new toolchain with a timing before you trust it.

## The commands

```sh
# Java tests. The Parquet, Avro and Iceberg tests judge their output with real readers:
# pip install "pyiceberg[pyarrow]" fastavro pyarrow tzdata
# Set NODUS_PYTHON to that interpreter if it is not `python` on the path. Without the packages those
# tests skip, and NODUS_REQUIRE_PYTHON_VERIFIERS=1 turns the skip into a failure, as CI does.
mvn test

# Native library, written to target/native/
GRAALVM_HOME=/path/to/graalvm-community mvn -Pnative -pl core package -DskipTests

# Python tests, which load the native library
python -m unittest discover -s python/tests -v

# The same tests under pytest, as CI runs them. Needs: pip install pytest pyarrow
python -m pytest python/tests -ra

# Wheel for the current platform, written to the current directory
python -m pip wheel python/ --no-deps -w dist
```

The native build is opt-in, so `mvn test` never needs GraalVM. It runs `native/build-native.cmd` (or `.sh`), which calls `native-image --shared` and writes `libnodusdb` and its headers to `target/native/`.

## Where Python finds the library

The loader looks in three places, in order:

1. The path in the `NODUSDB_LIBRARY` environment variable.
2. `python/nodusdb/bin/<os>-<arch>/`.
3. `target/native/`.

CI copies the library into `bin/`. If you rebuild `target/native/` by hand, copy it into `bin/` too, or the loader keeps using the old copy. The copy is git-ignored.

To test the library a wheel carries, install the wheel into a virtual environment and point `NODUSDB_LIBRARY` at the file under `site-packages/nodusdb/bin/`.

## Tests on a slow machine

CI runs the Java tests on Oracle GraalVM 25 with three to four cores and a slow disk; only the native library uses the Community Edition. A test that passes on a 16-core machine can fail there when it assumes a background thread is idle, has finished, or has not started. Before you push a change that touches threads, shipping, followers or any test that waits, run the suite pinned to two cores with CPU spinners on them (Windows):

```powershell
$env:JAVA_HOME = "C:\path\to\graalvm-jdk-25"
powershell -File scripts\contention.ps1 "mvn -B -ntp -pl core test"
powershell -File scripts\contention.ps1 "python -m pytest python/tests -q" -Cores 2 -Spinners 4
```

## What the tests cover

On the development laptop (Windows, JDK 22, a native library built by GraalVM CE 22.0.2) the Java suite ran 630 tests and the Python suite 120, with `pyarrow` installed so that none skipped. CI runs both on Ubuntu, macOS (arm64) and Windows for every push.

- **The log.** A crash simulator replays every prefix of the writes and forces of a run, with torn writes (every byte for writes up to 64 bytes in the synchronous run, every 13th byte otherwise), and requires recovery to return a prefix of what was acknowledged. Two more runs set the flush and mark intervals to 1 ms so that the forced mark moves while the workload runs; with the default 100 ms it would stay at the start of the log. Corruption below the forced mark and a failed fsync have their own tests.
- **Durability on the graph.** Clean restarts, crashes in both sync modes, torn log tails, snapshot rolls, a crash between the snapshot rename and the deletion of the covered segments, corrupted and truncated snapshots, the directory lock and the memory limit.
- **Upgrade.** The version 1 corpus (integer, string and snapshot-only directories) upgrades with equal edge fingerprints, and the upgrade is crashed after every step and finished by running it again.
- **Compatibility.** A golden version 2 graph directory in the test suite is opened with the current code on every build.
- **Permissions.** A seeded differential test compares the iterative evaluator with a naive recursive one over random tuples on ten seeds. Disabling arrow traversal makes four of them fail.
- **Concurrency.** Readers run against a busy writer on one kernel, one handle and one Python object, and a reader is not blocked while a write waits for its fsync.
- **Shipping, followers, erasure, watch and time travel.** A deterministic simulation harness drives the shipper, the verifier and the projector with a faulty store and a manual clock. Erasure tests check byte for byte that no file in the directory, the bucket or the Iceberg table still holds the erased name.
- **Lake.** The Parquet output is checked against PyArrow bit for bit on 100,000 mixed rows, and the Snappy codec, dictionary encoding, row groups and statistics have Java tests that need no PyArrow.
- **The native performance gate.** It compares the same Java code on the JVM and in the native library (`python/benchmarks/native_ratio.py`). In CI the ratio is 5.8 to 7.7 for `add_edge` and 12.5 to 14.2 for `has_edge`, and the gate fails above 30.
- **The examples.** `python/tests/test_examples.py` runs every script in `examples/`.

## Benchmarks

```sh
# JMH microbenchmarks
mvn -pl bench -am package -DskipTests
java -jar bench/target/benchmarks.jar -prof gc

# Comparison against NetworkX and SQLite (several minutes)
python python/benchmarks/compare.py

# Shipping overhead: add_tuple with and without shipping, one run, fails above a ratio of 3
python python/benchmarks/shipping_ratio.py

# Multi-engine suites on real data. Install the competitors first.
# The first run downloads the Pokec archive (132 MB) and the taxi file into
# the system temp directory, or into NODUS_BENCH_CACHE if it is set.
pip install -r python/benchmarks/requirements.txt
python python/benchmarks/benchmark_graph_heavyweights.py run --out bench/baseline/graph-heavyweights-windows-dev.json
python python/benchmarks/benchmark_lake_heavyweights.py run --out bench/baseline/lake-heavyweights-windows-dev-v2.json
```

The graph suite takes about 2.5 hours and the lake suite about 20 minutes. Always pass `-prof gc` to JMH: any allocation in a hot benchmark is a regression, even if throughput looks fine. [Benchmarks](benchmarks.md) explains how to read the results.

## Release wheels

Release wheels come from `.github/workflows/wheels.yml`. It builds the native library with GraalVM Community Edition on Linux (x86_64 and arm64), macOS (x86_64 and arm64) and Windows, and then runs cibuildwheel once per platform.

The native library is the only compiled part of the package and Python loads it through `ctypes`, so each wheel is tagged `py3-none-<platform>` and works on every CPython from 3.10 on. Every wheel is installed and smoke-tested on Python 3.10 to 3.14 on its own platform, and the smoke test calls each feature family and checks the license files and `__version__`. A separate step points `Shipping.s3` at `https://s3.amazonaws.com` with a fake key from the installed wheel and requires the S3 authentication error, which shows that the library trusts public certificates.

The workflow runs on every push. It publishes to PyPI only when a release is published, through trusted publishing and a required reviewer on the `pypi` environment. Before it uploads, `scripts/check_release.py` requires the release tag to be `v` plus the package version, the release's pre-release flag to match the version suffix, and the five expected wheels to be present. A verify job then installs the published version from PyPI and runs the smoke test on five platforms and on Python 3.10 and 3.14.

Linux wheels are tagged `manylinux_2_34`, so they need glibc 2.34 or newer: Ubuntu 22.04 and later, Debian 12, and RHEL 9. The manylinux2014 build container cannot install a 2.34 wheel, so the install step runs on the host runner. macOS wheels target 11.0 and later. Windows wheels are x86_64 only.
