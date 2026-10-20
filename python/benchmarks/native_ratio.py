"""Fail when the native library is much slower than the same code on the JVM.

Two probes run the same Java code once inside the JVM and once inside the native library, so no foreign-call
overhead is timed. The graph probe adds and looks up a chain of edges through a graph session. The lake probe
loads 250,000 rows through the columnar path, sums a column in memory and flushes the buffer to Parquet.

Before the native-memory fix, every MemorySegment read or write in the native library cost about 300 ns
against under 2 ns on the JVM. The graph ratio was 5.8 to 7.7 for add_edge and 12.5 to 14.2 for has_edge in
CI, and the lake sum ran about 350 times slower than on the JVM. With NativeMemory the laptop measured 2.5 to 2.7
for add_edge, 1.8 to 2.2 for has_edge, 2.2 to 2.3 for lake ingest, 1.2 for the lake sum and 2.3 to 2.4 for
the lake flush. The limit of 10 leaves about 3.7 times headroom over the worst of those and fails any change
that brings back per-element segment access, which costs ten times or more."""

import argparse
import ctypes
import os
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import _native  # noqa: E402

DEFAULT_OPERATIONS = 200_000
DEFAULT_RUNS = 5
DEFAULT_WARMUPS = 2
DEFAULT_LIMIT = 10.0
DEFAULT_LAKE_ROWS = 250_000
DEFAULT_LAKE_LIMIT = 10.0
PROBE_CLASS = "io.nodusdb.capi.PerformanceProbe"
LAKE_PROBE_CLASS = "io.nodusdb.capi.LakeProbe"
LAKE_METRICS = ("lake_ingest", "lake_sum", "lake_flush")


def native_nanoseconds_per_operation(operations, runs, warmups, library_path):
    library, isolate = _native.load(library_path)
    probe = library.nodus_probe
    probe.restype = ctypes.c_int
    probe.argtypes = [_native.THREAD, ctypes.c_int, ctypes.POINTER(ctypes.c_int64)]
    thread = _native.current_thread(library, isolate)
    adds, lookups = [], []
    for run in range(warmups + runs):
        out = (ctypes.c_int64 * 2)()
        code = probe(thread, operations, out)
        if code != 0:
            raise SystemExit(f"nodus_probe failed with code {code}")
        if run >= warmups:
            adds.append(out[0] / operations)
            lookups.append(out[1] / operations)
    return statistics.median(adds), statistics.median(lookups)


def native_lake_nanoseconds_per_row(rows, runs, warmups, library_path):
    library, isolate = _native.load(library_path)
    probe = getattr(library, "nodus_lake_probe", None)
    if probe is None:
        raise SystemExit("this libnodusdb has no nodus_lake_probe; rebuild the native library")
    probe.restype = ctypes.c_int
    probe.argtypes = [_native.THREAD, ctypes.c_int, ctypes.POINTER(ctypes.c_int64)]
    thread = _native.current_thread(library, isolate)
    samples = [[], [], []]
    for run in range(warmups + runs):
        out = (ctypes.c_int64 * 3)()
        code = probe(thread, rows, out)
        if code != 0:
            raise SystemExit(f"nodus_lake_probe failed with code {code}: {_native.last_error_message(library, thread)}")
        if run >= warmups:
            for index in range(3):
                samples[index].append(out[index] / rows)
    return tuple(statistics.median(values) for values in samples)


def jvm_lake_nanoseconds_per_row(rows, runs, warmups, classes):
    java = shutil.which("java") or str(Path(os.environ["JAVA_HOME"]) / "bin" / "java")
    command = [java, "-cp", str(classes), LAKE_PROBE_CLASS, str(rows), str(runs), str(warmups)]
    output = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    values = dict(line.split() for line in output.strip().splitlines())
    return tuple(float(values[f"{name}_ns_per_row"]) for name in LAKE_METRICS)


def jvm_nanoseconds_per_operation(operations, runs, warmups, classes):
    java = shutil.which("java") or str(Path(os.environ["JAVA_HOME"]) / "bin" / "java")
    command = [java, "-cp", str(classes), PROBE_CLASS, str(operations), str(runs), str(warmups)]
    output = subprocess.run(command, check=True, capture_output=True, text=True).stdout
    values = dict(line.split() for line in output.strip().splitlines())
    return float(values["add_edge_ns_per_op"]), float(values["has_edge_ns_per_op"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classes", type=Path, default=Path("core/target/classes"))
    parser.add_argument("--library", default=None)
    parser.add_argument("--operations", type=int, default=DEFAULT_OPERATIONS)
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    parser.add_argument("--limit", type=float, default=DEFAULT_LIMIT)
    parser.add_argument("--lake-rows", type=int, default=DEFAULT_LAKE_ROWS)
    parser.add_argument("--lake-limit", type=float, default=DEFAULT_LAKE_LIMIT)
    arguments = parser.parse_args()

    native = native_nanoseconds_per_operation(arguments.operations, arguments.runs, arguments.warmups,
                                              arguments.library)
    jvm = jvm_nanoseconds_per_operation(arguments.operations, arguments.runs, arguments.warmups, arguments.classes)

    failed = False
    for name, native_ns, jvm_ns in (("add_edge", native[0], jvm[0]), ("has_edge", native[1], jvm[1])):
        ratio = native_ns / jvm_ns if jvm_ns else float("inf")
        verdict = "ok" if ratio <= arguments.limit else "FAIL"
        failed |= ratio > arguments.limit
        print(f"{name}: native {native_ns:,.0f} ns/op, jvm {jvm_ns:,.0f} ns/op, ratio {ratio:.2f} "
              f"(limit {arguments.limit:g}) {verdict}")
    lake_native = native_lake_nanoseconds_per_row(arguments.lake_rows, arguments.runs, arguments.warmups,
                                                  arguments.library)
    lake_jvm = jvm_lake_nanoseconds_per_row(arguments.lake_rows, arguments.runs, arguments.warmups, arguments.classes)
    for name, native_ns, jvm_ns in zip(LAKE_METRICS, lake_native, lake_jvm):
        ratio = native_ns / jvm_ns if jvm_ns else float("inf")
        verdict = "ok" if ratio <= arguments.lake_limit else "FAIL"
        failed |= ratio > arguments.lake_limit
        print(f"{name}: native {native_ns:,.1f} ns/row, jvm {jvm_ns:,.1f} ns/row, ratio {ratio:.2f} "
              f"(limit {arguments.lake_limit:g}) {verdict}")
    if failed:
        raise SystemExit("the native library is too slow compared with the JVM; check the GraalVM version")


if __name__ == "__main__":
    main()
