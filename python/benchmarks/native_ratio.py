"""Fail when the native library is much slower than the same code on the JVM.

The probe adds and looks up a chain of edges through a graph session. It runs once inside the JVM and once
inside the native library, so both sides execute the same Java code and no foreign-call overhead is timed.

Ahead-of-time code is slower than JIT-compiled code for off-heap memory access. On a laptop with GraalVM CE
22.0.2 the ratio measured 9.5 for add_edge and 16.9 for has_edge, and the CE 23 library, which does not
optimize memory-segment access, ran add_edge about 130 times slower than that build. The default limit of 50
passes the first and fails the second. Lower it with --limit once a CI toolchain has a recorded baseline."""

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
DEFAULT_LIMIT = 50.0
PROBE_CLASS = "io.nodusdb.capi.PerformanceProbe"


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
    if failed:
        raise SystemExit("the native library is too slow compared with the JVM; check the GraalVM version")


if __name__ == "__main__":
    main()
