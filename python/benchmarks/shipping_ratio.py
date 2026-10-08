"""Fail when shipping makes local writes much slower than the same writes without it.

Both graphs live in this process and take the same writes in the same run, so the ratio holds on any machine.
The shipper works on its own thread and only the log append touches shipping state, so a local write should cost
about what it did before. A write that waited for the object store would cost milliseconds and push the ratio far
past the limit. The lake write time is printed for the record and is not part of the gate."""

import argparse
import shutil
import statistics
import sys
import tempfile
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph, Shipping, Transaction, generate_signing_key  # noqa: E402

DEFAULT_OPERATIONS = 20_000
DEFAULT_RUNS = 5
DEFAULT_WARMUPS = 1
DEFAULT_LIMIT = 3.0
DEFAULT_LAKE_WRITES = 20

SCHEMA = """
schema 1
type user
type document {
  relation viewer: user
  permission view = viewer
}
"""


def add_tuples(graph, operations, offset):
    started = time.perf_counter()
    for index in range(offset, offset + operations):
        graph.add_tuple(f"document:d{index}", "viewer", "user:alice")
    return (time.perf_counter() - started) * 1e9 / operations


def median_nanoseconds(graph, operations, runs, warmups):
    samples = []
    for run in range(warmups + runs):
        elapsed = add_tuples(graph, operations, run * operations)
        if run >= warmups:
            samples.append(elapsed)
    return statistics.median(samples)


def lake_milliseconds(graph, writes):
    samples = []
    for index in range(writes):
        started = time.perf_counter()
        graph.write(Transaction().add(f"document:lake{index}", "viewer", "user:alice"), durability="lake")
        samples.append((time.perf_counter() - started) * 1e3)
    return statistics.median(samples)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", default=None)
    parser.add_argument("--operations", type=int, default=DEFAULT_OPERATIONS)
    parser.add_argument("--runs", type=int, default=DEFAULT_RUNS)
    parser.add_argument("--warmups", type=int, default=DEFAULT_WARMUPS)
    parser.add_argument("--limit", type=float, default=DEFAULT_LIMIT)
    parser.add_argument("--lake-writes", type=int, default=DEFAULT_LAKE_WRITES)
    arguments = parser.parse_args()

    root = Path(tempfile.mkdtemp(prefix="nodus-ship-ratio-"))
    try:
        private_key, public_key = root / "signing.pem", root / "signing.pub"
        generate_signing_key(private_key, public_key, arguments.library)
        shipping = Shipping.directory(root / "bucket", key_file=private_key, key_id=1, public_key_file=public_key)
        with Graph(arguments.library, path=root / "off", sync_mode="async") as plain, \
                Graph(arguments.library, path=root / "on", sync_mode="async", shipping=shipping) as shipped:
            plain.apply_schema(SCHEMA)
            shipped.apply_schema(SCHEMA)
            off = median_nanoseconds(plain, arguments.operations, arguments.runs, arguments.warmups)
            on = median_nanoseconds(shipped, arguments.operations, arguments.runs, arguments.warmups)
            lake = lake_milliseconds(shipped, arguments.lake_writes)
    finally:
        shutil.rmtree(root, ignore_errors=True)

    ratio = on / off if off else float("inf")
    verdict = "ok" if ratio <= arguments.limit else "FAIL"
    print(f"add_tuple: shipping off {off:,.0f} ns/op, on {on:,.0f} ns/op, ratio {ratio:.2f} "
          f"(limit {arguments.limit:g}) {verdict}")
    print(f"lake write: median {lake:,.1f} ms over {arguments.lake_writes} writes (not gated)")
    if ratio > arguments.limit:
        raise SystemExit("shipping slows local writes too much; look for work done on the commit path")


if __name__ == "__main__":
    main()
