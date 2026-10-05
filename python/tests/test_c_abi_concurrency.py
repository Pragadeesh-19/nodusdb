import ctypes
import random
import sys
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph  # noqa: E402

TARGETS = 500
SHARED = TARGETS + 1
ROUNDS = 150
CAPACITY = 4096


def _hub_graph():
    graph = Graph()
    for target in range(1, TARGETS + 1):
        graph.add_edge(0, target)
        graph.add_edge(SHARED, target)
    return graph


def _raw_call(graph, name, *arguments):
    """Call a C export directly, bypassing the Python lock on Graph."""
    return getattr(graph._lib, name)(graph._thread, graph._handle, *arguments)


def _read(buffer, total):
    return [buffer[i] for i in range(min(total, CAPACITY))]


class CAbiConcurrencyTest(unittest.TestCase):
    """Concurrent C-ABI calls on one handle must not return another call's results."""

    def test_concurrent_queries_and_writes_on_one_handle(self):
        graph = _hub_graph()
        errors = []
        writing = threading.Event()
        writing.set()

        def writer():
            try:
                rng = random.Random(11)
                present = set(range(1, TARGETS + 1))
                for _ in range(ROUNDS * 4):
                    target = rng.randint(1, TARGETS)
                    name = "nodus_remove_edge" if target in present else "nodus_add_edge"
                    if not _raw_call(graph, name, 0, target):
                        raise AssertionError(f"{name} failed for edge 0->{target}")
                    present.symmetric_difference_update({target})
            except Exception as error:
                errors.append(error)
            finally:
                writing.clear()

        def reader(name, arguments):
            try:
                buffer = (ctypes.c_int64 * CAPACITY)()
                while writing.is_set():
                    total = _raw_call(graph, name, *arguments, buffer, CAPACITY)
                    if total < 0:
                        raise AssertionError(f"{name} returned an error")
                    values = _read(buffer, total)
                    if len(values) != len(set(values)):
                        raise AssertionError(f"{name} returned a duplicate: {values}")
                    if any(not 1 <= value <= TARGETS for value in values):
                        raise AssertionError(f"{name} returned an out-of-range target")
            except Exception as error:
                errors.append(error)

        workers = [threading.Thread(target=writer)]
        for _ in range(3):
            workers.append(threading.Thread(target=reader, args=("nodus_khop", (0, 1))))
            workers.append(threading.Thread(target=reader, args=("nodus_common_neighbors", (0, SHARED))))
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=120)
        self.assertFalse(any(worker.is_alive() for worker in workers), "a thread did not finish")
        self.assertEqual([], errors)
        graph.close()

    def test_concurrent_batches_on_disjoint_ranges_leave_the_exact_state(self):
        graph = Graph()
        errors = []
        per_writer = 400

        def batch_writer(base):
            try:
                sources = [0] * per_writer
                targets = [base + i for i in range(per_writer)]
                u_column = (ctypes.c_int64 * per_writer)(*sources)
                v_column = (ctypes.c_int64 * per_writer)(*targets)
                if _raw_call(graph, "nodus_add_edges_batch", u_column, v_column, per_writer) != per_writer:
                    raise AssertionError(f"batch add from base {base} did not apply every edge")
                drop = targets[::2]
                d_u = (ctypes.c_int64 * len(drop))(*([0] * len(drop)))
                d_v = (ctypes.c_int64 * len(drop))(*drop)
                if _raw_call(graph, "nodus_remove_edges_batch", d_u, d_v, len(drop)) != len(drop):
                    raise AssertionError(f"batch remove from base {base} did not apply every edge")
            except Exception as error:
                errors.append(error)

        bases = [1_000_000 * (w + 1) for w in range(4)]
        workers = [threading.Thread(target=batch_writer, args=(base,)) for base in bases]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=120)
        self.assertEqual([], errors)
        self.assertEqual(len(bases) * (per_writer // 2), graph.degree(0))
        for base in bases:
            for i in range(per_writer):
                self.assertEqual(i % 2 == 1, graph.has_edge(0, base + i), f"base {base} edge {i}")
        graph.close()


if __name__ == "__main__":
    unittest.main()
