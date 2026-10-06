import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph, NodusError, NodusMemoryError, _native  # noqa: E402

MEMORY_LIMIT = -3
CHAIN_BOUND = 2_000_000


def _native_has_memory_limit():
    try:
        library, _ = _native.load()
    except NodusError:
        return False
    return hasattr(library, "nodus_create_limited")


HAS_MEMORY_LIMIT = _native_has_memory_limit()


def fill_chain(graph, first=0):
    """Add edges (n, n + 1) until the limit rejects one, and return that n."""
    for node in range(first, first + CHAIN_BOUND):
        try:
            graph.add_edge(node, node + 1)
        except NodusMemoryError:
            return node
    raise AssertionError("the memory limit was never reached")


class MemoryLimitArgumentTest(unittest.TestCase):
    def test_limit_must_be_a_positive_integer_or_none(self):
        for bad in (0, -1, 2**60):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                Graph(max_memory_mb=bad)
        for bad in (1.5, "8", True, [8]):
            with self.subTest(bad=bad), self.assertRaises(TypeError):
                Graph(max_memory_mb=bad)

    def test_the_error_is_a_nodus_error_and_a_memory_error(self):
        self.assertTrue(issubclass(NodusMemoryError, NodusError))
        self.assertTrue(issubclass(NodusMemoryError, MemoryError))


@unittest.skipUnless(HAS_MEMORY_LIMIT, "libnodusdb lacks the memory limit; rebuild the native library")
class MemoryLimitTest(unittest.TestCase):
    def test_without_a_limit_a_large_graph_is_accepted(self):
        with Graph() as graph:
            graph.add_edges_from([(i, i + 1) for i in range(100_000)])
            self.assertTrue(graph.has_edge(99_999, 100_000))

    def test_writes_past_the_limit_raise_and_earlier_edges_stay_queryable(self):
        with Graph(max_memory_mb=1) as graph:
            rejected = fill_chain(graph)

            self.assertGreater(rejected, 100)
            self.assertFalse(graph.has_edge(rejected, rejected + 1))
            self.assertEqual(0, graph.degree(rejected))
            for node in range(0, rejected, max(1, rejected // 200)):
                self.assertTrue(graph.has_edge(node, node + 1))
            self.assertEqual([1], graph.khop(0, 1))
            self.assertEqual([2], graph.khop(1, 1))
            self.assertEqual(1, graph.in_degree(1))

    def test_a_rejected_write_does_not_stop_later_writes_that_need_no_new_storage(self):
        with Graph(max_memory_mb=1) as graph:
            rejected = fill_chain(graph)

            self.assertTrue(graph.add_edge(0, 5))
            self.assertFalse(graph.add_edge(0, 5))
            self.assertEqual(2, graph.degree(0))
            self.assertGreater(rejected, 10)

    def test_removing_edges_always_works_and_frees_room_to_add_them_back(self):
        with Graph(max_memory_mb=1) as graph:
            rejected = fill_chain(graph)

            for node in range(rejected):
                self.assertTrue(graph.remove_edge(node, node + 1))
            for node in range(rejected):
                self.assertTrue(graph.add_edge(node, node + 1))
            self.assertEqual(1, graph.degree(0))

    def test_a_huge_node_id_is_rejected_cleanly(self):
        with Graph(max_memory_mb=4) as graph:
            graph.add_edge(1, 2)

            with self.assertRaises(NodusMemoryError):
                graph.add_edge(0, 2**31 - 20)

            self.assertTrue(graph.has_edge(1, 2))
            self.assertTrue(graph.add_edge(2, 3))

    def test_a_batch_stops_at_the_limit_and_keeps_what_it_applied(self):
        edges = [(i % 400, 400 + (i // 400) % 400) for i in range(150_000)]
        with Graph(max_memory_mb=1) as graph:
            with self.assertRaises(NodusMemoryError) as raised:
                graph.add_edges_from(edges)

            applied = 0
            while applied < len(edges) and graph.has_edge(*edges[applied]):
                applied += 1
            self.assertGreater(applied, 0)
            self.assertLess(applied, len(edges))
            self.assertIn("batch", str(raised.exception))
            for source, target in edges[applied:applied + 500]:
                self.assertFalse(graph.has_edge(source, target))

    def test_string_keyed_graphs_honor_the_limit(self):
        with Graph(max_memory_mb=1) as graph:
            graph.add_edges_from([("user:0", "user:1"), ("user:1", "user:2")])

            with self.assertRaises(NodusMemoryError):
                graph.add_edges_from([(f"user:{i}", f"user:{i + 1}") for i in range(2, 200_000)])

            self.assertTrue(graph.has_edge("user:0", "user:1"))
            self.assertFalse(graph.has_edge("user:2", "user:3"))
            self.assertEqual(["user:1"], graph.khop("user:0", 1))

    def test_the_raw_abi_reports_the_limit_with_its_own_code(self):
        with Graph(max_memory_mb=1) as graph:
            lib, thread, handle = graph._lib, graph._thread, graph._handle
            rejected = fill_chain(graph)

            self.assertEqual(MEMORY_LIMIT, lib.nodus_add_edge(thread, handle, rejected, rejected + 1))
            self.assertEqual(0, lib.nodus_add_edge(thread, handle, 0, 1))
            self.assertEqual(1, lib.nodus_add_edge(thread, handle, 0, 5))


@unittest.skipUnless(HAS_MEMORY_LIMIT, "libnodusdb lacks the memory limit; rebuild the native library")
class DurableMemoryLimitTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="nodus-limit-")

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def test_rejected_edges_do_not_come_back_after_reopen(self):
        with Graph(path=self.directory, max_memory_mb=1) as graph:
            rejected = fill_chain(graph)

        with Graph(path=self.directory) as reopened:
            self.assertTrue(reopened.has_edge(rejected - 1, rejected))
            self.assertFalse(reopened.has_edge(rejected, rejected + 1))
            self.assertEqual(1, reopened.degree(0))

    def test_a_stored_graph_that_does_not_fit_fails_to_open_and_can_be_reopened_without_a_limit(self):
        with Graph(path=self.directory) as graph:
            graph.add_edges_from([(i, i + 1) for i in range(60_000)])

        with self.assertRaises(NodusMemoryError):
            Graph(path=self.directory, max_memory_mb=1)

        with Graph(path=self.directory) as reopened:
            self.assertTrue(reopened.has_edge(59_999, 60_000))

    def test_a_stored_graph_opens_under_a_limit_that_fits_it(self):
        with Graph(path=self.directory) as graph:
            graph.add_edges_from([(i, i + 1) for i in range(1_000)])

        with Graph(path=self.directory, max_memory_mb=64) as reopened:
            self.assertTrue(reopened.has_edge(999, 1_000))


if __name__ == "__main__":
    unittest.main()
