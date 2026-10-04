import array
import ctypes
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph, NodusError  # noqa: E402


class GraphTest(unittest.TestCase):
    def setUp(self):
        self.graph = Graph(result_capacity=8)

    def tearDown(self):
        self.graph.close()

    def test_add_has_remove_round_trip(self):
        self.assertTrue(self.graph.add_edge(1, 2))
        self.assertTrue(self.graph.has_edge(1, 2))
        self.assertFalse(self.graph.has_edge(2, 1))
        self.assertFalse(self.graph.add_edge(1, 2))
        self.assertTrue(self.graph.remove_edge(1, 2))
        self.assertFalse(self.graph.has_edge(1, 2))
        self.assertFalse(self.graph.remove_edge(1, 2))

    def test_degrees_track_direction(self):
        self.graph.add_edge(0, 1)
        self.graph.add_edge(0, 2)
        self.graph.add_edge(3, 1)

        self.assertEqual(2, self.graph.degree(0))
        self.assertEqual(0, self.graph.degree(1))
        self.assertEqual(2, self.graph.in_degree(1))
        self.assertEqual(0, self.graph.in_degree(0))

    def test_unknown_nodes_have_no_edges(self):
        self.assertFalse(self.graph.has_edge(900_000, 1))
        self.assertEqual(0, self.graph.degree(900_000))
        self.assertEqual([], self.graph.khop(900_000, 3))

    def test_common_neighbors_returns_intersection(self):
        for v in (1, 2, 3):
            self.graph.add_edge(0, v)
        for v in (2, 3, 4):
            self.graph.add_edge(9, v)

        self.assertEqual({2, 3}, set(self.graph.common_neighbors(0, 9)))

    def test_khop_respects_depth_and_excludes_start(self):
        for u in range(5):
            self.graph.add_edge(u, u + 1)

        self.assertEqual({1}, set(self.graph.khop(0, 1)))
        self.assertEqual({1, 2, 3}, set(self.graph.khop(0, 3)))
        self.assertEqual([], self.graph.khop(0, 0))

    def test_khop_on_cycle_does_not_repeat_nodes(self):
        for u in range(4):
            self.graph.add_edge(u, (u + 1) % 4)

        result = self.graph.khop(0, 10)

        self.assertEqual(sorted(result), [1, 2, 3])

    def test_results_larger_than_buffer_are_returned_in_full(self):
        for v in range(1, 5001):
            self.graph.add_edge(0, v)

        result = self.graph.khop(0, 1)

        self.assertEqual(5000, len(result))
        self.assertEqual(set(range(1, 5001)), set(result))

    def test_closed_graph_rejects_operations(self):
        graph = Graph()
        graph.close()

        with self.assertRaises(NodusError):
            graph.add_edge(1, 2)

    def test_invalid_node_ids_raise_before_reaching_native_code(self):
        with self.assertRaises(ValueError):
            self.graph.add_edge(-1, 2)
        with self.assertRaises(ValueError):
            self.graph.has_edge(2**31, 0)
        with self.assertRaises(TypeError):
            self.graph.degree("7")
        with self.assertRaises(TypeError):
            self.graph.khop(1, 2.0)

    def test_context_manager_closes_handle(self):
        with Graph() as graph:
            graph.add_edge(1, 2)
        with self.assertRaises(NodusError):
            graph.has_edge(1, 2)


class CAbiBoundaryTest(unittest.TestCase):
    BOGUS_HANDLE = 0x1234_5678

    def setUp(self):
        self.graph = Graph()
        self.lib = self.graph._lib
        self.thread = self.graph._thread
        self.buffer = (ctypes.c_int64 * 8)()

    def tearDown(self):
        self.graph.close()

    def test_invalid_handle_returns_sentinels_instead_of_crashing(self):
        handle = self.BOGUS_HANDLE

        self.assertEqual(0, self.lib.nodus_has_edge(self.thread, handle, 1, 2))
        self.assertEqual(0, self.lib.nodus_add_edge(self.thread, handle, 1, 2))
        self.assertEqual(-1, self.lib.nodus_degree(self.thread, handle, 1))
        self.assertEqual(-1, self.lib.nodus_in_degree(self.thread, handle, 1))
        self.assertEqual(-1, self.lib.nodus_common_neighbors(self.thread, handle, 1, 2, self.buffer, 8))
        self.assertEqual(-1, self.lib.nodus_khop(self.thread, handle, 1, 2, self.buffer, 8))

    def test_closed_handle_is_rejected_after_destroy(self):
        handle = self.graph._handle
        self.graph.close()

        self.assertEqual(0, self.lib.nodus_has_edge(self.thread, handle, 1, 2))
        self.assertEqual(-1, self.lib.nodus_khop(self.thread, handle, 1, 2, self.buffer, 8))
        self.lib.nodus_destroy(self.thread, handle)

    def test_double_destroy_is_contained(self):
        handle = self.graph._handle
        self.assertEqual(0, self.lib.nodus_destroy(self.thread, handle))
        self.assertEqual(-1, self.lib.nodus_destroy(self.thread, handle))
        self.graph._handle = None

        self.assertEqual(0, self.lib.nodus_has_edge(self.thread, handle, 1, 2))

    def test_negative_node_ids_reach_the_kernel_and_fail_cleanly(self):
        handle = self.graph._handle

        self.assertEqual(0, self.lib.nodus_add_edge(self.thread, handle, -1, 2))
        self.assertEqual(-1, self.lib.nodus_degree(self.thread, handle, -7))
        self.assertEqual(-1, self.lib.nodus_khop(self.thread, handle, -3, 2, self.buffer, 8))

    def test_batch_exports_reject_bad_handles_and_counts(self):
        sources = (ctypes.c_int64 * 2)(1, 2)
        targets = (ctypes.c_int64 * 2)(3, 4)

        self.assertEqual(-1, self.lib.nodus_add_edges_batch(self.thread, self.BOGUS_HANDLE, sources, targets, 2))
        self.assertEqual(-1, self.lib.nodus_add_edges_batch(self.thread, self.graph._handle, sources, targets, -1))
        self.assertFalse(self.graph.has_edge(1, 3))

    def test_batch_with_invalid_node_applies_nothing_at_the_abi(self):
        sources = (ctypes.c_int64 * 2)(1, -5)
        targets = (ctypes.c_int64 * 2)(2, 6)

        self.assertEqual(-1, self.lib.nodus_add_edges_batch(self.thread, self.graph._handle, sources, targets, 2))
        self.assertFalse(self.graph.has_edge(1, 2))

    def test_out_capacity_smaller_than_result_still_reports_full_count(self):
        handle = self.graph._handle
        for v in range(1, 6):
            self.graph.add_edge(0, v)

        total = self.lib.nodus_khop(self.thread, handle, 0, 1, self.buffer, 2)

        self.assertEqual(5, total)


class BatchTest(unittest.TestCase):
    def setUp(self):
        self.graph = Graph()

    def tearDown(self):
        self.graph.close()

    def test_add_edges_from_tuples_counts_new_edges(self):
        added = self.graph.add_edges_from([(1, 2), (1, 3), (1, 2)])

        self.assertEqual(2, added)
        self.assertTrue(self.graph.has_edge(1, 3))
        self.assertEqual(2, self.graph.degree(1))

    def test_add_edges_from_flat_list_and_array(self):
        self.assertEqual(2, self.graph.add_edges_from([1, 2, 3, 4]))
        self.assertEqual(1, self.graph.add_edges_from(array.array("q", [5, 6])))

        self.assertTrue(self.graph.has_edge(3, 4))
        self.assertTrue(self.graph.has_edge(5, 6))

    def test_flat_buffer_with_odd_length_is_rejected(self):
        with self.assertRaises(ValueError):
            self.graph.add_edges_from([1, 2, 3])

    def test_empty_batches_are_no_ops(self):
        self.assertEqual(0, self.graph.add_edges_from([]))
        self.assertEqual(0, self.graph.remove_edges_from([]))

    def test_invalid_node_rejects_the_whole_batch(self):
        self.graph.add_edge(9, 10)

        with self.assertRaises(ValueError):
            self.graph.add_edges_from([(1, 2), (3, -1), (5, 6)])
        with self.assertRaises(ValueError):
            self.graph.add_edges_from([1, 2, 3, 2**31])
        with self.assertRaises(ValueError):
            self.graph.add_edges_from([(1, 2**70)])

        self.assertFalse(self.graph.has_edge(1, 2))
        self.assertFalse(self.graph.has_edge(5, 6))
        self.assertEqual(1, self.graph.degree(9))

    def test_non_integer_nodes_are_rejected(self):
        with self.assertRaises(TypeError):
            self.graph.add_edges_from([(1.5, 2)])

    def test_remove_edges_from_counts_existing_edges(self):
        self.graph.add_edges_from([(0, 1), (0, 2), (0, 3)])

        removed = self.graph.remove_edges_from([(0, 1), (0, 1), (0, 2), (7, 7)])

        self.assertEqual(2, removed)
        self.assertEqual({3}, set(self.graph.khop(0, 1)))

    def test_large_batch_round_trips(self):
        count = 200_000
        edges = [(i, i + 1) for i in range(count)]

        self.assertEqual(count, self.graph.add_edges_from(edges))
        self.assertEqual(count, self.graph.remove_edges_from(edges))
        self.assertEqual(0, self.graph.degree(0))

    def test_batches_agree_with_single_edge_model(self):
        rng = random.Random(7)
        nodes = 48
        model = {u: set() for u in range(nodes)}

        for round_index in range(120):
            batch = [(u, v) for u, v in (
                (rng.randrange(nodes), rng.randrange(nodes)) for _ in range(rng.randrange(40))) if u != v]
            if rng.random() < 0.6:
                expected = 0
                for u, v in batch:
                    if v not in model[u]:
                        model[u].add(v)
                        expected += 1
                actual = self.graph.add_edges_from(batch)
            else:
                expected = 0
                for u, v in batch:
                    if v in model[u]:
                        model[u].remove(v)
                        expected += 1
                actual = self.graph.remove_edges_from(batch)
            label = f"round={round_index}"
            self.assertEqual(expected, actual, label)
            for u in range(nodes):
                self.assertEqual(len(model[u]), self.graph.degree(u), label)
                self.assertEqual(model[u], set(self.graph.khop(u, 1)), label)


class ModelAgreementTest(unittest.TestCase):
    NODES = 64
    OPERATIONS = 20_000

    def test_random_operations_agree_with_set_model(self):
        rng = random.Random(42)
        graph = Graph()
        out_edges = {u: set() for u in range(self.NODES)}
        in_edges = {v: set() for v in range(self.NODES)}

        for op in range(self.OPERATIONS):
            u = rng.randrange(self.NODES)
            v = rng.randrange(self.NODES)
            roll = rng.random()
            label = f"op={op} u={u} v={v}"
            if roll < 0.45:
                expected = v not in out_edges[u]
                if expected:
                    out_edges[u].add(v)
                    in_edges[v].add(u)
                self.assertEqual(expected, graph.add_edge(u, v), label)
            elif roll < 0.7:
                expected = v in out_edges[u]
                if expected:
                    out_edges[u].remove(v)
                    in_edges[v].remove(u)
                self.assertEqual(expected, graph.remove_edge(u, v), label)
            elif roll < 0.8:
                self.assertEqual(v in out_edges[u], graph.has_edge(u, v), label)
                self.assertEqual(len(out_edges[u]), graph.degree(u), label)
                self.assertEqual(len(in_edges[v]), graph.in_degree(v), label)
            elif roll < 0.9:
                expected = out_edges[u] & out_edges[v]
                self.assertEqual(expected, set(graph.common_neighbors(u, v)), label)
            else:
                depth = rng.randrange(4)
                expected = self._model_khop(out_edges, u, depth)
                self.assertEqual(expected, set(graph.khop(u, depth)), label + f" depth={depth}")

        graph.close()

    @staticmethod
    def _model_khop(out_edges, start, depth):
        seen = {start}
        frontier = {start}
        reached = set()
        for _ in range(depth):
            next_frontier = set()
            for node in frontier:
                for neighbor in out_edges[node]:
                    if neighbor not in seen:
                        seen.add(neighbor)
                        reached.add(neighbor)
                        next_frontier.add(neighbor)
            frontier = next_frontier
        return reached


if __name__ == "__main__":
    unittest.main()
