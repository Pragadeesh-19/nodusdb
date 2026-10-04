import ctypes
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph, NodusError, find_library  # noqa: E402


def _native_has_durable_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    return hasattr(ctypes.CDLL(str(library)), "nodus_open_durable")


@unittest.skipUnless(_native_has_durable_exports(), "libnodusdb lacks durable exports; rebuild the native library")
class DurableGraphTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="nodus-graph-")

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def test_edges_survive_a_clean_close_and_reopen(self):
        with Graph(path=self.directory, sync_mode="sync") as graph:
            graph.add_edges_from([(1, 2), (2, 3), (3, 4), (1, 5), (5, 4)])
            graph.remove_edge(2, 3)

        with Graph(path=self.directory) as reopened:
            self.assertEqual(2, reopened.degree(1))
            self.assertFalse(reopened.has_edge(2, 3))
            self.assertEqual([4], reopened.common_neighbors(3, 5))
            self.assertEqual({2, 4, 5}, set(reopened.khop(start=1, max_depth=2)))

    def test_checkpoint_then_reopen_restores_the_same_graph(self):
        with Graph(path=self.directory) as graph:
            graph.add_edges_from([(i, i + 1) for i in range(1, 500)])
            graph.checkpoint()
            graph.add_edge(900, 901)

        with Graph(path=self.directory) as reopened:
            self.assertTrue(reopened.has_edge(1, 2))
            self.assertTrue(reopened.has_edge(499, 500))
            self.assertTrue(reopened.has_edge(900, 901))

    def test_second_handle_on_the_same_directory_is_rejected(self):
        with Graph(path=self.directory):
            with self.assertRaises(NodusError):
                Graph(path=self.directory)

    def test_unknown_sync_mode_is_rejected_before_opening(self):
        with self.assertRaises(ValueError):
            Graph(path=self.directory, sync_mode="eventually")

    def test_in_memory_graph_ignores_durability_arguments(self):
        with Graph() as graph:
            graph.add_edge(1, 2)
            self.assertTrue(graph.has_edge(1, 2))
            with self.assertRaises(NodusError):
                graph.checkpoint()


if __name__ == "__main__":
    unittest.main()
