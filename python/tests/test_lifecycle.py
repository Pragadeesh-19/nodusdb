import contextlib
import gc
import shutil
import sys
import tempfile
import threading
import unittest
import warnings
import weakref
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph, LakeTable, NodusError, _native  # noqa: E402


def _native_has_lake():
    try:
        library, _ = _native.load()
    except NodusError:
        return False
    return hasattr(library, "nodus_lake_open")


SCHEMA = {"amount": "int64", "score": "float64", "status": "int32", "label": "utf8"}
ROW = {"amount": 7, "score": 1.5, "status": 2, "label": "kept"}


@contextlib.contextmanager
def resource_warnings():
    """Yield a list that holds the ResourceWarnings raised in the block and by a collection after it."""
    found = []
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield found
        gc.collect()
    found.extend(w for w in caught if issubclass(w.category, ResourceWarning))


def mentions(warned, text):
    return any(text in str(w.message) for w in warned)


class OwnedHandleTest(unittest.TestCase):
    def owned(self, outcomes):
        calls = []

        def release(thread, value):
            calls.append(value)
            return outcomes.pop(0)

        return _native.OwnedHandle(object(), object(), 42, release, "release failed"), calls

    def setUp(self):
        patcher = mock.patch.object(_native, "current_thread", return_value=object())
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_a_successful_release_clears_the_handle_and_happens_once(self):
        owned, calls = self.owned([True])

        owned.close()
        owned.close()

        self.assertIsNone(owned.value)
        self.assertEqual([42], calls)

    def test_a_failed_release_keeps_the_handle_so_close_can_be_retried(self):
        owned, calls = self.owned([False, True])

        with self.assertRaises(NodusError):
            owned.close()
        self.assertEqual(42, owned.value)
        owned.close()

        self.assertIsNone(owned.value)
        self.assertEqual([42, 42], calls)

    def test_a_handle_that_was_never_set_is_not_released(self):
        owned, calls = self.owned([])
        owned.value = None

        owned.close()

        self.assertEqual([], calls)


class GraphLifecycleTest(unittest.TestCase):
    def test_a_collected_graph_releases_its_handle_and_warns(self):
        graph = Graph()
        graph.add_edge(1, 2)
        owned = graph._owned

        with resource_warnings() as warned:
            del graph

        self.assertIsNone(owned.value)
        self.assertTrue(mentions(warned, "unclosed nodusdb Graph"))

    def test_a_closed_graph_leaves_nothing_to_finalize(self):
        graph = Graph()
        graph.close()

        self.assertFalse(graph._finalizer.alive)
        with resource_warnings() as warned:
            del graph
        self.assertEqual([], warned)

    def test_the_finalizer_does_not_keep_the_graph_alive(self):
        graph = Graph()
        reference = weakref.ref(graph)

        with resource_warnings():
            del graph

        self.assertIsNone(reference())

    def test_a_graph_that_fails_to_construct_is_released_without_a_warning(self):
        with resource_warnings() as warned:
            with self.assertRaises(ValueError):
                Graph(result_capacity=-1)

        self.assertEqual([], warned)

    def test_a_graph_dropped_on_another_thread_is_released_there(self):
        holder = [Graph()]
        holder[0].add_edge(1, 2)
        owned = holder[0]._owned

        def drop():
            with resource_warnings():
                holder.clear()

        worker = threading.Thread(target=drop)
        worker.start()
        worker.join()

        self.assertIsNone(owned.value)


class DurableGraphLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="nodus-life-")

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def test_a_collected_durable_graph_frees_its_directory_and_keeps_its_edges(self):
        graph = Graph(path=self.directory)
        graph.add_edges_from([(1, 2), (2, 3)])

        with resource_warnings():
            del graph

        with Graph(path=self.directory) as reopened:
            self.assertTrue(reopened.has_edge(1, 2))
            self.assertTrue(reopened.has_edge(2, 3))

    def test_an_unclosed_durable_graph_blocks_a_second_open_until_it_is_released(self):
        graph = Graph(path=self.directory)

        with self.assertRaises(NodusError):
            Graph(path=self.directory)

        graph.close()
        with Graph(path=self.directory):
            pass


@unittest.skipUnless(_native_has_lake(), "libnodusdb lacks lake exports")
class LakeTableLifecycleTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="nodus-lake-life-")

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def test_a_collected_table_is_flushed_and_released(self):
        table = LakeTable(self.directory, SCHEMA)
        table.upsert(1, ROW)
        owned = table._owned

        with resource_warnings() as warned:
            del table

        self.assertIsNone(owned.value)
        self.assertTrue(mentions(warned, "unclosed nodusdb LakeTable"))
        with LakeTable(self.directory, SCHEMA) as reopened:
            self.assertEqual(ROW, reopened.get(1))

    def test_a_closed_table_leaves_nothing_to_finalize(self):
        table = LakeTable(self.directory, SCHEMA)
        table.close()

        self.assertFalse(table._finalizer.alive)
        with resource_warnings() as warned:
            del table
        self.assertEqual([], warned)

    def test_using_a_closed_table_names_the_problem(self):
        table = LakeTable(self.directory, SCHEMA)
        table.close()

        with self.assertRaisesRegex(NodusError, "closed"):
            table.upsert(1, ROW)
        with self.assertRaisesRegex(NodusError, "closed"):
            table.get(1)
        table.close()

    def test_context_manager_flushes_returns_the_table_and_keeps_errors(self):
        with self.assertRaises(KeyError):
            with LakeTable(self.directory, SCHEMA) as table:
                self.assertIsInstance(table, LakeTable)
                table.upsert(1, ROW)
                raise KeyError("boom")

        with LakeTable(self.directory, SCHEMA) as reopened:
            self.assertEqual(ROW, reopened.get(1))


if __name__ == "__main__":
    unittest.main()
