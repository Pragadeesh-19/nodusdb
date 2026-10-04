import ctypes
import shutil
import sys
import tempfile
import unittest
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph, NodusError, find_library  # noqa: E402

UUID_COUNT = 100_000


def _native_has_string_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    return hasattr(ctypes.CDLL(str(library)), "nodus_intern")


NATIVE_AVAILABLE = _native_has_string_exports()


def _uuid_sequence(count):
    mask = (1 << 128) - 1
    return [uuid.UUID(int=(index * 0x9E3779B97F4A7C15 + 1) & mask) for index in range(count)]


@unittest.skipUnless(NATIVE_AVAILABLE, "libnodusdb lacks string exports; rebuild with mvn -Pnative -pl core package")
class StringKeyTest(unittest.TestCase):
    def test_mixed_name_edges_work_in_memory(self):
        with Graph() as graph:
            self.assertTrue(graph.add_edge("user:alice", "role:admin"))
            graph.add_edge("user:bob", "role:admin")

            self.assertTrue(graph.has_edge("user:alice", "role:admin"))
            self.assertFalse(graph.has_edge("role:admin", "user:alice"))
            self.assertEqual(graph.khop("user:alice", 1), ["role:admin"])
            self.assertEqual(graph.common_neighbors("user:alice", "user:bob"), ["role:admin"])
            self.assertEqual(graph.degree("user:alice"), 1)
            self.assertEqual(graph.in_degree("role:admin"), 2)

    def test_removal_and_duplicates_follow_integer_semantics(self):
        with Graph() as graph:
            self.assertTrue(graph.add_edge("a", "b"))
            self.assertFalse(graph.add_edge("a", "b"))
            self.assertTrue(graph.remove_edge("a", "b"))
            self.assertFalse(graph.remove_edge("a", "b"))
            self.assertFalse(graph.has_edge("a", "b"))

    def test_uuid_objects_and_their_text_are_the_same_key(self):
        identifier = uuid.UUID("12345678-1234-5678-1234-567812345678")
        with Graph() as graph:
            graph.add_edge(identifier, "role:admin")

            self.assertTrue(graph.has_edge(str(identifier), "role:admin"))
            self.assertEqual(graph.khop(identifier, 1), ["role:admin"])

    def test_reads_of_unknown_strings_answer_without_creating_them(self):
        with Graph() as graph:
            graph.add_edge("a", "b")

            self.assertFalse(graph.has_edge("missing", "a"))
            self.assertFalse(graph.remove_edge("missing", "a"))
            self.assertEqual(graph.khop("missing", 3), [])
            self.assertEqual(graph.common_neighbors("missing", "a"), [])
            self.assertEqual(graph.degree("missing"), 0)
            self.assertEqual(graph._lookup("missing".encode()), -1)

    def test_one_edge_cannot_mix_integers_and_strings(self):
        with Graph() as graph:
            with self.assertRaises(TypeError):
                graph.add_edge("user:alice", 7)

    def test_a_graph_keyed_by_strings_refuses_integers(self):
        with Graph() as graph:
            graph.add_edge("user:alice", "role:admin")

            with self.assertRaises(TypeError):
                graph.add_edge(1, 2)
            with self.assertRaises(TypeError):
                graph.has_edge(1, 2)
            with self.assertRaises(TypeError):
                graph.khop(1, 1)

    def test_a_graph_keyed_by_integers_refuses_strings(self):
        with Graph() as graph:
            graph.add_edge(1, 2)

            with self.assertRaises(TypeError):
                graph.add_edge("user:alice", "role:admin")
            with self.assertRaises(TypeError):
                graph.common_neighbors("user:alice", "role:admin")

    def test_string_batch_adds_pairs_and_removes_known_ones(self):
        with Graph() as graph:
            added = graph.add_edges_from([("user:a", "role:x"), ("user:b", "role:x"), ("user:a", "role:y")])
            self.assertEqual(added, 3)

            removed = graph.remove_edges_from([("user:a", "role:x"), ("user:zzz", "role:x")])

            self.assertEqual(removed, 1)
            self.assertFalse(graph.has_edge("user:a", "role:x"))
            self.assertTrue(graph.has_edge("user:a", "role:y"))

    def test_hundred_thousand_uuids_map_to_dense_ids_and_resolve_exactly(self):
        identifiers = _uuid_sequence(UUID_COUNT)
        with Graph() as graph:
            pairs = [(identifiers[index], identifiers[index + 1]) for index in range(UUID_COUNT - 1)]

            self.assertEqual(graph.add_edges_from(pairs), UUID_COUNT - 1)

            ids = [graph._lookup(str(identifier).encode()) for identifier in identifiers]
            self.assertEqual(sorted(ids), list(range(UUID_COUNT)))
            for index in range(UUID_COUNT):
                self.assertEqual(graph._resolve(ids[index]), str(identifiers[index]))
            self.assertEqual(graph.khop(str(identifiers[0]), 1), [str(identifiers[1])])
            self.assertEqual(graph.khop(str(identifiers[50_000]), 1), [str(identifiers[50_001])])

    def test_string_keys_survive_a_durable_reopen(self):
        directory = tempfile.mkdtemp(prefix="nodus-strings-")
        try:
            with Graph(path=directory) as graph:
                graph.add_edge("user:alice", "role:admin")
                graph.add_edges_from([("user:bob", "role:admin")])

            with Graph(path=directory) as reopened:
                self.assertEqual(sorted(reopened.khop("role:admin", 1)), [])
                self.assertTrue(reopened.has_edge("user:alice", "role:admin"))
                self.assertEqual(reopened.in_degree("role:admin"), 2)
                with self.assertRaises(TypeError):
                    reopened.add_edge(1, 2)
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    def test_a_durable_integer_graph_stays_integer_keyed(self):
        directory = tempfile.mkdtemp(prefix="nodus-ints-")
        try:
            with Graph(path=directory) as graph:
                graph.add_edge(1, 2)

            with Graph(path=directory) as reopened:
                self.assertTrue(reopened.has_edge(1, 2))
                with self.assertRaises(TypeError):
                    reopened.add_edge("user:alice", "role:admin")
        finally:
            shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
