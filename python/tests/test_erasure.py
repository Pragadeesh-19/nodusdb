import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))

from nodusdb import (  # noqa: E402
    Graph,
    NodusSchemaError,
    NodusUnsupportedError,
    Token,
)

SCHEMA = """
schema 1
type user
type group {
  relation member: user | group#member
}
type document {
  relation viewer: user | group#member
  permission view = viewer
}
"""

NAME = "user:alice-erasure-target"
WAIT_SECONDS = 30

CHILD = f"""
import sys
from nodusdb import Graph
graph = Graph(path=sys.argv[1], sync_mode="sync")
graph.apply_schema('''{SCHEMA}''')
graph.add_tuple("document:readme", "viewer", "{NAME}")
graph.add_tuple("group:eng", "member", "{NAME}")
token = graph.erase("{NAME}")
print(token.epoch, token.lsn, flush=True)
sys.stdin.readline()
"""


def files_holding(root, needle):
    holding = []
    for directory, _, names in os.walk(root):
        for name in names:
            if name == "nodus.lock":
                continue
            path = os.path.join(directory, name)
            with open(path, "rb") as stream:
                if needle in stream.read():
                    holding.append(path)
    return holding


class InMemoryErasureTest(unittest.TestCase):
    def setUp(self):
        self.graph = Graph()
        self.graph.apply_schema(SCHEMA)
        self.graph.add_tuple("document:readme", "viewer", "user:alice")
        self.graph.add_tuple("group:eng", "member", "user:alice")
        self.graph.add_tuple("document:plan", "viewer", "group:eng", "member")

    def tearDown(self):
        self.graph.close()

    def test_an_erased_name_loses_every_grant_and_is_complete_at_once(self):
        token = self.graph.erase("user:alice")

        self.assertFalse(self.graph.check("document:readme", "view", "user:alice"))
        self.assertFalse(self.graph.check("document:plan", "view", "user:alice"))
        self.assertEqual(self.graph.token, token)
        self.graph.wait_erased(token, timeout=1)
        erasure = self.graph.stats()["erasure"]
        self.assertEqual(1, erasure["erased_symbols"])
        self.assertEqual(token.lsn, erasure["newest_erase_lsn"])
        self.assertFalse(erasure["local"]["configured"])

    def test_the_same_name_written_again_starts_without_grants(self):
        self.graph.erase("user:alice")

        self.graph.add_tuple("document:new", "viewer", "user:alice")

        self.assertTrue(self.graph.check("document:new", "view", "user:alice"))
        self.assertFalse(self.graph.check("document:readme", "view", "user:alice"))

    def test_an_unknown_name_or_a_second_erase_changes_nothing(self):
        first = self.graph.erase("user:alice")

        self.assertEqual(first, self.graph.erase("user:alice"))
        self.assertEqual(first, self.graph.erase("user:nobody"))

    def test_a_schema_name_cannot_be_erased(self):
        with self.assertRaises(NodusSchemaError):
            self.graph.erase("document")

    def test_the_name_must_be_text(self):
        with self.assertRaises(TypeError):
            self.graph.erase(42)


class IntegerGraphErasureTest(unittest.TestCase):
    def test_an_integer_keyed_graph_has_no_names_to_erase(self):
        with Graph() as graph:
            graph.add_edge(1, 2)
            with self.assertRaises(NodusUnsupportedError):
                graph.erase("user:alice")


class DurableErasureTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="nodus-erasure-")
        self.path = os.path.join(self.directory, "graph")

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def test_after_wait_erased_no_file_in_the_directory_holds_the_name(self):
        with Graph(path=self.path, sync_mode="sync") as graph:
            graph.apply_schema(SCHEMA)
            graph.add_tuple("document:readme", "viewer", NAME)
            graph.add_tuple("group:eng", "member", NAME)
            graph.checkpoint()
            self.assertTrue(files_holding(self.path, NAME.encode("utf-8")))

            token = graph.erase(NAME)
            graph.wait_erased(token, timeout=WAIT_SECONDS)

            self.assertEqual([], files_holding(self.path, NAME.encode("utf-8")))
            local = graph.stats()["erasure"]["local"]
            self.assertTrue(local["configured"])
            self.assertFalse(local["pending"])
            self.assertGreaterEqual(local["covered_lsn"], token.lsn)

    def test_a_killed_writer_leaves_an_erasure_that_the_next_open_finishes(self):
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(PYTHON_DIRECTORY), env.get("PYTHONPATH")]))
        child = subprocess.Popen(
            [sys.executable, "-u", "-c", CHILD, self.path],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env,
        )
        try:
            line = child.stdout.readline()
            self.assertTrue(line, "the child exited before it erased")
            epoch, lsn = (int(part) for part in line.split())
        finally:
            child.kill()
            child.wait()
            child.stdout.close()
            child.stdin.close()
        self.assertTrue(files_holding(self.path, NAME.encode("utf-8")))

        with Graph(path=self.path, sync_mode="sync") as graph:
            self.assertTrue(graph.stats()["erasure"]["local"]["pending"])
            graph.wait_erased(Token(epoch, lsn), timeout=WAIT_SECONDS)

            self.assertFalse(graph.check("document:readme", "view", NAME))
            self.assertEqual([], files_holding(self.path, NAME.encode("utf-8")))


if __name__ == "__main__":
    unittest.main()
