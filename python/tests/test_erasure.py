import ctypes
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
    NodusError,
    NodusSchemaError,
    NodusUnsupportedError,
    Shipping,
    Token,
    Transaction,
    find_library,
    generate_signing_key,
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
ERASED_ID = NAME.split(":", 1)[1].encode("utf-8")
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
            try:
                with open(path, "rb") as stream:
                    content = stream.read()
            except FileNotFoundError:
                continue
            if needle in content:
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
            self.assertTrue(files_holding(self.path, ERASED_ID))

            token = graph.erase(NAME)
            graph.wait_erased(token, timeout=WAIT_SECONDS)

            self.assertEqual([], files_holding(self.path, ERASED_ID))
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
        self.assertTrue(files_holding(self.path, ERASED_ID))

        with Graph(path=self.path, sync_mode="sync") as graph:
            self.assertTrue(graph.stats()["erasure"]["local"]["pending"])
            graph.wait_erased(Token(epoch, lsn), timeout=WAIT_SECONDS)

            self.assertFalse(graph.check("document:readme", "view", NAME))
            self.assertEqual([], files_holding(self.path, ERASED_ID))



def _native_has_shipping_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    return hasattr(ctypes.CDLL(str(library)), "nodus_open_durable_shipping")


@unittest.skipUnless(_native_has_shipping_exports(), "libnodusdb lacks the shipping exports; rebuild the native library")
class ShippedErasureTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="nodus-erasure-ship-"))
        self.bucket = self.root / "bucket"
        self.private_key = self.root / "signing.pem"
        generate_signing_key(self.private_key, self.root / "signing.pub")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_after_wait_erased_neither_the_bucket_nor_the_iceberg_table_holds_the_name(self):
        shipping = Shipping.directory(self.bucket, key_file=self.private_key, key_id=7, interval_ms=10,
                                      iceberg=True, iceberg_commit_interval_s=1)
        with Graph(path=self.root / "graph", sync_mode="sync", shipping=shipping) as graph:
            graph.apply_schema(SCHEMA)
            graph.write(Transaction().add("document:readme", "viewer", NAME).add("group:eng", "member", NAME)
                        .add("document:plan", "viewer", "user:bob"), durability="lake")
            self.assertTrue(files_holding(self.bucket, ERASED_ID))

            token = graph.erase(NAME)
            graph.wait_erased(token, timeout=WAIT_SECONDS)

            self.assertEqual([], files_holding(self.bucket, ERASED_ID))
            self.assertEqual([], files_holding(self.root / "graph", ERASED_ID))
            erasure = graph.stats()["erasure"]
            for layer in ("local", "bucket", "lake"):
                self.assertTrue(erasure[layer]["configured"], layer)
                self.assertFalse(erasure[layer]["pending"], layer)
            self.assertTrue(graph.check("document:plan", "view", "user:bob"))

if __name__ == "__main__":
    unittest.main()
