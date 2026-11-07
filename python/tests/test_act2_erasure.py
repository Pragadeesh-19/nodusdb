import ctypes
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))
sys.path.insert(0, str(PYTHON_DIRECTORY / "benchmarks"))

from act2 import erasure  # noqa: E402
from nodusdb import Graph, NodusError, Shipping, find_library, generate_signing_key  # noqa: E402

WAIT_SECONDS = 60
POLL_NS = 10_000_000
NAME = "user:target"
SCHEMA = """
schema 1
type user
type document {
  relation viewer: user
  permission view = viewer
}
"""


def _native_has_shipping_exports():
    try:
        return hasattr(ctypes.CDLL(str(find_library())), "nodus_open_durable_shipping")
    except NodusError:
        return False


@unittest.skipUnless(_native_has_shipping_exports(), "libnodusdb lacks the shipping exports; rebuild the native library")
class ErasureTimingTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="act2-erasure-"))
        generate_signing_key(self.root / "signing.pem", self.root / "signing.pub")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def open(self, iceberg):
        shipping = Shipping.directory(self.root / "bucket", key_file=self.root / "signing.pem", key_id=1,
                                      interval_ms=10, iceberg=iceberg, iceberg_commit_interval_s=1)
        graph = Graph(path=self.root / "graph", sync_mode="sync", shipping=shipping)
        graph.apply_schema(SCHEMA)
        graph.add_tuple("document:readme", "viewer", NAME)
        return graph

    def test_every_layer_reports_when_it_was_scrubbed(self):
        with self.open(iceberg=True) as graph:
            token, times = erasure.erase(graph, NAME, 1, WAIT_SECONDS, POLL_NS)

            self.assertFalse(graph.check("document:readme", "view", NAME))
        self.assertEqual(set(erasure.LAYERS), set(times.layers_ns))
        self.assertTrue(0 < times.erase_ns <= times.wait_ns)
        self.assertEqual((NAME, 1, POLL_NS), (times.name, times.tuples, times.poll_ns))
        self.assertGreater(token.lsn, 0)

    def test_a_layer_that_is_not_configured_is_refused_before_anything_is_erased(self):
        with self.open(iceberg=False) as graph:
            with self.assertRaisesRegex(erasure.ErasureError, "lake; nothing was erased"):
                erasure.erase(graph, NAME, 1, WAIT_SECONDS, POLL_NS)

            self.assertTrue(graph.check("document:readme", "view", NAME))


if __name__ == "__main__":
    unittest.main()
