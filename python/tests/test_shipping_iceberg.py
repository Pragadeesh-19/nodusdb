import ctypes
import os
import re
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))

from nodusdb import Graph, NodusError, Shipping, Transaction, find_library, generate_signing_key  # noqa: E402

try:
    from pyiceberg.table import StaticTable
except ImportError:
    StaticTable = None

SCHEMA = """
schema 1
type user
type document {
  relation viewer: user
  permission view = viewer
}
"""

REQUIRED = os.environ.get("NODUS_REQUIRE_PYTHON_VERIFIERS") is not None


def _native_has_shipping_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    return hasattr(ctypes.CDLL(str(library)), "nodus_open_durable_shipping")


def _newest_metadata(table_directory):
    versions = []
    for entry in (Path(table_directory) / "metadata").glob("v*.metadata.json"):
        match = re.fullmatch(r"v(\d+)\.metadata\.json", entry.name)
        if match:
            versions.append((int(match.group(1)), entry))
    return max(versions)[1]


@unittest.skipUnless(_native_has_shipping_exports(), "libnodusdb lacks the shipping exports; rebuild the native library")
class ShippingIcebergTest(unittest.TestCase):
    def setUp(self):
        if StaticTable is None:
            if REQUIRED:
                self.fail("pyiceberg is not installed and NODUS_REQUIRE_PYTHON_VERIFIERS is set")
            self.skipTest("pyiceberg is not installed")
        self.root = Path(tempfile.mkdtemp(prefix="nodus-iceberg-"))
        self.bucket = self.root / "bucket"
        self.private_key = self.root / "signing.pem"
        self.public_key = self.root / "signing.pub"
        generate_signing_key(self.private_key, self.public_key)

    def tearDown(self):
        shutil.rmtree(getattr(self, "root", "."), ignore_errors=True)

    def shipping(self):
        return Shipping.directory(
            self.bucket, key_file=self.private_key, key_id=7, public_key_file=self.public_key, interval_ms=10,
            iceberg=True, iceberg_commit_interval_s=1)

    def rows(self):
        table = StaticTable.from_metadata(str(_newest_metadata(self.bucket / "iceberg")).replace("\\", "/"))
        return table.scan().to_arrow().drop_columns(["commit_ts"]).to_pylist()

    def test_the_log_is_readable_as_an_iceberg_table_with_names_not_ids(self):
        with Graph(path=self.root / "graph", sync_mode="sync", shipping=self.shipping()) as graph:
            graph.apply_schema(SCHEMA)
            graph.write(Transaction().add("document:readme", "viewer", "user:alice"), durability="lake")
            graph.write(Transaction().add("document:plan", "viewer", "user:bob"), durability="lake")
            graph.write(Transaction().remove("document:readme", "viewer", "user:alice"), durability="lake")

        rows = self.rows()

        changes = [(row["event"], row["object_type"], row["object_id"], row["relation"], row["subject_type"],
                    row["subject_id"]) for row in rows if row["object_type"] == "document"]
        self.assertIn(("add", "document", "readme", "viewer", "user", "alice"), changes)
        self.assertIn(("add", "document", "plan", "viewer", "user", "bob"), changes)
        self.assertIn(("remove", "document", "readme", "viewer", "user", "alice"), changes)
        lsns = [row["lsn"] for row in rows]
        self.assertEqual(sorted(lsns), lsns)
        self.assertEqual(len(set(lsns)), len(lsns))

    def test_stats_report_the_projection_and_a_restart_adds_to_the_same_table(self):
        graph_path = self.root / "graph"
        with Graph(path=graph_path, sync_mode="sync", shipping=self.shipping()) as graph:
            graph.apply_schema(SCHEMA)
            graph.write(Transaction().add("document:one", "viewer", "user:alice"), durability="lake")
            self.assertIn(graph.stats()["shipping"]["projection"]["phase"], ("STARTING", "ACTIVE"))
        first = len(self.rows())

        with Graph(path=graph_path, sync_mode="sync", shipping=self.shipping()) as graph:
            graph.write(Transaction().add("document:two", "viewer", "user:alice"), durability="lake")

        rows = self.rows()
        self.assertGreater(len(rows), first)
        self.assertEqual(len({row["lsn"] for row in rows}), len(rows))
        objects = {row["object_id"] for row in rows if row["object_type"] == "document"}
        self.assertTrue({"one", "two"} <= objects)


if __name__ == "__main__":
    unittest.main()
