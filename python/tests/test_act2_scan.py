import ctypes
import io
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))
sys.path.insert(0, str(PYTHON_DIRECTORY / "benchmarks"))

from nodusdb import Graph, NodusError, Shipping, find_library, generate_signing_key  # noqa: E402

try:
    import fastavro
    import pyarrow as pa
    import pyarrow.parquet as pq
    from act2 import lake, scan
except ImportError:
    scan = None

REQUIRED = os.environ.get("NODUS_REQUIRE_PYTHON_VERIFIERS") is not None
WAIT_SECONDS = 60
NAME = "user:target"
FIELD_IDS = {"object_type": 6, "object_id": 7, "subject_type": 9, "subject_id": 10}
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


def _native_has_shipping_exports():
    try:
        return hasattr(ctypes.CDLL(str(find_library())), "nodus_open_durable_shipping")
    except NodusError:
        return False


def _require_readers(case):
    if scan is None:
        if REQUIRED:
            case.fail("pyarrow or fastavro is not installed and NODUS_REQUIRE_PYTHON_VERIFIERS is set")
        case.skipTest("pyarrow or fastavro is not installed")


class ScanFormatsTest(unittest.TestCase):
    def setUp(self):
        _require_readers(self)
        self.root = Path(tempfile.mkdtemp(prefix="act2-scan-"))
        self.bucket = self.root / "bucket"
        self.graph_directory = self.root / "graph"
        self.bucket.mkdir()
        self.graph_directory.mkdir()

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def put(self, key, content):
        path = self.bucket / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path

    def scan(self, name=NAME):
        return scan.scan(name, self.graph_directory, lake.DirectoryObjects(self.bucket))

    def parquet(self, rows, **options):
        columns = ("object_type", "object_id", "subject_type", "subject_id", "detail")
        table = pa.table({column: [row[index] for row in rows] for index, column in enumerate(columns)})
        stream = io.BytesIO()
        pq.write_table(table, stream, **options)
        return stream.getvalue()

    def put_manifest(self, lower, upper):
        metadata = {"current-schema-id": 0, "schemas": [{"schema-id": 0, "fields": [
            {"id": field, "name": name, "type": "string"} for name, field in FIELD_IDS.items()]}],
            "current-snapshot-id": 1, "snapshots": [{"snapshot-id": 1, "summary": {lake.PROJECTED_LSN: "1"}}]}
        self.put("iceberg/metadata/v1.metadata.json", json.dumps(metadata).encode("utf-8"))
        bound = {"type": "record", "name": "bound", "fields": [
            {"name": "key", "type": "int"}, {"name": "value", "type": "bytes"}]}
        schema = {"type": "record", "name": "manifest_entry", "fields": [{"name": "data_file", "type": {
            "type": "record", "name": "data_file", "fields": [
                {"name": "lower_bounds", "type": {"type": "array", "items": bound}},
                {"name": "upper_bounds", "type": {"type": "array", "items": "bound"}}]}}]}
        record = {"data_file": {
            "lower_bounds": [{"key": FIELD_IDS[name], "value": value.encode()} for name, value in lower.items()],
            "upper_bounds": [{"key": FIELD_IDS[name], "value": value.encode()} for name, value in upper.items()]}}
        stream = io.BytesIO()
        fastavro.writer(stream, fastavro.parse_schema(schema), [record])
        self.put("iceberg/metadata/m0.avro", stream.getvalue())

    def test_a_name_split_into_type_and_id_columns_is_found_as_a_row_that_raw_bytes_miss(self):
        content = self.parquet([("document", "d1", "user", "target", ""), ("document", "d2", "user", "zeta", "")],
                               compression="snappy")
        self.assertNotIn(NAME.encode(), content)
        self.put("iceberg/data/a.parquet", content)

        self.assertEqual([("Iceberg data files", "parquet row"), ("Iceberg data files", "parquet statistics")],
                         [(hit.layer, hit.how) for hit in self.scan()])

    def test_statistics_that_mix_types_do_not_pin_the_name(self):
        rows = [("document", "d1", "group", "target", ""), ("document", "d2", "user", "zeta", "")]
        self.put("iceberg/data/a.parquet", self.parquet(rows, compression="snappy"))

        self.assertEqual([], self.scan())

    def test_a_name_hidden_by_snappy_in_a_value_column_is_found_decoded(self):
        name = "user:" + "ab" * 40
        content = self.parquet([("document", "d1", "user", "x", "granted to " + name)] * 3, compression="snappy",
                               write_statistics=False)
        self.assertNotIn(name.encode(), content)
        self.put("iceberg/data/a.parquet", content)

        self.assertEqual(["parquet value"], [hit.how for hit in self.scan(name)])

    def test_the_same_id_under_another_type_is_not_the_name(self):
        self.put("iceberg/data/a.parquet", self.parquet([("document", "target", "user", "someone", "")]))
        self.put_manifest({"object_type": "document", "object_id": "target"},
                          {"object_type": "document", "object_id": "target"})

        self.assertEqual([], self.scan())

    def test_manifest_bounds_that_pin_the_name_are_found(self):
        self.put_manifest({"subject_type": "user", "subject_id": "target"},
                          {"subject_type": "user", "subject_id": "zeta"})

        self.assertEqual([("Iceberg manifests", "manifest bounds")], [(hit.layer, hit.how) for hit in self.scan()])

    def test_native_files_are_matched_as_plain_bytes(self):
        (self.graph_directory / "snapshot.bin").write_bytes(b"\x00\x01" + NAME.encode() + b"\x00")
        self.put("_nodus/chain/00000000000000000001.obj", b"\x07" + NAME.encode())

        self.assertEqual({("graph directory", "raw bytes"), ("chain objects", "raw bytes")},
                         {(hit.layer, hit.how) for hit in self.scan()})

    def test_an_object_that_vanishes_between_listing_and_reading_is_no_longer_a_copy(self):
        class Vanishing(lake.DirectoryObjects):
            def read(self, key):
                return None if key.startswith("_nodus/chain/") else super().read(key)

        self.put("_nodus/chain/00000000000000000001.obj", NAME.encode())
        self.put("_nodus/snapshots/00000000000000000002.nsnap", NAME.encode())

        hits = scan.scan(NAME, self.graph_directory, Vanishing(self.bucket))

        self.assertEqual([scan.SNAPSHOTS], [hit.layer for hit in hits])

    def test_a_layer_the_scan_never_sees_the_name_in_fails_the_control(self):
        hits = [scan.Hit(layer, "x", "raw bytes") for layer in (scan.GRAPH_DIRECTORY, scan.CHAIN, scan.SNAPSHOTS)]
        with self.assertRaisesRegex(scan.ScanError, "nowhere in: Iceberg data files"):
            scan.require_every_layer(hits, NAME)
        scan.require_every_layer(hits + [scan.Hit(scan.DATA, "y", "parquet row")], NAME)

    def test_any_copy_left_fails_the_erasure(self):
        with self.assertRaisesRegex(scan.ScanError, "1 copies still hold user:target, first y"):
            scan.require_nowhere([scan.Hit(scan.DATA, "y", "parquet row")], NAME)
        scan.require_nowhere([], NAME)


@unittest.skipUnless(_native_has_shipping_exports(), "libnodusdb lacks the shipping exports; rebuild the native library")
class ScanShippedGraphTest(unittest.TestCase):
    def setUp(self):
        _require_readers(self)
        self.root = Path(tempfile.mkdtemp(prefix="act2-scan-shipped-"))
        generate_signing_key(self.root / "signing.pem", self.root / "signing.pub")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def await_until(self, condition, what):
        deadline = time.monotonic() + WAIT_SECONDS
        while not condition():
            self.assertLess(time.monotonic(), deadline, what)
            time.sleep(0.05)

    def test_a_name_held_by_every_layer_is_gone_after_wait_erased_while_a_namesake_stays(self):
        shipping = Shipping.directory(self.root / "bucket", key_file=self.root / "signing.pem", key_id=1,
                                      interval_ms=10, iceberg=True, iceberg_commit_interval_s=1)
        objects = lake.DirectoryObjects(self.root / "bucket")
        with Graph(path=self.root / "graph", sync_mode="sync", shipping=shipping) as graph:
            graph.apply_schema(SCHEMA)
            graph.add_tuple("group:team", "member", NAME)
            graph.add_tuple("document:target", "viewer", "group:team", "member")
            written = graph.add_tuple("document:target", "viewer", "user:keeper")
            graph.checkpoint()
            checkpointed = graph.token.lsn
            self.await_until(lambda: graph.stats()["shipping"]["snapshot_references"]["lsn"] >= checkpointed,
                             "the checkpoint's snapshot was never referenced")
            self.await_until(lambda: (lake.newest_version(objects) or lake.TableVersion(0, "", -1)).projected_lsn
                             >= written.lsn, "the lake never held the writes")

            scan.require_every_layer(scan.scan(NAME, self.root / "graph", objects), NAME)

            graph.wait_erased(graph.erase(NAME), timeout=WAIT_SECONDS)

            scan.require_nowhere(scan.scan(NAME, self.root / "graph", objects), NAME)
            self.assertTrue(scan.scan("document:target", self.root / "graph", objects))
            self.assertTrue(graph.check("document:target", "view", "user:keeper"))


if __name__ == "__main__":
    unittest.main()
