import ctypes
import glob
import math
import os
import shutil
import struct
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import LakeTable, NodusError, find_library, key_hash  # noqa: E402
from nodusdb import lake as lake_module  # noqa: E402

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None

def _native_has_lake_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    return hasattr(ctypes.CDLL(str(library)), "nodus_lake_open")


NATIVE_AVAILABLE = _native_has_lake_exports()

SCHEMA = {"amount": "int64", "score": "float64", "status": "int32", "label": "utf8"}


def bits(value):
    return struct.unpack("<q", struct.pack("<d", value))[0]


class KeyHashTest(unittest.TestCase):
    def test_integers_pass_through_unchanged(self):
        self.assertEqual(key_hash(42), 42)
        self.assertEqual(key_hash(-1), -1)

    def test_strings_and_bytes_hash_identically(self):
        self.assertEqual(key_hash("order-7"), key_hash(b"order-7"))
        self.assertNotEqual(key_hash("order-7"), key_hash("order-8"))

    def test_rejects_booleans_and_out_of_range_integers(self):
        with self.assertRaises(TypeError):
            key_hash(True)
        with self.assertRaises(ValueError):
            key_hash(2**63)


class PackingTest(unittest.TestCase):
    def test_float_bits_survive_nan_payloads(self):
        payload_nan = struct.unpack("<d", struct.pack("<q", 0x7FF8_0000_DEAD_BEEF))[0]
        packed = lake_module._float_bits("score", payload_nan)
        self.assertEqual(packed, 0x7FF8_0000_DEAD_BEEF)
        self.assertTrue(math.isnan(lake_module._float_from_bits(packed)))

    def test_negative_zero_keeps_its_sign_bit(self):
        self.assertEqual(lake_module._float_bits("score", -0.0), bits(-0.0))

    def test_unknown_schema_type_is_rejected(self):
        with self.assertRaises(ValueError):
            lake_module._validate_schema({"a": "decimal"})


@unittest.skipUnless(NATIVE_AVAILABLE, "libnodusdb lacks lake exports; rebuild with mvn -Pnative -pl core package")
class LakeNativeTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="nodus-lake-")

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def test_get_returns_latest_value_before_and_after_flush(self):
        with LakeTable(self.directory, SCHEMA) as table:
            table.upsert("row-1", {"amount": 10, "score": 1.5, "status": 3, "label": "one"})
            self.assertEqual(table.get("row-1")["label"], "one")
            table.upsert("row-1", {"amount": 11, "score": 2.5, "status": 4, "label": "uno"})
            table.flush()
            self.assertEqual(table.get("row-1"), {"amount": 11, "score": 2.5, "status": 4, "label": "uno"})

    def test_delete_hides_committed_row(self):
        with LakeTable(self.directory, SCHEMA) as table:
            table.upsert(7, {"amount": 1, "score": 0.0, "status": 0, "label": ""})
            table.flush()
            table.delete(7)
            self.assertIsNone(table.get(7))
            table.flush()
            self.assertIsNone(table.get(7))

    def test_periodic_flush_commits_without_explicit_call(self):
        with LakeTable(self.directory, SCHEMA, flush_interval=0.05) as table:
            table.upsert(1, {"amount": 1, "score": 1.0, "status": 1, "label": "tick"})
            deadline = time.monotonic() + 10
            while not glob.glob(os.path.join(self.directory, "data-*.parquet")):
                if time.monotonic() > deadline:
                    self.fail("periodic flush did not commit")
                time.sleep(0.02)

    @unittest.skipIf(pq is None, "pyarrow is required for the independent Parquet read")
    def test_hundred_thousand_rows_read_back_bit_for_bit_with_pyarrow(self):
        expected = {}
        with LakeTable(self.directory, SCHEMA, flush_rows=1 << 20) as table:
            rows = []
            for i in range(100_000):
                label = f"row-{i}-{'é' if i % 3 == 0 else 'x'}"
                score = struct.unpack("<d", struct.pack("<Q", (i * 0x9E3779B97F4A7C15) & (2**64 - 1)))[0] \
                    if i % 1000 else float("nan")
                row = {"amount": i - 50_000, "score": score, "status": i % 7, "label": label}
                expected[i + 1] = row
                rows.append((i + 1, row))
            self.assertEqual(table.upsert_from(rows), len(rows))
            table.flush()

        data_files = sorted(glob.glob(os.path.join(self.directory, "data-*.parquet")))
        self.assertEqual(len(data_files), 1)
        table_data = pq.read_table(data_files[0])
        self.assertEqual(table_data.schema.names, ["key_hash", "amount", "score", "status", "label"])
        self.assertEqual(table_data.num_rows, 100_000)
        keys = table_data.column("key_hash").to_pylist()
        amounts = table_data.column("amount").to_pylist()
        scores = table_data.column("score").to_pylist()
        statuses = table_data.column("status").to_pylist()
        labels = table_data.column("label").to_pylist()
        for index, key in enumerate(keys):
            want = expected[key]
            self.assertEqual(amounts[index], want["amount"])
            self.assertEqual(bits(scores[index]), bits(want["score"]))
            self.assertEqual(statuses[index], want["status"])
            self.assertEqual(labels[index], want["label"])

    def test_invalid_schema_is_rejected_by_native_open(self):
        with self.assertRaises(NodusError):
            LakeTable(self.directory, {"bad name": "int64"})


if __name__ == "__main__":
    unittest.main()
