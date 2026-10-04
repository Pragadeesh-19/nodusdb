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
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    pa = None
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

    @unittest.skipIf(pq is None, "pyarrow is required for the independent Parquet read")
    def test_uncompressed_files_read_back_with_pyarrow(self):
        with LakeTable(self.directory, SCHEMA, flush_rows=1 << 20, compression="none") as table:
            table.upsert_table(_arrow_table(1_000), "id")
            table.flush()

        data_file = glob.glob(os.path.join(self.directory, "data-*.parquet"))[0]
        self.assertEqual(pq.ParquetFile(data_file).metadata.row_group(0).column(0).compression, "UNCOMPRESSED")
        read_back = pq.read_table(data_file)
        self.assertEqual(read_back.column("amount").to_pylist()[10], 10 * 3 - 7)
        self.assertEqual(read_back.column("label").to_pylist()[1], "alpha")

    def test_unknown_compression_is_rejected_before_opening(self):
        with self.assertRaises(ValueError):
            LakeTable(self.directory, SCHEMA, compression="zstd")

    def test_invalid_schema_is_rejected_by_native_open(self):
        with self.assertRaises(NodusError):
            LakeTable(self.directory, {"bad name": "int64"})

    @unittest.skipIf(pa is None, "pyarrow is required for columnar batches")
    def test_columnar_batch_matches_row_upserts(self):
        count = 20_000
        table = _arrow_table(count)
        rows = table.to_pylist()
        by_row = os.path.join(self.directory, "by-row")
        by_column = os.path.join(self.directory, "by-column")
        with LakeTable(by_row, SCHEMA) as row_table, LakeTable(by_column, SCHEMA) as column_table:
            self.assertEqual(row_table.upsert_from((row["id"], _row_values(row)) for row in rows), count)
            self.assertEqual(column_table.upsert_table(table, "id"), count)
            for key in (1, 2, 500, 19_999, count):
                self.assertEqual(column_table.get(key), row_table.get(key))
            self.assertEqual(column_table.sum("amount"), row_table.sum("amount"))
            self.assertAlmostEqual(column_table.average("score"), row_table.average("score"))

    @unittest.skipIf(pa is None, "pyarrow is required for columnar batches")
    def test_columnar_batch_rejects_nulls_and_wrong_types_without_writing(self):
        batch = _arrow_table(3).to_batches()[0]
        with LakeTable(self.directory, SCHEMA) as lake:
            with self.assertRaises(ValueError):
                lake.upsert_columns(pa.array([1, None, 3], pa.int64()), _columns(batch))
            with self.assertRaises(TypeError):
                lake.upsert_columns(batch.column("id"), {**_columns(batch), "amount": batch.column("status")})
            self.assertIsNone(lake.get(1))

    @unittest.skipIf(pa is None, "pyarrow is required for columnar batches")
    def test_sliced_arrays_respect_their_offsets(self):
        window = _arrow_table(10).slice(4, 3)
        with LakeTable(self.directory, SCHEMA) as lake:
            self.assertEqual(lake.upsert_table(window, "id"), 3)
            self.assertEqual(lake.get(5)["label"], "alpha")
            self.assertEqual(lake.get(6)["amount"], 5 * 3 - 7)
            self.assertIsNone(lake.get(4))
            self.assertIsNone(lake.get(8))

    def test_sum_and_average_cover_rows_in_the_active_buffer(self):
        with LakeTable(self.directory, SCHEMA, flush_rows=1 << 20) as table:
            self.assertIsNone(table.average("score"))
            for key in range(1, 101):
                table.upsert(key, {"amount": key, "score": float(key), "status": 1, "label": "x"})
            table.delete(5)
            self.assertEqual(table.sum("amount"), sum(range(1, 101)) - 5)
            self.assertAlmostEqual(table.average("score"), (sum(range(1, 101)) - 5) / 99)
            with self.assertRaises(TypeError):
                table.sum("label")

    @unittest.skipIf(pq is None, "pyarrow is required for the independent Parquet read")
    def test_pyarrow_reads_dictionary_snappy_files_with_statistics_and_row_groups(self):
        count = 300_000
        source = _arrow_table(count)
        with LakeTable(self.directory, SCHEMA, flush_rows=1 << 20) as table:
            table.upsert_table(source, "id")
            table.flush()

        data_file = glob.glob(os.path.join(self.directory, "data-*.parquet"))[0]
        parquet = pq.ParquetFile(data_file)
        self.assertEqual(parquet.metadata.num_row_groups, 3)
        first_group = parquet.metadata.row_group(0)
        label_column = first_group.column(4)
        self.assertEqual(label_column.compression, "SNAPPY")
        self.assertIn("RLE_DICTIONARY", label_column.encodings)
        labels = source.column("label").to_pylist()
        self.assertTrue(label_column.statistics.has_min_max)
        self.assertEqual(label_column.statistics.min, min(labels[:122_880]))
        self.assertEqual(label_column.statistics.max, max(labels[:122_880]))
        amount_column = first_group.column(1)
        self.assertTrue(amount_column.statistics.has_min_max)
        self.assertEqual(amount_column.statistics.min, -7)

        read_back = pq.read_table(data_file)
        self.assertEqual(read_back.num_rows, count)
        keys = read_back.column("key_hash").to_pylist()
        amounts = read_back.column("amount").to_pylist()
        read_labels = read_back.column("label").to_pylist()
        for index in (0, 1, 122_879, 122_880, count - 1):
            key = keys[index]
            self.assertEqual(amounts[index], (key - 1) * 3 - 7)
            self.assertEqual(read_labels[index], labels[key - 1])


def _arrow_table(count):
    ids = range(1, count + 1)
    return pa.table({
        "id": pa.array(ids, pa.int64()),
        "amount": pa.array([i * 3 - 7 for i in range(count)], pa.int64()),
        "score": pa.array([i * 0.5 for i in range(count)], pa.float64()),
        "status": pa.array([i % 9 for i in range(count)], pa.int32()),
        "label": pa.array([f"label-{i}" if i % 3 == 0 else ("alpha" if i % 3 == 1 else "gamma")
                           for i in range(count)], pa.string()),
    })


def _columns(batch):
    return {name: batch.column(name) for name in SCHEMA}


def _row_values(row):
    return {name: row[name] for name in SCHEMA}


if __name__ == "__main__":
    unittest.main()
