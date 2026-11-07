import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))
sys.path.insert(0, str(PYTHON_DIRECTORY / "benchmarks"))

from act2 import lake  # noqa: E402


class LakeTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="act2-lake-"))
        self.metadata = self.root / "iceberg" / "metadata"
        self.metadata.mkdir(parents=True)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def write_version(self, number, snapshots, current):
        metadata = {"current-snapshot-id": current,
                    "snapshots": [{"snapshot-id": snapshot, "summary": {lake.PROJECTED_LSN: str(lsn)}}
                                  for snapshot, lsn in snapshots]}
        (self.metadata / f"v{number}.metadata.json").write_text(json.dumps(metadata), encoding="utf-8")

    def test_the_newest_version_is_chosen_by_number_not_by_name(self):
        for number in (9, 10, 2):
            self.write_version(number, [(number, number * 100)], number)
        (self.metadata / "version-hint.text").write_text("9", encoding="utf-8")
        (self.metadata / "snap-1-m0.avro").write_bytes(b"")

        newest = lake.newest_version(lake.DirectoryObjects(self.root))

        self.assertEqual((10, "iceberg/metadata/v10.metadata.json", 1000),
                         (newest.number, newest.key, newest.projected_lsn))

    def test_the_position_comes_from_the_current_snapshot_not_the_last_listed(self):
        self.write_version(1, [(6, 10), (7, 40), (8, 25)], 7)

        self.assertEqual(40, lake.newest_version(lake.DirectoryObjects(self.root)).projected_lsn)

    def test_a_table_with_no_committed_version_has_no_newest(self):
        self.assertIsNone(lake.newest_version(lake.DirectoryObjects(self.root)))
        self.assertIsNone(lake.newest_version(lake.DirectoryObjects(self.root / "absent")))


if __name__ == "__main__":
    unittest.main()
