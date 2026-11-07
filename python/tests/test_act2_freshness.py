import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))
sys.path.insert(0, str(PYTHON_DIRECTORY / "benchmarks"))

from act2 import freshness, lake  # noqa: E402

MILLIS = 1_000_000


def observed(seen_ms, lsn, number=None):
    return freshness.Observation(seen_ms * MILLIS, lake.TableVersion(number or lsn, f"v{number or lsn}", lsn))


def write_metadata(root, number, lsn):
    directory = root / "iceberg" / "metadata"
    directory.mkdir(parents=True, exist_ok=True)
    metadata = {"current-snapshot-id": number,
                "snapshots": [{"snapshot-id": number, "summary": {lake.PROJECTED_LSN: str(lsn)}}]}
    (directory / f"v{number}.metadata.json").write_text(json.dumps(metadata), encoding="utf-8")


class FreshnessTest(unittest.TestCase):
    def test_each_write_waits_until_the_first_version_that_holds_it(self):
        acknowledged = [(3, 0), (5, 400 * MILLIS), (9, 900 * MILLIS)]
        observations = [observed(1_000, 5), observed(61_000, 9), observed(30_000, 7)]

        self.assertEqual([1_000 * MILLIS, 600 * MILLIS, 60_100 * MILLIS],
                         freshness.freshness_ns(acknowledged, observations))

    def test_a_version_seen_before_the_acknowledgement_counts_as_no_wait(self):
        self.assertEqual([0], freshness.freshness_ns([(7, 50 * MILLIS)], [observed(40, 7)]))

    def test_a_write_no_version_ever_holds_fails(self):
        with self.assertRaisesRegex(freshness.FreshnessError, "LSN 8"):
            freshness.freshness_ns([(3, 0), (8, 10)], [observed(5, 3)])

    def test_a_table_that_goes_back_fails(self):
        with self.assertRaisesRegex(freshness.FreshnessError, "went back from LSN 9 to 4"):
            freshness.freshness_ns([(3, 0)], [observed(5, 9, 1), observed(6, 4, 2)])

    def test_the_spread_uses_the_nearest_rank(self):
        spread = freshness.Spread.of([value * MILLIS for value in range(100, 0, -1)])

        self.assertEqual((100, 1, 50, 99, 100), (spread.count, spread.min_ns // MILLIS, spread.p50_ns // MILLIS,
                                                 spread.p99_ns // MILLIS, spread.max_ns // MILLIS))


class WatcherTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="act2-watcher-"))
        self.watcher = freshness.Watcher(lake.DirectoryObjects(self.root), poll_ns=MILLIS).start()

    def tearDown(self):
        self.watcher.stop()
        shutil.rmtree(self.root, ignore_errors=True)

    def test_the_reader_reports_each_new_version_once_and_waits_for_a_position(self):
        write_metadata(self.root, 1, 3)
        self.assertEqual(3, self.watcher.await_lsn(3, timeout_s=10).version.projected_lsn)
        with self.assertRaisesRegex(freshness.FreshnessError, "no table version holds LSN 4"):
            self.watcher.await_lsn(4, timeout_s=0.2)

        write_metadata(self.root, 2, 5)

        self.assertEqual(2, self.watcher.await_lsn(4, timeout_s=10).version.number)
        self.assertEqual([1, 2], [observation.version.number for observation in self.watcher.observations()])
        self.assertTrue(self.watcher.reads())


if __name__ == "__main__":
    unittest.main()
