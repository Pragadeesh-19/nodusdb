import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))
sys.path.insert(0, str(PYTHON_DIRECTORY / "benchmarks"))

from act2 import bucket, lake  # noqa: E402


class Paginator:
    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **_):
        return iter(self._pages)


class Client:
    def __init__(self, status=None, versions=(), markers=()):
        self._status = status
        self._pages = [{"Versions": [{"Key": key, "IsLatest": latest} for key, latest in versions],
                        "DeleteMarkers": [{"Key": key} for key in markers]}]

    def get_bucket_versioning(self, Bucket):
        return {"Status": self._status} if self._status else {}

    def get_paginator(self, name):
        assert name == "list_object_versions", name
        return Paginator(self._pages)


class BucketTest(unittest.TestCase):
    def test_a_bucket_that_never_kept_versions_is_accepted(self):
        self.assertIsNone(bucket.require_never_versioned(Client(), "b"))

    def test_a_bucket_with_versioning_enabled_or_suspended_is_refused(self):
        for status in ("Enabled", "Suspended"):
            with self.subTest(status=status), self.assertRaisesRegex(bucket.BucketError, status):
                bucket.require_never_versioned(Client(status=status), "b")

    def test_only_latest_versions_leave_nothing_behind(self):
        found = bucket.require_no_leftovers(Client(versions=[("p/a", True), ("p/b", True)]), "b", "p")

        self.assertEqual((), found.noncurrent_versions + found.delete_markers)

    def test_an_old_version_or_a_delete_marker_is_refused(self):
        for client in (Client(versions=[("p/a", True), ("p/a", False)]), Client(markers=["p/gone"])):
            with self.subTest(), self.assertRaisesRegex(bucket.BucketError, "old versions"):
                bucket.require_no_leftovers(client, "b", "p")

    def test_a_prefix_that_already_holds_objects_is_refused(self):
        root = Path(tempfile.mkdtemp(prefix="act2-bucket-"))
        try:
            bucket.require_empty(lake.DirectoryObjects(root))
            (root / "_nodus").mkdir()
            (root / "_nodus" / "earlier.obj").write_bytes(b"x")
            with self.assertRaisesRegex(bucket.BucketError, "already holds 1 objects"):
                bucket.require_empty(lake.DirectoryObjects(root))
        finally:
            shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
