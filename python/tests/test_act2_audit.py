import ctypes
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

from act2 import audit, fixture, lake  # noqa: E402
from nodusdb import Graph, NodusError, Shipping, find_library, generate_signing_key  # noqa: E402

try:
    import duckdb
except ImportError:
    duckdb = None

REQUIRED = os.environ.get("NODUS_REQUIRE_PYTHON_VERIFIERS") is not None
WAIT_SECONDS = 60
DOCUMENT = "document:doc"
USERS = ["user:direct", "user:leaver", "user:nested", "user:outsider"]
HISTORY = [
    ("folder:root", "viewer", "group:staff", "member"),
    ("folder:child", "parent", "folder:root", None),
    ("folder:root", "parent", "folder:child", None),
    ("document:doc", "parent", "folder:child", None),
    ("document:doc", "viewer", "user:direct", None),
    ("group:staff", "member", "group:inner", "member"),
    ("group:inner", "member", "group:staff", "member"),
    ("group:inner", "member", "user:nested", None),
    ("group:staff", "member", "user:leaver", None),
    ("document:other", "viewer", "user:outsider", None),
]
LEAVER = ("group:staff", "member", "user:leaver", None)


def _native_has_shipping_exports():
    try:
        return hasattr(ctypes.CDLL(str(find_library())), "nodus_open_durable_shipping")
    except NodusError:
        return False


@unittest.skipUnless(_native_has_shipping_exports(), "libnodusdb lacks the shipping exports; rebuild the native library")
class AuditTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if duckdb is None:
            if REQUIRED:
                raise AssertionError("duckdb is not installed and NODUS_REQUIRE_PYTHON_VERIFIERS is set")
            raise unittest.SkipTest("duckdb is not installed")
        cls.root = Path(tempfile.mkdtemp(prefix="act2-audit-"))
        generate_signing_key(cls.root / "signing.pem", cls.root / "signing.pub")
        shipping = Shipping.directory(cls.root / "bucket", key_file=cls.root / "signing.pem", key_id=1,
                                      interval_ms=10, iceberg=True, iceberg_commit_interval_s=1)
        cls.graph = Graph(path=cls.root / "graph", sync_mode="sync", shipping=shipping)
        cls.graph.apply_schema(fixture.read_fixture().schema)
        for tuple_ in HISTORY:
            cls.graph.add_tuple(*tuple_)
        cls.before = cls.graph.token
        cls.after = cls.graph.remove_tuple(*LEAVER)
        cls.objects = lake.DirectoryObjects(cls.root / "bucket")
        cls.location = cls.objects.location(cls._await_lake(cls.after.lsn).key)
        cls.connection = duckdb.connect()
        cls.connection.execute("INSTALL iceberg")
        cls.connection.execute("LOAD iceberg")

    @classmethod
    def tearDownClass(cls):
        cls.graph.close()
        shutil.rmtree(cls.root, ignore_errors=True)

    @classmethod
    def _await_lake(cls, lsn):
        deadline = time.monotonic() + WAIT_SECONDS
        while True:
            version = lake.newest_version(cls.objects)
            if version is not None and version.projected_lsn >= lsn:
                return version
            if time.monotonic() > deadline:
                raise AssertionError(f"the lake never reached LSN {lsn}")
            time.sleep(0.05)

    def test_as_of_and_sql_agree_through_nested_groups_parent_chains_cycles_and_a_remove(self):
        expected = {self.before: {"user:direct", "user:leaver", "user:nested"},
                    self.after: {"user:direct", "user:nested"}}
        for token, viewers in expected.items():
            with self.subTest(lsn=token.lsn):
                instant = audit.audit(self.graph, self.connection, self.location, "instant", token, DOCUMENT,
                                           USERS)
                self.assertEqual(viewers, instant.past.viewers)
                self.assertEqual(viewers, instant.lake.viewers)
                self.assertEqual(len(USERS), instant.past.checks)
                self.assertGreaterEqual(instant.past.replay["replayed_records"], 0)

    def test_answers_that_disagree_fail_the_audit(self):
        past = audit.PastAnswer(frozenset({"user:a", "user:b"}), 1, 1, 2, {})
        answered = audit.LakeAnswer(frozenset({"user:a", "user:c"}), 1)
        with self.assertRaisesRegex(audit.AuditError, r"only as_of: \['user:b'\], only SQL: \['user:c'\]"):
            audit.require_agreement(audit.Instant("instant", 7, past, answered))

    def test_a_revoked_user_who_does_not_flip_fails_the_audit(self):
        def instant(viewers):
            return audit.Instant("instant", 7, audit.PastAnswer(frozenset(viewers), 1, 1, 1, {}),
                                 audit.LakeAnswer(frozenset(viewers), 1))
        with self.assertRaisesRegex(audit.AuditError, "could not view before"):
            audit.require_flip(instant([]), instant([]), "user:leaver")
        with self.assertRaisesRegex(audit.AuditError, "could still view after"):
            audit.require_flip(instant(["user:leaver"]), instant(["user:leaver"]), "user:leaver")


if __name__ == "__main__":
    unittest.main()
