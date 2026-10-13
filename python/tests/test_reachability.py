import ctypes
import sys
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph, NodusError, Transaction, find_library  # noqa: E402

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

DEPTH = 40
SETTLE_SECONDS = 20


def _native_has_authz_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    return hasattr(ctypes.CDLL(str(library)), "nodus_check")


@unittest.skipUnless(_native_has_authz_exports(), "libnodusdb lacks the typed exports; rebuild the native library")
class ReachabilityTest(unittest.TestCase):
    def setUp(self):
        self.graph = Graph()
        self.graph.apply_schema(SCHEMA)

    def tearDown(self):
        self.graph.close()

    def nest(self, depth):
        transaction = Transaction()
        for index in range(depth):
            transaction.add(f"group:g{index}", "member", f"group:g{index + 1}", "member")
        transaction.add(f"group:g{depth}", "member", "user:alice")
        transaction.add("document:readme", "viewer", "group:g0", "member")
        self.graph.write(transaction)

    def settled_stats(self):
        deadline = time.monotonic() + SETTLE_SECONDS
        while True:
            reachability = self.graph.stats()["reachability"]
            if reachability["state"] == "current":
                return reachability
            self.assertLess(time.monotonic(), deadline, "the reachability index never settled")
            time.sleep(0.01)

    def test_a_chain_deeper_than_the_depth_limit_is_answered_through_the_index(self):
        self.nest(DEPTH)

        self.assertTrue(self.graph.check("document:readme", "view", "user:alice"))
        self.assertTrue(self.graph.check("group:g0", "member", "user:alice"))
        self.assertFalse(self.graph.check("document:readme", "view", "user:bob"))

    def test_removing_a_link_revokes_access_through_the_index(self):
        self.nest(DEPTH)
        self.assertTrue(self.graph.check("document:readme", "view", "user:alice"))

        self.graph.remove_tuple("group:g20", "member", "group:g21", "member")

        self.assertFalse(self.graph.check("document:readme", "view", "user:alice"))
        self.assertTrue(self.graph.check("group:g21", "member", "user:alice"))

    def test_stats_report_the_index(self):
        self.nest(5)
        self.graph.check("group:g0", "member", "user:alice")

        reachability = self.settled_stats()

        self.assertTrue(reachability["enabled"])
        self.assertEqual(0, reachability["dirty_nodes"])
        self.assertEqual(15, reachability["closure_entries"])
        self.assertGreater(reachability["incremental_updates"], 0)
        self.assertEqual("", reachability["last_error"])
        for key in ("entry_cap", "dirty_marks", "rebuilt_nodes", "refusals", "full_passes"):
            self.assertIn(key, reachability)


if __name__ == "__main__":
    unittest.main()
