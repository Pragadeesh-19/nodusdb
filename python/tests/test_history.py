import ctypes
import datetime
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import (  # noqa: E402
    Follower,
    Graph,
    NodusError,
    NodusHistoryExpiredError,
    NodusMemoryError,
    NodusUnsupportedError,
    PastGraph,
    Shipping,
    Token,
    Transaction,
    find_library,
    generate_signing_key,
)

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


def _native_has(*names):
    try:
        library = find_library()
    except NodusError:
        return False
    loaded = ctypes.CDLL(str(library))
    return all(hasattr(loaded, name) for name in names)


@unittest.skipUnless(_native_has("nodus_open_as_of", "nodus_past_advance", "nodus_open_follower"),
                     "libnodusdb lacks the as_of exports; rebuild the native library")
class AsOfTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="nodus-asof-"))
        self.bucket = self.root / "bucket"
        self.private_key = self.root / "signing.pem"
        self.public_key = self.root / "signing.pub"
        generate_signing_key(self.private_key, self.public_key)
        self.graphs = []

    def tearDown(self):
        for graph in reversed(self.graphs):
            graph.close()
        shutil.rmtree(self.root, ignore_errors=True)

    def writer(self, **settings):
        shipping = Shipping.directory(self.bucket, key_file=self.private_key, key_id=7,
                                      public_key_file=self.public_key, interval_ms=10, **settings)
        graph = Graph(path=self.root / "graph", sync_mode="sync", shipping=shipping)
        self.graphs.append(graph)
        graph.apply_schema(SCHEMA)
        return graph

    def grant(self, graph, document, user):
        return graph.write(Transaction().add(document, "viewer", user), durability="lake")

    def revoke(self, graph, document, user):
        return graph.write(Transaction().remove(document, "viewer", user), durability="lake")

    def test_a_view_at_a_token_answers_as_the_graph_did_then(self):
        graph = self.writer()
        granted = self.grant(graph, "document:plan", "user:alice")
        graph.checkpoint()
        revoked = self.revoke(graph, "document:plan", "user:alice")

        with graph.as_of(granted) as then, graph.as_of(revoked) as now:
            self.assertIsInstance(then, PastGraph)
            self.assertTrue(then.check("document:plan", "view", "user:alice"))
            self.assertFalse(now.check("document:plan", "view", "user:alice"))
            self.assertEqual(granted, then.token)
            self.assertEqual(revoked, now.token)
            self.assertIsInstance(then.commit_time, datetime.datetime)
            self.assertEqual(granted.lsn, then.stats()["as_of"]["lsn"])

    def test_a_view_at_a_time_stands_on_the_last_commit_before_it(self):
        graph = self.writer()
        before = self.grant(graph, "document:plan", "user:alice")
        time.sleep(0.01)
        moment = datetime.datetime.now(datetime.timezone.utc)
        time.sleep(0.01)
        self.grant(graph, "document:spec", "user:bob")

        with graph.as_of(moment) as view:
            self.assertEqual(before, view.token)
            self.assertLessEqual(view.commit_time, moment)
            self.assertTrue(view.check("document:plan", "view", "user:alice"))
            self.assertFalse(view.check("document:spec", "view", "user:bob"))

    def test_a_view_moves_forward_and_refuses_to_move_back(self):
        graph = self.writer()
        first = self.grant(graph, "document:plan", "user:alice")
        second = self.grant(graph, "document:spec", "user:bob")

        with graph.as_of(first) as view:
            self.assertEqual(second, view.advance(second))
            self.assertEqual(second, view.token)
            self.assertTrue(view.check("document:spec", "view", "user:bob"))
            with self.assertRaises(NodusError):
                view.advance(first)

    def test_a_view_is_read_only(self):
        graph = self.writer()
        token = self.grant(graph, "document:plan", "user:alice")

        with graph.as_of(token) as view:
            with self.assertRaises(NodusUnsupportedError):
                view.write(Transaction().add("document:x", "viewer", "user:bob"))
            with self.assertRaises(NodusUnsupportedError):
                view.erase("user:alice")
            with self.assertRaises(NodusUnsupportedError):
                view.changes(token)
            with self.assertRaises(NodusUnsupportedError):
                view.as_of(token)
            with self.assertRaises(TypeError):
                PastGraph()

    def test_history_before_the_newest_erasure_is_gone_and_a_view_forgets_a_name_erased_later(self):
        graph = self.writer()
        early = self.grant(graph, "document:plan", "user:carol")
        graph.erase("user:carol")
        graph.checkpoint()
        later = self.grant(graph, "document:spec", "user:alice")

        with self.assertRaises(NodusHistoryExpiredError):
            graph.as_of(early)
        with graph.as_of(later) as view:
            self.assertTrue(view.check("document:spec", "view", "user:alice"))
            graph.erase("user:alice")
            self.assertFalse(view.check("document:spec", "view", "user:alice"))

    def test_a_view_that_does_not_fit_its_limit_is_refused(self):
        graph = self.writer()
        token = None
        for chunk in range(5):
            transaction = Transaction()
            for user in range(12_000):
                transaction.add("document:plan", "viewer", f"user:u{chunk}-{user}")
            token = graph.write(transaction, durability="lake")

        with self.assertRaises(NodusMemoryError):
            graph.as_of(token, max_memory_mb=1)
        self.assertEqual(0, graph.stats()["history"]["used_bytes"])
        with graph.as_of(token) as view:
            self.assertTrue(view.check("document:plan", "view", "user:u4-11999"))

    def test_the_history_budget_is_configurable_and_reported(self):
        graph = self.writer(history_max_memory_mb=512)
        token = self.grant(graph, "document:plan", "user:alice")

        with graph.as_of(token):
            history = graph.stats()["history"]
            self.assertEqual(512 << 20, history["limit_bytes"])
            self.assertEqual(1, history["views"])
            self.assertGreater(history["used_bytes"], 0)

    def test_closing_the_graph_closes_its_views(self):
        graph = self.writer()
        token = self.grant(graph, "document:plan", "user:alice")
        view = graph.as_of(token)
        try:
            graph.close()
            self.graphs.remove(graph)
            with self.assertRaises(NodusError):
                view.check("document:plan", "view", "user:alice")
        finally:
            view.close()

    def test_targets_are_checked(self):
        graph = self.writer()
        with self.assertRaises(ValueError):
            graph.as_of(datetime.datetime.now())
        with self.assertRaises(TypeError):
            graph.as_of("yesterday")
        with self.assertRaises(ValueError):
            graph.as_of(Token(-1, 3))
        with self.assertRaises(NodusError):
            graph.as_of(datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1))

    def test_a_follower_opens_a_view_from_the_bucket_it_follows(self):
        graph = self.writer()
        granted = self.grant(graph, "document:plan", "user:alice")
        revoked = self.revoke(graph, "document:plan", "user:alice")
        follower = Graph(follow=Follower.directory(self.bucket, public_key_file=self.public_key, key_id=7,
                                                   poll_interval_ms=10, history_max_memory_mb=256))
        self.graphs.append(follower)
        follower.wait_until_current()
        self.assertFalse(follower.check("document:plan", "view", "user:alice", at_least=revoked))

        with follower.as_of(granted) as view:
            self.assertTrue(view.check("document:plan", "view", "user:alice"))
        self.assertEqual(256 << 20, follower.stats()["history"]["limit_bytes"])


@unittest.skipUnless(_native_has("nodus_open_as_of"), "libnodusdb lacks the as_of exports; rebuild the native library")
class AsOfWithoutShippingTest(unittest.TestCase):
    def test_a_graph_that_does_not_ship_cannot_travel_in_time(self):
        graph = Graph()
        try:
            graph.apply_schema(SCHEMA)
            token = graph.add_tuple("document:plan", "viewer", "user:alice")
            with self.assertRaises(NodusUnsupportedError):
                graph.as_of(token)
        finally:
            graph.close()


if __name__ == "__main__":
    unittest.main()
