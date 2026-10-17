import ctypes
import shutil
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import (  # noqa: E402
    AccessChange,
    AccessWatch,
    Follower,
    Graph,
    NodusError,
    NodusHistoryExpiredError,
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
  relation editor: user
  permission view = viewer + editor
}
"""

SCHEMA_WITHOUT_EDITORS = """
schema 2
type user
type group {
  relation member: user | group#member
}
type document {
  relation viewer: user | group#member
  relation editor: user
  permission view = viewer
}
"""

WAIT_SECONDS = 30


def _native_has(*names):
    try:
        library = find_library()
    except NodusError:
        return False
    loaded = ctypes.CDLL(str(library))
    return all(hasattr(loaded, name) for name in names)


HAS_WATCH = _native_has("nodus_changes", "nodus_watch_buffer")
HAS_FOLLOWER = _native_has("nodus_open_follower")


@unittest.skipUnless(HAS_WATCH, "libnodusdb lacks the watch exports; rebuild the native library")
class ChangesTest(unittest.TestCase):
    def setUp(self):
        self.graph = Graph()
        self.graph.apply_schema(SCHEMA)
        self.start = self.graph.changes(self.graph.token).next

    def tearDown(self):
        self.graph.close()

    def test_a_write_comes_back_with_its_names_and_its_token(self):
        token = self.graph.add_tuple("document:readme", "viewer", "group:eng", "member")

        batch = self.graph.changes(self.start)

        self.assertEqual(token, batch.next)
        self.assertEqual(1, len(batch.changes))
        change = batch.changes[0]
        self.assertEqual("add", change.kind)
        self.assertEqual("document:readme", change.object)
        self.assertEqual("viewer", change.relation)
        self.assertEqual("group:eng", change.subject)
        self.assertEqual("member", change.subject_relation)
        self.assertIsNone(change.detail)
        self.assertEqual(token, change.token)
        self.assertLess(change.lsn, token.lsn)
        self.assertIsInstance(change.token, Token)

    def test_a_transaction_arrives_whole_even_past_the_limit(self):
        token = self.graph.write(Transaction()
                                 .add("document:a", "viewer", "user:alice")
                                 .add("document:b", "viewer", "user:alice")
                                 .remove("document:a", "viewer", "user:alice"))

        batch = self.graph.changes(self.start, limit=1)

        self.assertEqual(["add", "add", "remove"], [change.kind for change in batch.changes])
        self.assertEqual({token}, {change.token for change in batch.changes})

    def test_a_caught_up_cursor_waits_and_returns_an_empty_batch(self):
        started = time.monotonic()
        batch = self.graph.changes(self.start, timeout=0.05)

        self.assertEqual((), batch.changes)
        self.assertEqual(self.start, batch.next)
        self.assertGreaterEqual(time.monotonic() - started, 0.04)

    def test_a_waiting_read_returns_when_a_write_commits(self):
        result = {}
        reader = threading.Thread(target=lambda: result.update(batch=self.graph.changes(
            self.start, timeout=WAIT_SECONDS)))
        reader.start()
        time.sleep(0.05)
        token = self.graph.add_tuple("document:readme", "viewer", "user:alice")
        reader.join(WAIT_SECONDS)

        self.assertFalse(reader.is_alive())
        self.assertEqual(token, result["batch"].next)

    def test_a_large_batch_is_read_through_bigger_buffers(self):
        names = [f"document:{'x' * 200}-{i}" for i in range(600)]
        for name in names:
            self.graph.add_tuple(name, "viewer", "user:alice")

        batch = self.graph.changes(self.start, limit=1000)

        self.assertEqual(names, [change.object for change in batch.changes])

    def test_schema_and_erase_changes_carry_their_details(self):
        self.graph.add_tuple("document:readme", "viewer", "user:alice")
        schema = self.graph.apply_schema(SCHEMA_WITHOUT_EDITORS)
        erased = self.graph.erase("user:alice")

        changes = self.graph.changes(self.start).changes

        schema_change = next(change for change in changes if change.kind == "schema")
        self.assertEqual(2, schema_change.schema_version)
        self.assertEqual(schema, schema_change.token)
        erase_change = changes[-1]
        self.assertEqual("erase", erase_change.kind)
        self.assertEqual(erased, erase_change.token)
        self.assertTrue(erase_change.detail.startswith("erased:"))
        self.assertFalse(any("alice" in str(field) for change in changes for field in change))
        self.assertEqual(erase_change.detail, changes[0].subject)

    def test_a_cursor_older_than_the_buffer_has_expired_on_an_in_memory_graph(self):
        graph = Graph()
        try:
            graph.apply_schema(SCHEMA)
            before = graph.token
            graph.add_tuple("document:readme", "viewer", "user:alice")
            with self.assertRaises(NodusHistoryExpiredError):
                graph.changes(before)
        finally:
            graph.close()

    def test_the_watch_generator_yields_batches_and_ends_on_close(self):
        tokens = [self.graph.add_tuple(f"document:{i}", "viewer", "user:alice") for i in range(3)]
        seen = []
        for batch in self.graph.watch(self.start, timeout=0.05):
            seen.extend(batch.changes)
            if len(seen) == 3:
                self.graph.close()
        self.assertEqual(tokens, [change.token for change in seen])
        self.graph = Graph()

    def test_arguments_are_checked(self):
        with self.assertRaises(ValueError):
            self.graph.changes(self.start, limit=0)
        with self.assertRaises(ValueError):
            self.graph.changes(self.start, timeout=-1)
        with self.assertRaises(TypeError):
            self.graph.changes("not a token")
        with self.assertRaises(TypeError):
            Graph(watch_buffer_changes="big")

    def test_an_invalid_buffer_size_is_refused_on_first_use(self):
        graph = Graph(watch_buffer_changes=1000)
        try:
            with self.assertRaises(NodusError):
                graph.changes(graph.token)
        finally:
            graph.close()

    def test_a_valid_buffer_size_is_reported(self):
        graph = Graph(watch_buffer_changes=1 << 16)
        try:
            graph.changes(graph.token)
            self.assertEqual(1 << 16, graph.stats()["watch"]["buffer_changes"])
        finally:
            graph.close()


@unittest.skipUnless(HAS_WATCH, "libnodusdb lacks the watch exports; rebuild the native library")
class IntegerChangesTest(unittest.TestCase):
    def test_an_integer_graph_reports_node_ids(self):
        graph = Graph()
        try:
            graph.add_edge(1, 2)
            start = graph.changes(graph.token).next
            graph.add_edge(3, 4)
            change = graph.changes(start).changes[0]
            self.assertEqual((3, 4), (change.object, change.subject))
            self.assertIsNone(change.relation)
        finally:
            graph.close()


@unittest.skipUnless(HAS_WATCH, "libnodusdb lacks the watch exports; rebuild the native library")
class AccessWatchTest(unittest.TestCase):
    def setUp(self):
        self.graph = Graph()
        self.graph.apply_schema(SCHEMA)
        self.graph.changes(self.graph.token)
        self.graph.add_tuple("group:eng", "member", "user:alice")
        self.graph.add_tuple("document:plan", "viewer", "group:eng", "member")
        self.graph.add_tuple("document:readme", "editor", "user:bob")
        self.access = AccessWatch(self.graph, [
            ("document:plan", "view", "user:alice"),
            ("document:readme", "view", "user:bob"),
            ("document:readme", "view", "user:carol"),
        ])

    def tearDown(self):
        self.graph.close()

    def test_the_baseline_is_not_reported(self):
        self.assertEqual([], self.access.poll(timeout=0.01))
        self.assertTrue(self.access.allowed("document:plan", "view", "user:alice"))
        self.assertFalse(self.access.allowed("document:readme", "view", "user:carol"))

    def test_leaving_a_group_reports_lost_access_through_the_group(self):
        token = self.graph.remove_tuple("group:eng", "member", "user:alice")

        changed = self.access.poll()

        self.assertEqual([AccessChange("document:plan", "view", "user:alice", False, token)], changed)
        self.assertEqual(token, self.access.token)

    def test_a_grant_reports_gained_access(self):
        token = self.graph.add_tuple("document:readme", "viewer", "user:carol")

        self.assertEqual([AccessChange("document:readme", "view", "user:carol", True, token)], self.access.poll())

    def test_a_schema_change_that_drops_a_path_reports_lost_access(self):
        token = self.graph.apply_schema(SCHEMA_WITHOUT_EDITORS)

        self.assertEqual([AccessChange("document:readme", "view", "user:bob", False, token)], self.access.poll())

    def test_a_change_undone_inside_one_batch_is_not_reported(self):
        self.graph.add_tuple("document:readme", "viewer", "user:carol")
        self.graph.remove_tuple("document:readme", "viewer", "user:carol")

        self.assertEqual([], self.access.poll())

    def test_registrations_are_checked(self):
        with self.assertRaises(ValueError):
            AccessWatch(self.graph, [])
        with self.assertRaises(TypeError):
            AccessWatch(self.graph, [("document:plan", "view")])
        with self.assertRaises(ValueError):
            AccessWatch(self.graph, [("document:plan", "view", "user:alice")] * 2)


@unittest.skipUnless(HAS_WATCH and HAS_FOLLOWER, "libnodusdb lacks the watch or follower exports")
class FollowerWatchTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="nodus-watch-"))
        self.bucket = self.root / "bucket"
        self.private_key = self.root / "signing.pem"
        self.public_key = self.root / "signing.pub"
        generate_signing_key(self.private_key, self.public_key)
        self.graphs = []

    def tearDown(self):
        for graph in reversed(self.graphs):
            graph.close()
        shutil.rmtree(self.root, ignore_errors=True)

    def writer(self):
        shipping = Shipping.directory(
            self.bucket, key_file=self.private_key, key_id=7, public_key_file=self.public_key, interval_ms=10)
        graph = Graph(path=self.root / "graph", sync_mode="sync", shipping=shipping)
        self.graphs.append(graph)
        return graph

    def follower(self):
        graph = Graph(follow=Follower.directory(self.bucket, public_key_file=self.public_key, key_id=7,
                                                poll_interval_ms=10))
        self.graphs.append(graph)
        return graph

    def test_a_revocation_on_the_writer_reaches_an_access_watch_on_a_follower(self):
        writer = self.writer()
        writer.apply_schema(SCHEMA)
        granted = writer.write(Transaction().add("document:plan", "viewer", "user:alice"), durability="lake")
        follower = self.follower()
        follower.wait_until_current()
        self.assertTrue(follower.check("document:plan", "view", "user:alice", at_least=granted))
        access = AccessWatch(follower, [("document:plan", "view", "user:alice")])

        revoked = writer.write(Transaction().remove("document:plan", "viewer", "user:alice"), durability="lake")
        changed = []
        deadline = time.monotonic() + WAIT_SECONDS
        while not changed and time.monotonic() < deadline:
            changed = access.poll(timeout=1)

        self.assertEqual([AccessChange("document:plan", "view", "user:alice", False, revoked)], changed)

    def test_a_writer_that_restarts_serves_old_cursors_from_the_bucket(self):
        writer = self.writer()
        writer.apply_schema(SCHEMA)
        start = writer.token
        first = writer.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        writer.close()
        self.graphs.remove(writer)
        reopened = self.writer()

        batch = reopened.changes(start)

        self.assertEqual(first, batch.changes[0].token)
        self.assertEqual("document:a", batch.changes[0].object)
        self.assertIsNotNone(batch.changes[0].commit_time)


if __name__ == "__main__":
    unittest.main()
