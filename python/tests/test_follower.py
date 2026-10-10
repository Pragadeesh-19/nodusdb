import ctypes
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))

from nodusdb import (  # noqa: E402
    Follower,
    Graph,
    NodusChainTrustError,
    NodusError,
    NodusStaleReadError,
    NodusUnsupportedError,
    Shipping,
    Transaction,
    find_library,
    generate_signing_key,
    restore,
)

SCHEMA = """
schema 1
type user
type document {
  relation viewer: user
  permission view = viewer
}
"""

SCHEMA_TWO = """
schema 2
type user
type document {
  relation viewer: user
  relation owner: user
  permission view = viewer + owner
}
"""


def _native_has_follower_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    loaded = ctypes.CDLL(str(library))
    return hasattr(loaded, "nodus_open_follower") and hasattr(loaded, "nodus_restore")


@unittest.skipUnless(_native_has_follower_exports(), "libnodusdb lacks the follower exports; rebuild the native library")
class FollowerTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="nodus-follow-"))
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
        if graph.schema_version == 0:
            graph.apply_schema(SCHEMA)
        return graph

    def description(self, public_key=None, **settings):
        settings.setdefault("poll_interval_ms", 10)
        return Follower.directory(
            self.bucket, public_key_file=public_key or self.public_key, key_id=7, **settings)

    def follower(self, **settings):
        graph = Graph(follow=self.description(**settings))
        self.graphs.append(graph)
        return graph

    def test_a_follower_catches_up_and_answers_a_check_with_the_writers_token(self):
        writer = self.writer()
        token = writer.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        follower = self.follower()

        follower.wait_until_current()

        self.assertTrue(follower.check("document:a", "view", "user:alice", at_least=token))
        self.assertFalse(follower.check("document:a", "view", "user:bob", at_least=token))
        self.assertEqual(token, follower.token)
        self.assertEqual(1, follower.schema_version)

    def test_a_check_with_a_token_waits_for_the_follower_to_catch_up(self):
        writer = self.writer()
        follower = self.follower()
        writer.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        follower.wait_until_current()

        for index in range(10):
            token = writer.write(Transaction().add(f"document:n{index}", "viewer", "user:alice"),
                                 durability="lake")
            self.assertTrue(follower.check(f"document:n{index}", "view", "user:alice", at_least=token))

    def test_a_schema_migration_reaches_a_follower_that_is_already_open(self):
        writer = self.writer()
        follower = self.follower()
        writer.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        follower.wait_until_current()
        writer.apply_schema(SCHEMA_TWO)
        token = writer.write(Transaction().add("document:a", "owner", "user:bob"), durability="lake")

        self.assertTrue(follower.check("document:a", "view", "user:bob", at_least=token))
        self.assertEqual(2, follower.schema_version)

    def test_every_write_through_a_follower_is_refused(self):
        writer = self.writer()
        writer.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        follower = self.follower()
        follower.wait_until_current()

        with self.assertRaises(NodusUnsupportedError):
            follower.add_tuple("document:b", "viewer", "user:alice")
        with self.assertRaises(NodusUnsupportedError):
            follower.apply_schema(SCHEMA_TWO)
        with self.assertRaises(NodusUnsupportedError):
            follower.write(Transaction().add("document:b", "viewer", "user:alice"), durability="lake")
        with self.assertRaises(NodusUnsupportedError):
            follower.checkpoint()

    def test_reads_before_the_first_snapshot_are_stale(self):
        follower = self.follower()

        with self.assertRaises(NodusStaleReadError):
            follower.check("document:a", "view", "user:alice")
        self.assertEqual("BOOTSTRAPPING", follower.stats()["follower"]["phase"])

    def test_wait_until_current_times_out_when_there_is_no_snapshot(self):
        follower = self.follower()

        with self.assertRaises(NodusStaleReadError):
            follower.wait_until_current(timeout=0.2)

    def test_the_stats_document_describes_the_follower(self):
        writer = self.writer()
        token = writer.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        follower = self.follower()
        follower.wait_until_current()

        stats = follower.stats()

        self.assertEqual(token.lsn, stats["applied_lsn"])
        self.assertFalse(stats["shipping"]["configured"])
        self.assertEqual("CURRENT", stats["follower"]["phase"])
        self.assertGreaterEqual(stats["follower"]["objects_applied"], 1)
        self.assertEqual("", stats["follower"]["stall_reason"])

    def test_a_follower_that_does_not_trust_the_signer_stalls_and_says_why(self):
        writer = self.writer()
        writer.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        other_private = self.root / "other.pem"
        other_public = self.root / "other.pub"
        generate_signing_key(other_private, other_public)
        follower = Graph(follow=self.description(public_key=other_public))
        self.graphs.append(follower)

        with self.assertRaises(NodusError) as raised:
            follower.wait_until_current(timeout=10)

        self.assertIn("stalled", str(raised.exception))
        self.assertEqual("STALLED", follower.stats()["follower"]["phase"])

    def test_follow_cannot_be_combined_with_a_path_or_with_shipping(self):
        description = self.description()

        with self.assertRaises(ValueError):
            Graph(follow=description, path=self.root / "graph")
        with self.assertRaises(ValueError):
            Graph(follow=description, shipping=Shipping.directory(self.bucket, key_file=self.private_key, key_id=7))
        with self.assertRaises(TypeError):
            Graph(follow=42)

    def test_wait_until_current_is_only_for_followers(self):
        graph = Graph()
        self.graphs.append(graph)

        with self.assertRaises(NodusUnsupportedError):
            graph.wait_until_current()

    def test_a_restore_writes_a_directory_that_opens_as_the_writers_graph(self):
        writer = self.writer()
        for index in range(20):
            token = writer.write(Transaction().add(f"document:d{index}", "viewer", "user:alice"),
                                 durability="lake")
        target = self.root / "restored"

        restored = restore(self.description(), target)

        self.assertEqual(token.lsn, restored.applied_lsn)
        self.assertEqual(token.epoch, restored.epoch)
        self.assertGreaterEqual(restored.applied_lsn, restored.snapshot_lsn)
        copy = Graph(path=target, sync_mode="sync")
        self.graphs.append(copy)
        self.assertTrue(copy.check("document:d7", "view", "user:alice"))
        self.assertFalse(copy.check("document:d7", "view", "user:bob"))
        self.assertEqual(1, copy.schema_version)

    def test_a_restore_refuses_a_target_that_is_not_empty(self):
        writer = self.writer()
        writer.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        target = self.root / "occupied"
        target.mkdir()
        (target / "keep.txt").write_text("keep")

        with self.assertRaises(NodusUnsupportedError):
            restore(self.description(), target)

        self.assertEqual("keep", (target / "keep.txt").read_text())

    def test_a_restore_with_the_wrong_key_raises_the_chain_trust_error(self):
        writer = self.writer()
        writer.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        other_private = self.root / "other.pem"
        other_public = self.root / "other.pub"
        generate_signing_key(other_private, other_public)
        target = self.root / "restored"

        with self.assertRaises(NodusChainTrustError):
            restore(self.description(public_key=other_public), target)

        self.assertFalse(target.exists())

    def test_a_restore_needs_a_follower_description(self):
        with self.assertRaises(TypeError):
            restore({"store": {}}, self.root / "restored")


if __name__ == "__main__":
    unittest.main()
