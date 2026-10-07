import ctypes
import shutil
import sys
import tempfile
import threading
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import (  # noqa: E402
    Graph,
    NodusCheckDepthError,
    NodusError,
    NodusSchemaError,
    NodusStaleReadError,
    NodusTokenLostError,
    NodusUnsupportedError,
    Token,
    Transaction,
    find_library,
)

SCHEMA = """
schema 1
type user
type group {
  relation member: user | group#member
}
type folder {
  relation viewer: user | group#member
  relation parent: folder
  permission view = viewer + parent->view
}
type document {
  relation parent: folder
  relation editor: user | group#member
  relation viewer: user | group#member
  permission view = viewer + editor + parent->view
}
"""


def _native_has_authz_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    return hasattr(ctypes.CDLL(str(library)), "nodus_check")


@unittest.skipUnless(_native_has_authz_exports(), "libnodusdb lacks the typed exports; rebuild the native library")
class AuthzTest(unittest.TestCase):
    def setUp(self):
        self.graph = Graph()
        self.graph.apply_schema(SCHEMA)

    def tearDown(self):
        self.graph.close()

    def test_a_grant_is_visible_to_the_permission_that_includes_it(self):
        self.graph.add_tuple("document:readme", "viewer", "user:alice")

        self.assertTrue(self.graph.check("document:readme", "view", "user:alice"))
        self.assertFalse(self.graph.check("document:readme", "view", "user:bob"))
        self.assertFalse(self.graph.check("document:readme", "editor", "user:alice"))

    def test_group_membership_and_folder_inheritance_compose(self):
        self.graph.add_tuple("document:readme", "parent", "folder:eng")
        self.graph.add_tuple("folder:eng", "viewer", "group:staff", "member")
        self.graph.add_tuple("group:staff", "member", "user:carol")

        self.assertTrue(self.graph.check("document:readme", "view", "user:carol"))
        self.graph.remove_tuple("group:staff", "member", "user:carol")
        self.assertFalse(self.graph.check("document:readme", "view", "user:carol"))

    def test_a_transaction_commits_every_tuple_or_none(self):
        good = Transaction().add("document:a", "viewer", "user:u").add("document:b", "viewer", "user:u")
        bad = Transaction().add("document:c", "viewer", "user:u").add("document:d", "nothing", "user:u")

        token = self.graph.write(good)
        with self.assertRaises(NodusSchemaError):
            self.graph.write(bad)

        self.assertEqual(token, self.graph.token)
        self.assertTrue(self.graph.check("document:b", "view", "user:u"))
        self.assertFalse(self.graph.check("document:c", "view", "user:u"))

    def test_an_empty_transaction_returns_the_current_token(self):
        self.assertEqual(self.graph.token, self.graph.write(Transaction()))

    def test_tokens_order_writes_and_demand_freshness(self):
        first = self.graph.add_tuple("document:a", "viewer", "user:u")
        second = self.graph.add_tuple("document:b", "viewer", "user:u")

        self.assertIsInstance(first, Token)
        self.assertEqual(first.epoch, second.epoch)
        self.assertGreater(second.lsn, first.lsn)
        self.assertTrue(self.graph.check("document:b", "view", "user:u", at_least=second))
        with self.assertRaises(NodusStaleReadError):
            self.graph.check("document:b", "view", "user:u", at_least=Token(second.epoch, second.lsn + 100))

    def test_the_schema_moves_forward_one_version_at_a_time(self):
        self.assertEqual(1, self.graph.schema_version)

        self.graph.apply_schema(SCHEMA.replace("schema 1", "schema 2"))

        self.assertEqual(2, self.graph.schema_version)
        with self.assertRaises(NodusSchemaError):
            self.graph.apply_schema(SCHEMA)

    def test_violations_carry_the_native_message(self):
        with self.assertRaisesRegex(NodusSchemaError, "has no relation 'nothing'"):
            self.graph.add_tuple("document:a", "nothing", "user:u")
        with self.assertRaisesRegex(NodusSchemaError, "unknown type 'widget'"):
            self.graph.check("widget:a", "view", "user:u")

    def test_lake_durability_is_not_available_yet(self):
        with self.assertRaises(NodusUnsupportedError):
            self.graph.write(Transaction().add("document:a", "viewer", "user:u"), durability="lake")
        with self.assertRaises(ValueError):
            self.graph.write(Transaction(), durability="elsewhere")

    def test_a_deep_chain_raises_the_depth_error(self):
        for index in range(60):
            self.graph.add_tuple(f"group:g{index}", "member", f"group:g{index + 1}", "member")
        self.graph.add_tuple("group:g60", "member", "user:deep")
        self.graph.add_tuple("document:d", "viewer", "group:g0", "member")

        with self.assertRaises(NodusCheckDepthError):
            self.graph.check("document:d", "view", "user:deep")

    def test_typed_tuples_claim_string_keys(self):
        self.graph.add_tuple("document:a", "viewer", "user:u")

        with self.assertRaises(TypeError):
            self.graph.add_edge(1, 2)

    def test_arguments_are_type_checked(self):
        with self.assertRaises(TypeError):
            self.graph.add_tuple("document:a", "viewer", 7)
        with self.assertRaises(TypeError):
            self.graph.write("not a transaction")
        with self.assertRaises(ValueError):
            self.graph.check("document:a", "view", "user:u", at_least=Token(-1, 0))

    def test_readers_on_one_handle_run_in_parallel_with_a_writer(self):
        for index in range(50):
            self.graph.add_tuple(f"document:d{index}", "viewer", "user:alice")
        failures = []
        readers = 4
        writing = threading.Event()
        writing.set()
        started = threading.Barrier(readers + 1, timeout=60)

        def writer():
            try:
                started.wait()
                for round_number in range(300):
                    self.graph.add_tuple(f"document:w{round_number % 20}", "viewer", "user:bob")
                    self.graph.remove_tuple(f"document:w{round_number % 20}", "viewer", "user:bob")
            except Exception as error:
                failures.append(error)
            finally:
                writing.clear()

        def check_everything():
            for index in range(50):
                if not self.graph.check(f"document:d{index}", "view", "user:alice"):
                    raise AssertionError(f"alice lost access to document:d{index}")
                if self.graph.check(f"document:d{index}", "view", "user:mallory"):
                    raise AssertionError("mallory gained access")

        def reader():
            try:
                check_everything()
                started.wait()
                while writing.is_set():
                    check_everything()
            except Exception as error:
                failures.append(error)
                started.abort()

        threads = [threading.Thread(target=writer)] + [threading.Thread(target=reader) for _ in range(readers)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=120)

        self.assertFalse(any(thread.is_alive() for thread in threads), "a thread did not finish")
        self.assertEqual([], failures)


@unittest.skipUnless(_native_has_authz_exports(), "libnodusdb lacks the typed exports; rebuild the native library")
class DurableAuthzTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="nodus-authz-")

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def test_the_schema_and_tuples_survive_a_restart(self):
        with Graph(path=self.directory, sync_mode="sync") as graph:
            graph.apply_schema(SCHEMA)
            graph.add_tuple("document:readme", "parent", "folder:eng")
            graph.add_tuple("folder:eng", "viewer", "group:staff", "member")
            token = graph.add_tuple("group:staff", "member", "user:alice")

        with Graph(path=self.directory) as reopened:
            self.assertEqual(1, reopened.schema_version)
            self.assertTrue(reopened.check("document:readme", "view", "user:alice", at_least=token))
            self.assertFalse(reopened.check("document:readme", "view", "user:bob"))
            self.assertGreater(reopened.token.epoch, token.epoch)

    def test_a_token_from_a_write_that_never_became_durable_is_lost(self):
        with Graph(path=self.directory, sync_mode="sync") as graph:
            graph.apply_schema(SCHEMA)
            durable = graph.add_tuple("document:readme", "viewer", "user:alice")

        with Graph(path=self.directory) as reopened:
            self.assertTrue(reopened.check("document:readme", "view", "user:alice", at_least=durable))
            with self.assertRaises(NodusTokenLostError):
                reopened.check("document:readme", "view", "user:alice",
                               at_least=Token(durable.epoch, durable.lsn + 50))


if __name__ == "__main__":
    unittest.main()
