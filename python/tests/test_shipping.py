import ctypes
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))

from nodusdb import (  # noqa: E402
    Graph,
    NodusError,
    NodusLogBacklogError,
    NodusShipTimeoutError,
    NodusUnsupportedError,
    NodusWriterFencedError,
    Shipping,
    Token,
    Transaction,
    find_library,
    generate_signing_key,
    static_credentials,
)

SCHEMA = """
schema 1
type user
type document {
  relation viewer: user
  permission view = viewer
}
"""

LAKE_WRITES = 25
BACKLOG_CAP = 1 << 20
BATCH_TUPLES = 2_000

CHILD = f"""
import sys
from nodusdb import Graph, Shipping, Transaction
shipping = Shipping.directory(sys.argv[2], key_file=sys.argv[3], key_id=7, interval_ms=10)
graph = Graph(path=sys.argv[1], sync_mode="sync", shipping=shipping)
graph.apply_schema({SCHEMA!r})
for i in range({LAKE_WRITES}):
    token = graph.write(Transaction().add(f"document:d{{i}}", "viewer", "user:alice"), durability="lake")
    print(i, token.epoch, token.lsn, flush=True)
"""


def _native_has_shipping_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    return hasattr(ctypes.CDLL(str(library)), "nodus_open_durable_shipping")


def _chain_objects(bucket):
    chain = Path(bucket) / "_nodus" / "chain"
    return sorted(chain.glob("*.obj")) if chain.is_dir() else []


@unittest.skipUnless(_native_has_shipping_exports(), "libnodusdb lacks the shipping exports; rebuild the native library")
class ShippingTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="nodus-ship-"))
        self.graph_path = self.root / "graph"
        self.bucket = self.root / "bucket"
        self.private_key = self.root / "signing.pem"
        self.public_key = self.root / "signing.pub"
        generate_signing_key(self.private_key, self.public_key)
        self.graphs = []

    def tearDown(self):
        for graph in self.graphs:
            graph.close()
        shutil.rmtree(self.root, ignore_errors=True)

    def shipping(self, **settings):
        settings.setdefault("interval_ms", 10)
        return Shipping.directory(
            self.bucket, key_file=self.private_key, key_id=7, public_key_file=self.public_key, **settings)

    def open(self, **settings):
        graph = Graph(path=self.graph_path, sync_mode="sync", shipping=self.shipping(**settings))
        self.graphs.append(graph)
        if graph.schema_version == 0:
            graph.apply_schema(SCHEMA)
        return graph

    def close(self, graph):
        graph.close()
        self.graphs.remove(graph)

    def break_store(self):
        deadline = time.monotonic() + 10
        while True:
            shutil.rmtree(self.bucket, ignore_errors=True)
            try:
                self.bucket.write_text("not a directory")
                return
            except OSError:
                self.assertLess(time.monotonic(), deadline, "the bucket directory could not be replaced")
                time.sleep(0.01)

    def test_a_lake_write_is_in_the_bucket_when_it_returns(self):
        graph = self.open()

        token = graph.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")

        self.assertTrue(_chain_objects(self.bucket))
        shipping = graph.stats()["shipping"]
        self.assertTrue(shipping["configured"])
        self.assertGreaterEqual(shipping["shipped_lsn"], token.lsn)
        self.assertEqual(0, shipping["lag_lsn"])
        self.assertTrue(graph.check("document:a", "view", "user:alice", at_least=token))

    def test_a_local_write_can_be_awaited_later(self):
        graph = self.open()

        token = graph.add_tuple("document:a", "viewer", "user:alice")
        graph.wait_shipped(token, timeout=20)

        self.assertGreaterEqual(graph.stats()["shipping"]["shipped_lsn"], token.lsn)

    def test_an_empty_lake_transaction_waits_for_the_current_position(self):
        graph = self.open()
        graph.add_tuple("document:a", "viewer", "user:alice")

        token = graph.write(Transaction(), durability="lake")

        self.assertGreaterEqual(graph.stats()["shipping"]["shipped_lsn"], token.lsn)

    def test_stats_report_the_shape_an_operator_needs(self):
        graph = self.open()
        graph.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")

        stats = graph.stats()

        self.assertEqual({"epoch", "applied_lsn", "shipping"}, set(stats))
        self.assertEqual("ACTIVE", stats["shipping"]["phase"])
        for key in ("shipped_lsn", "lag_lsn", "chain_seq", "backlog_bytes", "backlog_cap_bytes", "last_error",
                    "snapshot_references", "retention", "projection", "events"):
            self.assertIn(key, stats["shipping"])
        self.assertEqual("DISABLED", stats["shipping"]["projection"]["phase"])
        self.assertEqual(stats["epoch"], stats["shipping"]["epoch"])

    def test_stats_without_shipping_say_so(self):
        with Graph() as graph:
            stats = graph.stats()

        self.assertEqual({"configured": False}, stats["shipping"])

    def test_a_restart_continues_the_chain_in_a_new_epoch(self):
        graph = self.open()
        first = graph.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        self.close(graph)

        reopened = self.open()
        second = reopened.write(Transaction().add("document:b", "viewer", "user:alice"), durability="lake")

        self.assertGreater(second.epoch, first.epoch)
        self.assertGreater(second.lsn, first.lsn)
        self.assertTrue(reopened.check("document:a", "view", "user:alice"))
        self.assertTrue(reopened.check("document:b", "view", "user:alice"))

    def test_closing_ships_what_was_only_acknowledged_locally(self):
        graph = self.open(interval_ms=3_600_000)
        graph.wait_shipped(graph.token, timeout=20)
        before = len(_chain_objects(self.bucket))
        for index in range(30):
            graph.add_tuple(f"document:d{index}", "viewer", "user:alice")

        self.close(graph)

        self.assertGreater(len(_chain_objects(self.bucket)), before)

    def test_lake_durability_without_shipping_is_refused_and_writes_nothing(self):
        with Graph(path=self.graph_path, sync_mode="sync") as graph:
            graph.apply_schema(SCHEMA)
            before = graph.token

            with self.assertRaisesRegex(NodusUnsupportedError, "shipping is not configured"):
                graph.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")

            self.assertEqual(before, graph.token)
            self.assertFalse(graph.check("document:a", "view", "user:alice"))

    def test_shipping_needs_a_durable_graph(self):
        with self.assertRaisesRegex(ValueError, "durable"):
            Graph(shipping=self.shipping())

    def test_shipping_must_be_a_shipping_or_a_json_document(self):
        with self.assertRaises(TypeError):
            Graph(path=self.graph_path, shipping=42)

    def test_a_json_document_is_accepted_as_is(self):
        document = self.shipping().to_json()

        with Graph(path=self.graph_path, sync_mode="sync", shipping=document) as graph:
            graph.apply_schema(SCHEMA)
            token = graph.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")

            self.assertGreaterEqual(graph.stats()["shipping"]["shipped_lsn"], token.lsn)

    def test_a_bad_setting_is_reported_by_name_and_opens_nothing(self):
        with self.assertRaisesRegex(NodusError, "interval_ms"):
            Graph(path=self.graph_path, shipping=self.shipping(interval_ms=1))
        with self.assertRaisesRegex(NodusError, "absent.pem"):
            Graph(path=self.graph_path, shipping=Shipping.directory(
                self.bucket, key_file=self.root / "absent.pem", key_id=7))

        self.assertFalse(_chain_objects(self.bucket))

    def test_secrets_never_appear_in_an_error(self):
        shipping = Shipping.s3(
            "graphs", "us-east-1", key_file=self.private_key, key_id=7,
            credentials=static_credentials("AKIAEXAMPLE", "wJalrSECRETKEYexample"), backlog_cap_bytes=1)

        with self.assertRaises(NodusError) as raised:
            Graph(path=self.graph_path, shipping=shipping)

        self.assertNotIn("wJalrSECRETKEYexample", str(raised.exception))
        self.assertNotIn("AKIAEXAMPLE", str(raised.exception))

    def test_a_wait_that_times_out_names_the_write(self):
        graph = self.open()
        graph.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        self.break_store()
        token = graph.add_tuple("document:b", "viewer", "user:alice")

        with self.assertRaises(NodusShipTimeoutError) as raised:
            graph.wait_shipped(token, timeout=0.3)

        self.assertEqual(token, raised.exception.token)
        self.assertIsInstance(raised.exception.token, Token)
        self.assertGreater(graph.stats()["shipping"]["lag_lsn"], 0)

    def test_a_lake_write_that_times_out_is_still_applied(self):
        graph = self.open()
        self.break_store()

        with self.assertRaises(NodusShipTimeoutError) as raised:
            graph.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake", timeout=0.3)

        self.assertEqual(graph.token, raised.exception.token)
        self.assertTrue(graph.check("document:a", "view", "user:alice"))

    def test_waits_validate_their_arguments_before_doing_anything(self):
        graph = self.open()
        before = graph.token

        for bad in (0, -1, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                graph.wait_shipped(before, timeout=bad)
            with self.assertRaises(ValueError):
                graph.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake", timeout=bad)
        with self.assertRaises(TypeError):
            graph.wait_shipped(before, timeout="soon")
        with self.assertRaises(TypeError):
            graph.wait_shipped("not a token")
        with self.assertRaises(ValueError):
            graph.wait_shipped((-1, 0))
        self.assertEqual(before, graph.token)

    def test_a_token_from_the_future_is_refused_without_waiting(self):
        graph = self.open()
        token = graph.token

        started = time.monotonic()
        with self.assertRaises(NodusError):
            graph.wait_shipped((token.epoch, token.lsn + 1000), timeout=20)

        self.assertLess(time.monotonic() - started, 5)

    def fill_backlog(self, graph):
        written = 0
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            transaction = Transaction()
            for index in range(BATCH_TUPLES):
                transaction.add(f"document:b{written + index}", "viewer", "user:alice")
            try:
                graph.write(transaction)
            except NodusLogBacklogError:
                return written
            written += BATCH_TUPLES
        self.fail("the backlog never reached its cap")

    def test_writes_are_refused_at_the_backlog_cap_and_resume_when_shipping_recovers(self):
        graph = self.open(backlog_cap_bytes=BACKLOG_CAP)
        self.break_store()

        written = self.fill_backlog(graph)

        self.assertGreater(written, 0)
        before = graph.token
        with self.assertRaises(NodusLogBacklogError):
            graph.add_tuple("document:refused", "viewer", "user:alice")
        self.assertEqual(before, graph.token)
        self.assertFalse(graph.check("document:refused", "view", "user:alice"))
        self.assertGreaterEqual(graph.stats()["shipping"]["backlog_bytes"], BACKLOG_CAP)

        self.bucket.unlink()
        deadline = time.monotonic() + 60
        while True:
            try:
                resumed = graph.add_tuple("document:resumed", "viewer", "user:alice")
                break
            except NodusLogBacklogError:
                self.assertLess(time.monotonic(), deadline, "writes never resumed after the store came back")
                time.sleep(0.1)
        graph.wait_shipped(resumed, timeout=60)
        self.assertEqual(0, graph.stats()["shipping"]["lag_lsn"])

    def test_events_are_logged_once_each(self):
        graph = self.open(backlog_cap_bytes=BACKLOG_CAP)
        self.break_store()
        self.fill_backlog(graph)

        records = []
        handler = logging.Handler()
        handler.emit = records.append
        logger = logging.getLogger("nodusdb")
        logger.addHandler(handler)
        try:
            first = graph.stats()["shipping"]["events"]
            logged_after_first = len(records)
            second = graph.stats()["shipping"]["events"]
            logged_after_second = len(records)
        finally:
            logger.removeHandler(handler)

        self.assertTrue(first)
        self.assertEqual(len(first), logged_after_first)
        self.assertEqual(len(second) - len(first), logged_after_second - logged_after_first)

    def test_a_lake_wait_does_not_block_other_threads_on_the_graph(self):
        graph = self.open()
        self.break_store()
        outcome = []

        def writer():
            try:
                graph.write(Transaction().add("document:slow", "viewer", "user:alice"), durability="lake", timeout=5)
            except NodusShipTimeoutError as error:
                outcome.append(error)

        thread = threading.Thread(target=writer)
        thread.start()
        deadline = time.monotonic() + 10
        while not graph.check("document:slow", "view", "user:alice"):
            self.assertLess(time.monotonic(), deadline, "the lake write was never applied locally")
            time.sleep(0.01)

        started = time.monotonic()
        graph.add_tuple("document:fast", "viewer", "user:alice")
        graph.stats()
        elapsed = time.monotonic() - started
        thread.join(20)

        self.assertLess(elapsed, 3)
        self.assertEqual(1, len(outcome))

    def test_a_second_graph_on_a_copy_of_the_directory_is_refused(self):
        graph = self.open()
        graph.write(Transaction().add("document:a", "viewer", "user:alice"), durability="lake")
        self.close(graph)
        stale = self.root / "stale"
        stale.mkdir()
        with self.assertRaises(NodusWriterFencedError):
            Graph(path=stale, sync_mode="sync", shipping=self.shipping())

    def test_signing_keys_are_never_overwritten(self):
        with self.assertRaises(NodusError):
            generate_signing_key(self.private_key, self.root / "other.pub")
        self.assertFalse((self.root / "other.pub").exists())
        with self.assertRaises(NodusError):
            generate_signing_key(self.root / "other.pem", self.public_key)
        self.assertFalse((self.root / "other.pem").exists())

    def test_shipping_to_json_omits_unset_values_and_rejects_a_bad_key_id(self):
        document = json.loads(Shipping.directory(self.bucket, key_file="k.pem", key_id=3).to_json())

        self.assertEqual({"store", "signing"}, set(document))
        self.assertEqual({"key_file": "k.pem", "key_id": 3}, document["signing"])
        with self.assertRaises(TypeError):
            Shipping.directory(self.bucket, key_file="k.pem", key_id="3").to_json()
        with self.assertRaises(TypeError):
            Shipping.directory(self.bucket, key_file="k.pem", key_id=True).to_json()

    def test_shipping_to_json_covers_every_setting(self):
        shipping = Shipping.s3(
            "graphs", "eu-west-1", prefix="team", endpoint="http://localhost:9000", path_style=True,
            key_file=Path("k.pem"), key_id=1, public_key_file=Path("k.pub"),
            credentials=static_credentials("id", "secret", "token"), interval_ms=50, max_object_bytes=1 << 20,
            backlog_cap_bytes=1 << 30, retention_days=3, request_timeout_ms=2_000, iceberg=True,
            iceberg_commit_interval_s=30, iceberg_table_retention_days=2)

        document = json.loads(shipping.to_json())

        self.assertEqual({"type": "s3", "bucket": "graphs", "region": "eu-west-1", "prefix": "team",
                          "endpoint": "http://localhost:9000", "path_style": True}, document["store"])
        self.assertEqual({"source": "static", "access_key_id": "id", "secret_access_key": "secret",
                          "session_token": "token"}, document["credentials"])
        self.assertEqual({"interval_ms": 50, "max_object_bytes": 1 << 20, "backlog_cap_bytes": 1 << 30,
                          "retention_days": 3, "request_timeout_ms": 2_000}, document["ship"])
        self.assertEqual({"enabled": True, "commit_interval_s": 30, "table_retention_days": 2},
                         document["iceberg"])
        self.assertEqual({"key_file": "k.pem", "key_id": 1, "public_key_file": "k.pub"}, document["signing"])


@unittest.skipUnless(_native_has_shipping_exports(), "libnodusdb lacks the shipping exports; rebuild the native library")
class ShippedWritesSurviveKillTest(unittest.TestCase):
    """A lake write that returned must be in the bucket and in the log even if the process is killed."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="nodus-ship-crash-"))
        self.private_key = self.root / "signing.pem"
        self.public_key = self.root / "signing.pub"
        generate_signing_key(self.private_key, self.public_key)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def test_every_acknowledged_lake_write_survives_a_kill(self):
        graph_path = self.root / "graph"
        bucket = self.root / "bucket"
        environment = dict(os.environ)
        environment["PYTHONPATH"] = os.pathsep.join(
            filter(None, [str(PYTHON_DIRECTORY), environment.get("PYTHONPATH")]))
        child = subprocess.Popen(
            [sys.executable, "-u", "-c", CHILD, str(graph_path), str(bucket), str(self.private_key)],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=environment)
        acknowledged = []
        try:
            for _ in range(LAKE_WRITES // 2):
                line = child.stdout.readline()
                self.assertTrue(line, "the child exited before acknowledging enough writes")
                index, epoch, lsn = (int(part) for part in line.split())
                acknowledged.append((index, epoch, lsn))
        finally:
            child.kill()
            child.wait()
            child.stdout.close()
        shippedBeforeRestart = len(_chain_objects(bucket))

        shipping = Shipping.directory(bucket, key_file=self.private_key, key_id=7, public_key_file=self.public_key,
                                      interval_ms=10)
        with Graph(path=graph_path, sync_mode="sync", shipping=shipping) as recovered:
            missing = [index for index, _, _ in acknowledged
                       if not recovered.check(f"document:d{index}", "view", "user:alice")]
            self.assertEqual([], missing)
            self.assertGreater(shippedBeforeRestart, 0)
            top = max(lsn for _, _, lsn in acknowledged)
            recovered.wait_shipped((recovered.token.epoch, recovered.token.lsn), timeout=20)
            stats = recovered.stats()["shipping"]
            self.assertGreaterEqual(stats["shipped_lsn"], top)
            self.assertGreater(recovered.token.epoch, acknowledged[0][1])
            after = recovered.write(Transaction().add("document:after", "viewer", "user:alice"), durability="lake")
            self.assertGreater(after.lsn, top)


if __name__ == "__main__":
    unittest.main()
