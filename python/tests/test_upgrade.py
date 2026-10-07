import ctypes
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph, NodusError, NodusUpgradeRequiredError, find_library, upgrade, upgrade_cleanup  # noqa: E402

GOLDEN = Path(__file__).resolve().parents[2] / "core" / "src" / "test" / "resources" / "golden" / "v1"


def _native_has_upgrade_exports():
    try:
        library = find_library()
    except NodusError:
        return False
    return hasattr(ctypes.CDLL(str(library)), "nodus_upgrade")


@unittest.skipUnless(_native_has_upgrade_exports(), "libnodusdb lacks the upgrade exports; rebuild the native library")
@unittest.skipUnless(GOLDEN.is_dir(), "the golden version 1 directories are not in this checkout")
class UpgradeTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp(prefix="nodus-upgrade-"))

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def _fixture(self, name):
        shutil.copytree(GOLDEN / name, self.directory, dirs_exist_ok=True)

    def test_an_old_directory_is_refused_until_it_is_upgraded(self):
        self._fixture("integer")

        with self.assertRaises(NodusUpgradeRequiredError):
            Graph(path=self.directory)

    def test_an_integer_graph_keeps_its_edges_after_the_upgrade(self):
        self._fixture("integer")

        report = upgrade(self.directory)

        self.assertTrue(report.performed)
        self.assertEqual(1040, report.edges)
        with Graph(path=self.directory) as graph:
            self.assertTrue(graph.has_edge(2000, 2001))
            self.assertFalse(graph.has_edge(0, 1))
            self.assertEqual(39, graph.degree(5000))

    def test_a_string_graph_keeps_its_names(self):
        self._fixture("string")

        report = upgrade(self.directory)

        self.assertEqual(8, report.symbols)
        with Graph(path=self.directory) as graph:
            self.assertTrue(graph.has_edge("user:alice", "group:eng"))
            self.assertEqual(["group:eng"], graph.khop("user:alice", 1))

    def test_upgrading_twice_changes_nothing_and_cleanup_removes_the_backup(self):
        self._fixture("legacy-snapshot")
        upgrade(self.directory)

        again = upgrade(self.directory)

        self.assertFalse(again.performed)
        self.assertTrue((self.directory / "pre-v2").is_dir())
        upgrade_cleanup(self.directory)
        self.assertFalse((self.directory / "pre-v2").exists())
        with Graph(path=self.directory) as graph:
            self.assertEqual(3, graph.degree(0))

    def test_cleanup_refuses_a_directory_that_was_never_upgraded(self):
        self._fixture("integer")

        with self.assertRaises(NodusError):
            upgrade_cleanup(self.directory)

    def test_a_missing_directory_is_an_error(self):
        with self.assertRaises(NodusError):
            upgrade(self.directory / "nowhere")


if __name__ == "__main__":
    unittest.main()
