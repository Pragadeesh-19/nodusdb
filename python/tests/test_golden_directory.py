import shutil
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from nodusdb import Graph  # noqa: E402

GOLDEN = Path(__file__).resolve().parents[2] / "core" / "src" / "test" / "resources" / "golden" / "v2" / "directory"

ANSWERS = [
    ("document:plan", "view", "user:alice", True),
    ("document:plan", "view", "user:dave", True),
    ("document:plan", "view", "user:bob", False),
    ("document:readme", "view", "user:bob", True),
    ("document:readme", "view", "user:carol", True),
    ("document:readme", "view", "user:alice", False),
    ("document:old", "view", "user:erin", False),
]


@unittest.skipUnless(GOLDEN.is_dir(), "the golden version 2 directory is not in this checkout")
class GoldenDirectoryTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp(prefix="nodus-golden-")) / "graph"
        shutil.copytree(GOLDEN, self.directory)

    def tearDown(self):
        shutil.rmtree(self.directory.parent, ignore_errors=True)

    def _assert_answers(self, graph):
        self.assertEqual(1, graph.schema_version)
        for object_, permission, subject, expected in ANSWERS:
            with self.subTest(object=object_, permission=permission, subject=subject):
                self.assertEqual(expected, graph.check(object_, permission, subject))

    def test_a_directory_of_this_format_opens_with_its_snapshot_and_its_log_tail(self):
        with Graph(path=self.directory) as graph:
            self._assert_answers(graph)

    def test_the_answers_survive_a_clean_close_and_a_reopen(self):
        Graph(path=self.directory).close()

        with Graph(path=self.directory) as graph:
            self._assert_answers(graph)


if __name__ == "__main__":
    unittest.main()
