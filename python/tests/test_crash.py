import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))

from nodusdb import Graph  # noqa: E402

EDGES = 2_000
CHECKPOINT_EVERY = 250

CHILD = f"""
import sys
from nodusdb import Graph
graph = Graph(path=sys.argv[1], sync_mode="sync")
for i in range({EDGES}):
    graph.add_edge(i, i + 1)
    print(i, flush=True)
    if i % {CHECKPOINT_EVERY} == {CHECKPOINT_EVERY - 1}:
        graph.checkpoint()
"""


class AcknowledgedWritesSurviveKillTest(unittest.TestCase):
    """A sync-mode call that returned must survive SIGKILL, even mid-checkpoint."""

    def setUp(self):
        self.directory = tempfile.mkdtemp(prefix="nodus-crash-")

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def _kill_after_acknowledgements(self, path, acknowledged):
        env = dict(os.environ)
        env["PYTHONPATH"] = os.pathsep.join(filter(None, [str(PYTHON_DIRECTORY), env.get("PYTHONPATH")]))
        child = subprocess.Popen(
            [sys.executable, "-u", "-c", CHILD, path],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, env=env,
        )
        try:
            for expected in range(acknowledged):
                line = child.stdout.readline()
                self.assertTrue(line, f"child exited before acknowledgement {expected}")
                self.assertEqual(expected, int(line))
        finally:
            child.kill()
            child.wait()
            child.stdout.close()

    def test_every_acknowledged_edge_is_present_after_kill(self):
        for round_number, acknowledged in enumerate((120, 400, 900)):
            path = os.path.join(self.directory, f"round-{round_number}")
            self._kill_after_acknowledgements(path, acknowledged)
            with Graph(path=path, sync_mode="sync") as recovered:
                missing = [i for i in range(acknowledged) if not recovered.has_edge(i, i + 1)]
                self.assertEqual([], missing, f"round {round_number}: acknowledged edges lost")


if __name__ == "__main__":
    unittest.main()
