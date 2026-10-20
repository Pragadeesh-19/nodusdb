import importlib.util
import os
import subprocess
import sys
import unittest
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
EXAMPLES = sorted((REPOSITORY / "examples").glob("[0-9][0-9]_*.py"))
NEEDS = {"04_lake_table.py": "pyarrow", "06_read_lake_with_duckdb.py": "duckdb"}


def _environment():
    environment = dict(os.environ)
    paths = [str(REPOSITORY / "python")]
    if environment.get("PYTHONPATH"):
        paths.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(paths)
    return environment


class ExamplesTest(unittest.TestCase):
    def test_the_examples_directory_has_the_examples(self):
        self.assertGreaterEqual(len(EXAMPLES), 6)

    def test_every_example_runs_and_prints_something(self):
        for script in EXAMPLES:
            with self.subTest(example=script.name):
                needed = NEEDS.get(script.name)
                if needed is not None and importlib.util.find_spec(needed) is None:
                    self.skipTest(f"{needed} is not installed")
                done = subprocess.run([sys.executable, "-W", "error", str(script)], env=_environment(),
                                      capture_output=True, text=True, timeout=240)
                self.assertEqual(0, done.returncode, done.stderr)
                self.assertTrue(done.stdout.strip())
                self.assertNotIn("WARNING", done.stderr)


if __name__ == "__main__":
    unittest.main()
