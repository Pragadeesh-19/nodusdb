import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

PYTHON_DIRECTORY = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PYTHON_DIRECTORY))
sys.path.insert(0, str(PYTHON_DIRECTORY / "benchmarks"))

from act2 import fixture  # noqa: E402


class FixtureTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp(prefix="act2-fixture-"))
        shutil.copytree(fixture.DIRECTORY, self.directory, dirs_exist_ok=True)

    def tearDown(self):
        shutil.rmtree(self.directory, ignore_errors=True)

    def edit_manifest(self, **changes):
        path = self.directory / "dataset.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        manifest.update(changes)
        path.write_text(json.dumps(manifest), encoding="utf-8")

    def test_the_committed_fixture_is_authzbench_s_with_seed_1(self):
        data = fixture.read_fixture()

        self.assertEqual(("S", 1, 5487), (data.scale, data.seed, len(data.relationships)))
        self.assertEqual(fixture.Relationship("group:revocation-team", "member", "user:revocation-target"),
                         data.revoke)
        self.assertIn(data.revoke, data.relationships)
        self.assertEqual(fixture.Query("document:revocation-target", "view", "user:revocation-target"), data.probe)
        self.assertIn(data.probe.subject, data.users())
        self.assertTrue(all(user.startswith("user:") for user in data.users()))

    def test_the_user_erased_is_the_one_with_the_most_grants_left_after_the_revoke(self):
        data = fixture.read_fixture()

        self.assertEqual(("user:u662", 10), data.busiest_user_after_the_revoke())

    def test_a_tie_goes_to_the_first_name_and_the_revoked_grant_does_not_count(self):
        data = fixture.read_fixture(self.directory)
        revoke = data.revoke
        tied = fixture.Fixture(data.scale, data.seed, data.schema, (
            fixture.Relationship("document:a", "viewer", "user:b"),
            fixture.Relationship("document:b", "viewer", "user:a"),
            revoke,
            fixture.Relationship("document:c", "viewer", revoke.subject)), revoke, data.probe)

        self.assertEqual(("user:a", 1), tied.busiest_user_after_the_revoke())

    def test_another_format_is_refused(self):
        self.edit_manifest(format="authzbench.dataset/2")
        with self.assertRaisesRegex(fixture.FixtureError, "authzbench.dataset/2"):
            fixture.read_fixture(self.directory)

    def test_another_model_is_refused(self):
        self.edit_manifest(model="github")
        with self.assertRaisesRegex(fixture.FixtureError, "github"):
            fixture.read_fixture(self.directory)

    def test_a_manifest_whose_counts_disagree_with_its_files_is_refused(self):
        self.edit_manifest(relationships=5486)
        with self.assertRaisesRegex(fixture.FixtureError, "5486 relationships but the file holds 5487"):
            fixture.read_fixture(self.directory)

    def test_a_schema_the_audit_sql_was_not_written_for_is_refused(self):
        schema = self.directory / fixture.SCHEMA_FILE
        schema.write_text(schema.read_text(encoding="utf-8").replace("parent->view", "viewer"), encoding="utf-8")
        with self.assertRaisesRegex(fixture.FixtureError, "the audit SQL is written for the schema"):
            fixture.read_fixture(self.directory)

    def test_a_schema_checked_out_with_windows_line_endings_is_the_same_schema(self):
        schema = self.directory / fixture.SCHEMA_FILE
        schema.write_bytes(schema.read_bytes().replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))

        self.assertEqual(fixture.SCHEMA_SHA256, fixture.schema_digest(fixture.read_fixture(self.directory).schema))

    def test_a_line_that_is_not_a_relationship_is_refused(self):
        with (self.directory / "relationships.txt").open("a", encoding="utf-8") as stream:
            stream.write("document:d1#viewer\n")
        with self.assertRaisesRegex(fixture.FixtureError, "not a relationship"):
            fixture.read_fixture(self.directory)


if __name__ == "__main__":
    unittest.main()
