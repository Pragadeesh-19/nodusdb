import contextlib
import io
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY / "scripts"))

import check_release  # noqa: E402
import https_probe  # noqa: E402
import stage_package_files  # noqa: E402
import verify_published  # noqa: E402


class TagCheckTest(unittest.TestCase):
    def test_a_tag_must_be_v_plus_the_version_and_the_flag_must_match_the_suffix(self):
        cases = [
            ("v0.1.0rc2", "0.1.0rc2", True, 0),
            ("v0.1.0", "0.1.0", False, 0),
            ("v0.1.0rc2", "0.1.0", True, 2),
            ("v0.1.0", "0.1.0rc2", False, 2),
            ("v0.1.0rc2", "0.1.0rc2", False, 1),
            ("v0.1.0", "0.1.0", True, 1),
            ("0.1.0", "0.1.0", False, 1),
            ("v0.1.1", "0.1.0", False, 1),
            ("v0.1.0a1", "0.1.0a1", True, 0),
            ("v0.1.0.dev3", "0.1.0.dev3", True, 0),
            ("v0.1.0b2", "0.1.0b2", False, 1),
        ]
        for tag, version, prerelease, problems in cases:
            with self.subTest(tag=tag, version=version, prerelease=prerelease):
                self.assertEqual(problems, len(check_release.check_tag(tag, version, prerelease)))

    def test_pre_release_detection_follows_pep_440_suffixes(self):
        for version, expected in [("0.1.0", False), ("1.2.3", False), ("0.1.0rc1", True), ("2.0a3", True),
                                  ("2.0b1", True), ("1.0.dev4", True), ("1.0.post1", False)]:
            with self.subTest(version=version):
                self.assertEqual(expected, check_release.is_prerelease(version))


class WheelCheckTest(unittest.TestCase):
    def setUp(self):
        self.directory = Path(tempfile.mkdtemp(prefix="nodus-wheels-"))

    def _touch(self, names):
        for name in names:
            (self.directory / name).write_bytes(b"")

    def test_exactly_the_five_expected_wheels_pass(self):
        self._touch(check_release.wheel_name("0.1.0", p) for p in check_release.PLATFORMS)

        self.assertEqual([], check_release.check_wheels(self.directory, "0.1.0"))

    def test_a_missing_wheel_is_named(self):
        names = [check_release.wheel_name("0.1.0", p) for p in check_release.PLATFORMS]
        self._touch(names[:-1])

        problems = check_release.check_wheels(self.directory, "0.1.0")

        self.assertEqual([f"missing wheel {names[-1]}"], problems)

    def test_a_wheel_of_another_version_or_tag_is_unexpected_and_its_expected_twin_missing(self):
        self._touch(check_release.wheel_name("0.1.0", p) for p in check_release.PLATFORMS[:-1])
        self._touch(["nodusdb-0.1.0-cp312-cp312-win_amd64.whl", "nodusdb-0.0.9-py3-none-win_amd64.whl"])

        problems = check_release.check_wheels(self.directory, "0.1.0")

        self.assertEqual(3, len(problems))
        self.assertIn("missing wheel nodusdb-0.1.0-py3-none-win_amd64.whl", problems)
        self.assertIn("unexpected wheel nodusdb-0.1.0-cp312-cp312-win_amd64.whl", problems)

    def test_a_build_job_checks_only_its_own_platform(self):
        self._touch([check_release.wheel_name("0.1.0", "win_amd64")])

        self.assertEqual([], check_release.check_wheels(self.directory, "0.1.0", ("win_amd64",)))
        self.assertEqual(4, len(check_release.check_wheels(self.directory, "0.1.0")))

    def test_the_command_line_exits_nonzero_on_a_problem_and_prints_it(self):
        version = check_release.package_version()
        errors = io.StringIO()

        with contextlib.redirect_stderr(errors), contextlib.redirect_stdout(io.StringIO()):
            failed = check_release.main(["tag", "--tag", "v9.9.9", "--prerelease", "false"])
            passed = check_release.main(["tag", "--tag", f"v{version}",
                                         "--prerelease", str(check_release.is_prerelease(version)).lower()])

        self.assertEqual(1, failed)
        self.assertEqual(0, passed)
        self.assertIn("v9.9.9", errors.getvalue())

    def test_the_package_version_is_read_from_pyproject(self):
        self.assertRegex(check_release.package_version(), r"^\d+\.\d+\.\d+")


class HttpsProbeTest(unittest.TestCase):
    def test_the_s3_authentication_error_means_tls_worked(self):
        message = ("failed to create graph handle: create _nodus/probe/6031 failed with HTTP 403 InvalidAccessKeyId: "
                   "The AWS Access Key Id you provided does not exist in our records.")

        self.assertEqual(https_probe.REACHED, https_probe.classify(message))

    def test_certificate_and_handshake_errors_mean_the_trust_store_failed(self):
        for message in [
            "javax.net.ssl.SSLHandshakeException: PKIX path building failed",
            "unable to find valid certification path to requested target",
            "the trustAnchors parameter must be non-empty",
            "remote host terminated the handshake",
            "HTTP 403 but the certificate chain was rejected",
        ]:
            with self.subTest(message=message):
                self.assertEqual(https_probe.TLS, https_probe.classify(message))

    def test_anything_else_is_a_network_problem_to_retry(self):
        for message in ["connection timed out", "UnknownHostException: s3.amazonaws.com", ""]:
            with self.subTest(message=message):
                self.assertEqual(https_probe.UNREACHABLE, https_probe.classify(message))


class VerifyPublishedTest(unittest.TestCase):
    class Clock:
        def __init__(self):
            self.now = 0.0
            self.sleeps = []

        def time(self):
            return self.now

        def sleep(self, seconds):
            self.sleeps.append(seconds)
            self.now += seconds

    @staticmethod
    def result(code):
        return subprocess.CompletedProcess([], code, "", "")

    def test_the_wait_returns_at_once_on_success(self):
        clock = self.Clock()

        outcome = verify_published.wait_for(lambda: self.result(0), 600, 30, clock.time, clock.sleep)

        self.assertEqual(0, outcome.returncode)
        self.assertEqual([], clock.sleeps)

    def test_the_wait_retries_while_the_index_catches_up(self):
        clock = self.Clock()
        outcomes = iter([1, 1, 0])

        outcome = verify_published.wait_for(lambda: self.result(next(outcomes)), 600, 30, clock.time, clock.sleep)

        self.assertEqual(0, outcome.returncode)
        self.assertEqual([30, 30], clock.sleeps)

    def test_the_wait_gives_up_at_the_deadline_with_the_last_failure(self):
        clock = self.Clock()
        calls = []

        def attempt():
            calls.append(clock.now)
            return self.result(1)

        outcome = verify_published.wait_for(attempt, 100, 30, clock.time, clock.sleep)

        self.assertEqual(1, outcome.returncode)
        self.assertEqual([0.0, 30.0, 60.0, 90.0], calls)

    def test_a_download_resolves_to_a_single_version(self):
        self.assertEqual("0.1.0", verify_published.resolved_version(["nodusdb-0.1.0-py3-none-win_amd64.whl"]))
        self.assertEqual("0.1.0rc2", verify_published.resolved_version(
            ["nodusdb-0.1.0rc2-py3-none-manylinux_2_34_x86_64.whl"]))
        self.assertIsNone(verify_published.resolved_version([]))
        self.assertIsNone(verify_published.resolved_version(
            ["nodusdb-0.1.0-py3-none-win_amd64.whl", "nodusdb-0.0.9-py3-none-win_amd64.whl"]))
        self.assertIsNone(verify_published.resolved_version(["something-else-1.0-py3-none-any.whl"]))


class StageTest(unittest.TestCase):
    def setUp(self):
        self.root = Path(tempfile.mkdtemp(prefix="nodus-stage-root-"))
        self.package = Path(tempfile.mkdtemp(prefix="nodus-stage-package-"))
        (self.root / "LICENSE").write_text("license", encoding="utf-8")
        (self.root / "NOTICE").write_text("notice", encoding="utf-8")
        (self.root / "licenses").mkdir()
        (self.root / "licenses" / "runtime.txt").write_text("runtime", encoding="utf-8")

    def test_the_license_files_are_copied_into_the_package_directory(self):
        stage_package_files.stage(self.root, self.package)

        self.assertEqual("license", (self.package / "LICENSE").read_text(encoding="utf-8"))
        self.assertEqual("notice", (self.package / "NOTICE").read_text(encoding="utf-8"))
        self.assertEqual("runtime", (self.package / "licenses" / "runtime.txt").read_text(encoding="utf-8"))

    def test_staging_twice_is_harmless(self):
        stage_package_files.stage(self.root, self.package)
        stage_package_files.stage(self.root, self.package)

        self.assertEqual("license", (self.package / "LICENSE").read_text(encoding="utf-8"))

    def test_a_missing_notice_is_an_error(self):
        (self.root / "NOTICE").unlink()

        with self.assertRaises(FileNotFoundError):
            stage_package_files.stage(self.root, self.package)

    def test_the_repository_has_every_file_the_script_stages(self):
        for name in stage_package_files.FILES + stage_package_files.DIRECTORIES:
            with self.subTest(name=name):
                self.assertTrue((REPOSITORY / name).exists())


if __name__ == "__main__":
    unittest.main()
