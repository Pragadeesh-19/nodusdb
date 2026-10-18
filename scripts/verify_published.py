import argparse
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from check_release import package_version

DEADLINE_SECONDS = 600
INTERVAL_SECONDS = 30
YANK_ADVICE = (
    "yank this version at https://pypi.org/manage/project/nodusdb/releases/ (Options, Yank), "
    "fix the problem and publish the next version"
)


def run_pip(*arguments):
    return subprocess.run([sys.executable, "-m", "pip", *arguments], capture_output=True, text=True)


def wait_for(attempt, deadline_seconds=DEADLINE_SECONDS, interval_seconds=INTERVAL_SECONDS,
             clock=time.monotonic, sleep=time.sleep):
    started = clock()
    while True:
        result = attempt()
        if result.returncode == 0:
            return result
        if clock() - started + interval_seconds > deadline_seconds:
            return result
        sleep(interval_seconds)


def resolved_version(filenames):
    versions = set()
    for name in filenames:
        parts = name.split("-")
        if len(parts) >= 5 and parts[0] == "nodusdb" and name.endswith(".whl"):
            versions.add(parts[1])
    return versions.pop() if len(versions) == 1 else None


def install(version):
    return wait_for(lambda: run_pip("install", "--no-cache-dir", f"nodusdb=={version}"))


def resolve(version):
    def attempt():
        with tempfile.TemporaryDirectory() as directory:
            result = run_pip("download", "--no-cache-dir", "--no-deps", "--only-binary=:all:", "nodusdb",
                             "--dest", directory)
            if result.returncode == 0:
                found = resolved_version(path.name for path in Path(directory).iterdir())
                if found != version:
                    return subprocess.CompletedProcess(result.args, 1, result.stdout,
                                                       f"a plain install resolves to {found}, not {version}")
            return result
    return wait_for(attempt)


def main(arguments=None):
    parser = argparse.ArgumentParser(description="Check what PyPI serves for the released version.")
    parser.add_argument("command", choices=["install", "resolve"])
    parser.add_argument("--version", default="")
    parsed = parser.parse_args(arguments)
    version = parsed.version or package_version()
    result = install(version) if parsed.command == "install" else resolve(version)
    sys.stdout.write(result.stdout)
    if result.returncode != 0:
        sys.stderr.write(result.stderr)
        print(f"error: {parsed.command} of nodusdb {version} from the index failed; {YANK_ADVICE}", file=sys.stderr)
        return 1
    print(f"{parsed.command} of nodusdb {version} from the index passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
