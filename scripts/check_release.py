import argparse
import re
import sys
import tomllib
from pathlib import Path

PLATFORMS = (
    "manylinux_2_34_x86_64",
    "manylinux_2_34_aarch64",
    "macosx_11_0_x86_64",
    "macosx_11_0_arm64",
    "win_amd64",
)
PRE_RELEASE = re.compile(r"(a|b|rc)\d+|\.dev\d+")
PYPROJECT = Path(__file__).resolve().parents[1] / "python" / "pyproject.toml"


def package_version(pyproject=PYPROJECT):
    return tomllib.loads(Path(pyproject).read_text(encoding="utf-8"))["project"]["version"]


def is_prerelease(version):
    return PRE_RELEASE.search(version) is not None


def check_tag(tag, version, prerelease):
    problems = []
    if tag != f"v{version}":
        problems.append(f"the release tag is {tag} but the package version is {version}; the tag must be v{version}")
    if prerelease != is_prerelease(version):
        kind = "a pre-release" if is_prerelease(version) else "a final release"
        flagged = "a pre-release" if prerelease else "not a pre-release"
        problems.append(f"version {version} is {kind} but the GitHub release is marked {flagged}")
    return problems


def wheel_name(version, platform):
    return f"nodusdb-{version}-py3-none-{platform}.whl"


def check_wheels(directory, version, platforms=PLATFORMS):
    present = {path.name for path in Path(directory).glob("*.whl")}
    expected = {wheel_name(version, platform) for platform in platforms}
    problems = [f"missing wheel {name}" for name in sorted(expected - present)]
    problems += [f"unexpected wheel {name}" for name in sorted(present - expected)]
    return problems


def parse_flag(text):
    if text not in ("true", "false"):
        raise argparse.ArgumentTypeError(f"expected true or false, got {text}")
    return text == "true"


def main(arguments=None):
    parser = argparse.ArgumentParser(description="Refuse a release whose tag, version or wheels do not agree.")
    commands = parser.add_subparsers(dest="command", required=True)
    tag = commands.add_parser("tag")
    tag.add_argument("--tag", required=True)
    tag.add_argument("--prerelease", required=True, type=parse_flag)
    wheels = commands.add_parser("wheels")
    wheels.add_argument("--dir", required=True, type=Path)
    wheels.add_argument("--platform", action="append", choices=PLATFORMS)
    parsed = parser.parse_args(arguments)

    version = package_version()
    if parsed.command == "tag":
        problems = check_tag(parsed.tag, version, parsed.prerelease)
    else:
        problems = check_wheels(parsed.dir, version, tuple(parsed.platform) if parsed.platform else PLATFORMS)
    for problem in problems:
        print(f"error: {problem}", file=sys.stderr)
    if not problems:
        print(f"release check passed for {version}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
