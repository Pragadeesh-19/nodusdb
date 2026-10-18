import shutil
import sys
from pathlib import Path

FILES = ("LICENSE", "NOTICE")
DIRECTORIES = ("licenses",)


def stage(root, package):
    root = Path(root)
    package = Path(package)
    for name in FILES:
        shutil.copy2(root / name, package / name)
    for name in DIRECTORIES:
        shutil.copytree(root / name, package / name, dirs_exist_ok=True)
    return [*FILES, *DIRECTORIES]


def main():
    root = Path(__file__).resolve().parents[1]
    staged = stage(root, root / "python")
    print("staged " + ", ".join(staged) + " into python/")
    return 0


if __name__ == "__main__":
    sys.exit(main())
