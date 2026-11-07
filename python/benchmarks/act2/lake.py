"""What an outside reader sees of the Iceberg table: its newest committed version and the log position it holds."""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

METADATA_PREFIX = "iceberg/metadata/"
PROJECTED_LSN = "nodus.projected.lsn"
_VERSION = re.compile(r"v(\d+)\.metadata\.json")


class Objects(Protocol):
    def keys(self, prefix: str) -> list[str]: ...

    def read(self, key: str) -> bytes | None: ...

    def location(self, key: str) -> str: ...


@dataclass(frozen=True)
class TableVersion:
    number: int
    key: str
    projected_lsn: int


class DirectoryObjects:
    def __init__(self, root):
        self._root = Path(root)

    def keys(self, prefix):
        base = self._root / prefix
        if not base.is_dir():
            return []
        return [path.relative_to(self._root).as_posix() for path in base.rglob("*") if path.is_file()]

    def read(self, key):
        try:
            return (self._root / key).read_bytes()
        except FileNotFoundError:
            return None

    def location(self, key):
        return (self._root / key).as_posix()


def newest_version_key(keys):
    versions = [(int(match[1]), key) for key in keys if (match := _VERSION.fullmatch(key.rsplit("/", 1)[-1]))]
    return max(versions, default=None)


def newest_version(objects):
    newest = newest_version_key(objects.keys(METADATA_PREFIX))
    if newest is None:
        return None
    number, key = newest
    content = objects.read(key)
    if content is None:
        return None
    return TableVersion(number, key, projected_lsn(json.loads(content)))


def projected_lsn(metadata):
    current = metadata["current-snapshot-id"]
    snapshot = next(snapshot for snapshot in metadata["snapshots"] if snapshot["snapshot-id"] == current)
    return int(snapshot["summary"][PROJECTED_LSN])
