import ctypes
import dataclasses
import json
import os
import typing

from . import _native
from .errors import error_for
from .stores import directory_store, s3_store, without_none

Path = typing.Union[str, os.PathLike]


@dataclasses.dataclass(frozen=True)
class Follower:
    """Where a read-only follower reads its chain from, and how it follows it.

    A follower rebuilds the graph from the newest verified snapshot in the object store and keeps applying the
    signed chain. It holds only the public key of the writer. Every write through a follower raises
    NodusUnsupportedError. Values are checked when the follower opens."""

    store: typing.Mapping[str, typing.Any]
    public_key_file: Path
    key_id: int
    credentials: typing.Optional[typing.Mapping[str, typing.Any]] = None
    poll_interval_ms: typing.Optional[int] = None
    read_wait_ms: typing.Optional[int] = None
    max_staleness_ms: typing.Optional[int] = None
    download_parallelism: typing.Optional[int] = None
    request_timeout_ms: typing.Optional[int] = None
    state_directory: typing.Optional[Path] = None

    @classmethod
    def directory(cls, path, **settings):
        """Follow a chain in a local directory. Meant for tests and single-machine setups."""
        return cls(store=directory_store(path), **settings)

    @classmethod
    def s3(cls, bucket, region, *, prefix=None, endpoint=None, path_style=None, ca_bundle=None, **settings):
        """Follow a chain in an S3 bucket, or any S3-compatible server when endpoint is given."""
        return cls(store=s3_store(bucket, region, prefix, endpoint, path_style, ca_bundle), **settings)

    def to_json(self):
        if isinstance(self.key_id, bool) or not isinstance(self.key_id, int):
            raise TypeError(f"key_id must be an int, got {type(self.key_id).__name__}")
        document = {
            "store": dict(self.store),
            "trust": {"key_id": self.key_id, "public_key_file": os.fspath(self.public_key_file)},
        }
        if self.credentials is not None:
            document["credentials"] = dict(self.credentials)
        follow = without_none({
            "poll_interval_ms": self.poll_interval_ms,
            "read_wait_ms": self.read_wait_ms,
            "max_staleness_ms": self.max_staleness_ms,
            "download_parallelism": self.download_parallelism,
            "request_timeout_ms": self.request_timeout_ms,
            "state_directory": None if self.state_directory is None else os.fspath(self.state_directory),
        })
        if follow:
            document["follow"] = follow
        return json.dumps(document)


@dataclasses.dataclass(frozen=True)
class Restored:
    """What a restore wrote: the last LSN it applied, the writer epoch it ended in and the snapshot it started from."""

    applied_lsn: int
    epoch: int
    snapshot_lsn: int


def restore(follower, target, library_path=None):
    """Rebuild a graph directory from the chain a Follower describes.

    The target must not exist or must be an empty directory. It appears only when the restore is complete, so a
    failed or interrupted restore leaves no directory that looks like a graph."""
    if not isinstance(follower, Follower):
        raise TypeError(f"expected a Follower, got {type(follower).__name__}")
    library, isolate = _native.load(library_path)
    if not hasattr(library, "nodus_restore"):
        raise error_for(-10, "this libnodusdb has no restore support; rebuild or upgrade it")
    function = library.nodus_restore
    function.restype = ctypes.c_int
    function.argtypes = [_native.THREAD, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p,
                         ctypes.POINTER(ctypes.c_int64)]
    document = follower.to_json().encode("utf-8")
    report = (ctypes.c_int64 * 3)()
    thread = _native.current_thread(library, isolate)
    code = function(thread, document, len(document), os.fsencode(os.fspath(target)), report)
    if code != 0:
        detail = _native.last_error_message(library, thread)
        raise error_for(code, f"restore failed: {detail}" if detail else "restore failed")
    return Restored(applied_lsn=report[0], epoch=report[1], snapshot_lsn=report[2])
