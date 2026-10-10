import ctypes
import dataclasses
import json
import os
import typing

from . import _native
from .errors import error_for
from .stores import directory_store, s3_store, without_none

Path = typing.Union[str, os.PathLike]


def static_credentials(access_key_id, secret_access_key, session_token=None):
    """Fixed S3 credentials. Prefer environment_credentials() or file_credentials() outside tests."""
    return without_none({
        "source": "static",
        "access_key_id": access_key_id,
        "secret_access_key": secret_access_key,
        "session_token": session_token,
    })


def environment_credentials():
    """Read AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN when the graph opens."""
    return {"source": "environment"}


def file_credentials(path=None, profile=None):
    """Read a profile from an AWS credentials file (the default location and profile when omitted)."""
    return without_none({
        "source": "file",
        "path": None if path is None else os.fspath(path),
        "profile": profile,
    })


@dataclasses.dataclass(frozen=True)
class Shipping:
    """Where a durable graph ships its log, and how.

    The graph writes a signed, hash-linked chain of log objects to the store. A write made with
    durability="lake" returns once the chain holds it. Values are checked when the graph opens, and a
    problem is reported with the name of the setting."""

    store: typing.Mapping[str, typing.Any]
    key_file: Path
    key_id: int
    public_key_file: typing.Optional[Path] = None
    credentials: typing.Optional[typing.Mapping[str, typing.Any]] = None
    interval_ms: typing.Optional[int] = None
    max_object_bytes: typing.Optional[int] = None
    backlog_cap_bytes: typing.Optional[int] = None
    retention_days: typing.Optional[int] = None
    request_timeout_ms: typing.Optional[int] = None
    iceberg: bool = False
    iceberg_commit_interval_s: typing.Optional[int] = None
    iceberg_table_retention_days: typing.Optional[int] = None

    @classmethod
    def directory(cls, path, **settings):
        """Ship into a local directory. Meant for tests and single-machine setups."""
        return cls(store=directory_store(path), **settings)

    @classmethod
    def s3(cls, bucket, region, *, prefix=None, endpoint=None, path_style=None, ca_bundle=None, **settings):
        """Ship into an S3 bucket, or any S3-compatible server when endpoint is given."""
        store = s3_store(bucket, region, prefix, endpoint, path_style, ca_bundle)
        return cls(store=store, **settings)

    def to_json(self):
        if isinstance(self.key_id, bool) or not isinstance(self.key_id, int):
            raise TypeError(f"key_id must be an int, got {type(self.key_id).__name__}")
        document = {
            "store": dict(self.store),
            "signing": without_none({
                "key_file": os.fspath(self.key_file),
                "key_id": self.key_id,
                "public_key_file": None if self.public_key_file is None else os.fspath(self.public_key_file),
            }),
        }
        if self.credentials is not None:
            document["credentials"] = dict(self.credentials)
        ship = without_none({
            "interval_ms": self.interval_ms,
            "max_object_bytes": self.max_object_bytes,
            "backlog_cap_bytes": self.backlog_cap_bytes,
            "retention_days": self.retention_days,
            "request_timeout_ms": self.request_timeout_ms,
        })
        if ship:
            document["ship"] = ship
        iceberg = without_none({
            "enabled": bool(self.iceberg),
            "commit_interval_s": self.iceberg_commit_interval_s,
            "table_retention_days": self.iceberg_table_retention_days,
        })
        if self.iceberg or len(iceberg) > 1:
            document["iceberg"] = iceberg
        return json.dumps(document)


def generate_signing_key(private_path, public_path, library_path=None):
    """Write a new Ed25519 signing key pair. Neither file may exist yet; the private file is readable by its owner."""
    library, isolate = _native.load(library_path)
    if not hasattr(library, "nodus_signing_key_generate"):
        raise error_for(-10, "this libnodusdb has no shipping support; rebuild or upgrade it")
    function = library.nodus_signing_key_generate
    function.restype = ctypes.c_int
    function.argtypes = [_native.THREAD, ctypes.c_char_p, ctypes.c_char_p]
    thread = _native.current_thread(library, isolate)
    code = function(thread, os.fsencode(os.fspath(private_path)), os.fsencode(os.fspath(public_path)))
    if code != 0:
        detail = _native.last_error_message(library, thread)
        raise error_for(code, f"generate_signing_key failed: {detail}" if detail else "generate_signing_key failed")
