import ctypes
import os
import typing

from . import _native
from .errors import error_for


class UpgradeReport(typing.NamedTuple):
    """What upgrade() did: performed is False when the directory was already current."""

    performed: bool
    edges: int
    symbols: int


def _call(name, path, library_path, outputs):
    library, isolate = _native.load(library_path)
    function = getattr(library, name)
    function.restype = ctypes.c_int
    function.argtypes = [_native.THREAD, ctypes.c_char_p] + ([ctypes.POINTER(ctypes.c_int64)] if outputs else [])
    thread = _native.current_thread(library, isolate)
    out = (ctypes.c_int64 * outputs)()
    arguments = (thread, os.fsencode(os.fspath(path))) + ((out,) if outputs else ())
    code = function(*arguments)
    if code != 0:
        detail = _native.last_error_message(library, thread)
        raise error_for(code, f"{name[len('nodus_'):]} failed: {detail}" if detail else f"{name} failed")
    return list(out)


def upgrade(path, library_path=None):
    """Convert a directory written by an earlier version to the current format.

    The original files are kept in pre-v2/ until upgrade_cleanup() is called. Running it again after an
    interruption finishes the conversion."""
    performed, edges, symbols = _call("nodus_upgrade", path, library_path, 3)
    return UpgradeReport(bool(performed), edges, symbols)


def upgrade_cleanup(path, library_path=None):
    """Delete the pre-v2/ backup left by upgrade()."""
    _call("nodus_upgrade_cleanup", path, library_path, 0)
