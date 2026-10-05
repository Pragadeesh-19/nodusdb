import ctypes
import functools
import os
import platform
import sys
import threading
from pathlib import Path

ENVIRONMENT_VARIABLE = "NODUSDB_LIBRARY"

PACKAGE_DIRECTORY = Path(__file__).resolve().parent
REPOSITORY_ROOT = PACKAGE_DIRECTORY.parents[1]

HANDLE = ctypes.c_void_p
THREAD = ctypes.c_void_p
NODE = ctypes.c_int64
RESULT_BUFFER = ctypes.POINTER(ctypes.c_int64)

_OPERATING_SYSTEMS = {"Windows": "windows", "Darwin": "macos", "Linux": "linux"}
_ARCHITECTURES = {"amd64": "x86_64", "x86_64": "x86_64", "arm64": "arm64", "aarch64": "arm64"}

_loaded = {}
_attached = threading.local()


class NodusError(RuntimeError):
    pass


def platform_tag():
    system = platform.system()
    operating_system = _OPERATING_SYSTEMS.get(system, system.lower())
    machine = platform.machine().lower()
    architecture = _ARCHITECTURES.get(machine, machine)
    return f"{operating_system}-{architecture}"


def _library_file_names():
    if sys.platform == "win32":
        return ("libnodusdb.dll", "nodusdb.dll")
    if sys.platform == "darwin":
        return ("libnodusdb.dylib",)
    return ("libnodusdb.so",)


def _search_directories():
    return (
        PACKAGE_DIRECTORY / "bin" / platform_tag(),
        REPOSITORY_ROOT / "target" / "native",
    )


def find_library(explicit_path=None):
    if explicit_path is not None:
        candidate = Path(explicit_path)
        if candidate.is_file():
            return candidate
        raise NodusError(f"native library not found: {candidate}")

    from_environment = os.environ.get(ENVIRONMENT_VARIABLE)
    if from_environment:
        return find_library(from_environment)

    for directory in _search_directories():
        for name in _library_file_names():
            candidate = directory / name
            if candidate.is_file():
                return candidate

    raise NodusError(
        "libnodusdb not found; build it with `mvn -Pnative -pl core package` "
        f"or set {ENVIRONMENT_VARIABLE} to the library path"
    )


def _create_isolate(library):
    create = library.graal_create_isolate
    create.restype = ctypes.c_int
    create.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p), ctypes.POINTER(ctypes.c_void_p)]
    isolate = ctypes.c_void_p()
    thread = ctypes.c_void_p()
    if create(None, ctypes.byref(isolate), ctypes.byref(thread)) != 0 or not isolate.value:
        raise NodusError("failed to create the GraalVM isolate")
    return isolate


def serialized(method):
    @functools.wraps(method)
    def locked(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)
    return locked


def current_thread(library, isolate):
    """Return this OS thread's isolate thread, attaching it on first use."""
    attached = _attached.__dict__.setdefault("threads", {})
    thread = attached.get(isolate.value)
    if thread is None:
        attach = library.graal_attach_thread
        attach.restype = ctypes.c_int
        attach.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        candidate = ctypes.c_void_p()
        if attach(isolate, ctypes.byref(candidate)) != 0 or not candidate.value:
            raise NodusError("failed to attach this thread to the GraalVM isolate")
        thread = candidate
        attached[isolate.value] = thread
    return thread


def load(explicit_path=None):
    path = str(find_library(explicit_path))
    if path not in _loaded:
        library = ctypes.CDLL(path)
        _loaded[path] = (library, _create_isolate(library))
    return _loaded[path]
