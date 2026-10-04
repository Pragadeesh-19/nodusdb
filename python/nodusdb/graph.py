import array
import ctypes
import os

from . import _native
from ._native import NodusError

MAX_NODE_ID = 2**31 - 10
DEFAULT_RESULT_CAPACITY = 1 << 16
ERROR = -1
SYNC_MODES = {"async": 0, "sync": 1}


def _check_node(node):
    if not isinstance(node, int) or isinstance(node, bool):
        raise TypeError(f"node id must be an int, got {type(node).__name__}")
    if node < 0 or node > MAX_NODE_ID:
        raise ValueError(f"node id out of range [0, {MAX_NODE_ID}]: {node}")
    return node


def _check_depth(depth):
    if not isinstance(depth, int) or isinstance(depth, bool):
        raise TypeError(f"max_depth must be an int, got {type(depth).__name__}")
    if depth < 0 or depth > 2**31 - 1:
        raise ValueError(f"max_depth out of range: {depth}")
    return depth


def _column(values):
    try:
        column = array.array("q", values)
    except OverflowError as error:
        raise ValueError("node id out of range") from error
    if column:
        lowest, highest = min(column), max(column)
        if lowest < 0 or highest > MAX_NODE_ID:
            raise ValueError(f"node id out of range [0, {MAX_NODE_ID}]: {lowest}..{highest}")
    return column


def _edge_columns(edges):
    items = edges if isinstance(edges, (list, tuple, array.array)) else list(edges)
    if len(items) and isinstance(items[0], int):
        if len(items) % 2:
            raise ValueError("a flat edge buffer must hold an even number of node ids")
        flat = _column(items)
        return flat[0::2], flat[1::2]
    sources = _column([u for u, _ in items])
    targets = _column([v for _, v in items])
    return sources, targets


def _int64_pointer(column):
    return (ctypes.c_int64 * len(column)).from_buffer(column)


class Graph:
    def __init__(self, library_path=None, result_capacity=DEFAULT_RESULT_CAPACITY, *, path=None, sync_mode="async"):
        self._lib, self._thread = _native.load(library_path)
        self._bind_signatures()
        self._handle = self._open(path, sync_mode)
        if not self._handle:
            raise NodusError("failed to create graph handle")
        self._buffer = (ctypes.c_int64 * result_capacity)()

    def _open(self, path, sync_mode):
        if path is None:
            return self._lib.nodus_create(self._thread)
        if sync_mode not in SYNC_MODES:
            raise ValueError(f"sync_mode must be 'async' or 'sync', got {sync_mode!r}")
        return self._lib.nodus_open_durable(self._thread, os.fsencode(os.fspath(path)), SYNC_MODES[sync_mode])

    def _bind_signatures(self):
        lib = self._lib
        thread, handle, node = _native.THREAD, _native.HANDLE, _native.NODE
        lib.nodus_create.restype = _native.HANDLE
        lib.nodus_create.argtypes = [thread]
        lib.nodus_destroy.restype = ctypes.c_int
        lib.nodus_destroy.argtypes = [thread, handle]
        lib.nodus_open_durable.restype = _native.HANDLE
        lib.nodus_open_durable.argtypes = [thread, ctypes.c_char_p, ctypes.c_int]
        lib.nodus_checkpoint.restype = ctypes.c_int
        lib.nodus_checkpoint.argtypes = [thread, handle]
        lib.nodus_sync.restype = ctypes.c_int
        lib.nodus_sync.argtypes = [thread, handle]
        for name in ("nodus_add_edge", "nodus_remove_edge", "nodus_has_edge"):
            function = getattr(lib, name)
            function.restype = ctypes.c_int
            function.argtypes = [thread, handle, node, node]
        for name in ("nodus_degree", "nodus_in_degree"):
            function = getattr(lib, name)
            function.restype = ctypes.c_int
            function.argtypes = [thread, handle, node]
        for name in ("nodus_add_edges_batch", "nodus_remove_edges_batch"):
            function = getattr(lib, name)
            function.restype = ctypes.c_int
            function.argtypes = [thread, handle, _native.RESULT_BUFFER, _native.RESULT_BUFFER, ctypes.c_int]
        lib.nodus_common_neighbors.restype = ctypes.c_int
        lib.nodus_common_neighbors.argtypes = [thread, handle, node, node, _native.RESULT_BUFFER, ctypes.c_int]
        lib.nodus_khop.restype = ctypes.c_int
        lib.nodus_khop.argtypes = [thread, handle, node, ctypes.c_int, _native.RESULT_BUFFER, ctypes.c_int]

    def checkpoint(self):
        self._require_open()
        if self._lib.nodus_checkpoint(self._thread, self._handle) != 0:
            raise NodusError("checkpoint failed; the graph is not durable up to this point")

    def sync(self):
        self._require_open()
        if self._lib.nodus_sync(self._thread, self._handle) != 0:
            raise NodusError("sync failed; accepted writes may not be on disk yet")

    def close(self):
        if self._handle:
            if self._lib.nodus_destroy(self._thread, self._handle) != 0:
                raise NodusError("graph close failed; the final checkpoint did not complete")
            self._handle = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def _require_open(self):
        if not self._handle:
            raise NodusError("graph is closed")
        return self._handle

    def add_edge(self, u, v):
        return bool(self._lib.nodus_add_edge(self._thread, self._require_open(), _check_node(u), _check_node(v)))

    def remove_edge(self, u, v):
        return bool(self._lib.nodus_remove_edge(self._thread, self._require_open(), _check_node(u), _check_node(v)))

    def add_edges_from(self, edges):
        return self._batch(self._lib.nodus_add_edges_batch, edges, "add_edges_from")

    def remove_edges_from(self, edges):
        return self._batch(self._lib.nodus_remove_edges_batch, edges, "remove_edges_from")

    def has_edge(self, u, v):
        return bool(self._lib.nodus_has_edge(self._thread, self._require_open(), _check_node(u), _check_node(v)))

    def degree(self, u):
        result = self._lib.nodus_degree(self._thread, self._require_open(), _check_node(u))
        return self._checked(result, "degree")

    def in_degree(self, v):
        result = self._lib.nodus_in_degree(self._thread, self._require_open(), _check_node(v))
        return self._checked(result, "in_degree")

    def common_neighbors(self, u, v):
        _check_node(u)
        _check_node(v)
        return self._collect(
            lambda buffer, capacity: self._lib.nodus_common_neighbors(
                self._thread, self._require_open(), u, v, buffer, capacity),
            "common_neighbors",
        )

    def khop(self, start, max_depth):
        _check_node(start)
        _check_depth(max_depth)
        return self._collect(
            lambda buffer, capacity: self._lib.nodus_khop(
                self._thread, self._require_open(), start, max_depth, buffer, capacity),
            "khop",
        )

    def _batch(self, function, edges, operation):
        sources, targets = _edge_columns(edges)
        if not sources:
            return 0
        result = function(
            self._thread, self._require_open(), _int64_pointer(sources), _int64_pointer(targets), len(sources)
        )
        return self._checked(result, operation)

    def _collect(self, call, operation):
        total = self._checked(call(self._buffer, len(self._buffer)), operation)
        if total > len(self._buffer):
            self._buffer = (ctypes.c_int64 * total)()
            total = self._checked(call(self._buffer, total), operation)
        return list(self._buffer[:total])

    @staticmethod
    def _checked(result, operation):
        if result == ERROR:
            raise NodusError(f"{operation} failed inside the native kernel")
        return result
