import array
import ctypes
import os
import threading
import uuid
import weakref

from . import _native
from ._native import ERROR, MEMORY_LIMIT, NodusError, NodusMemoryError, serialized

MAX_NODE_ID = 2**31 - 10
MAX_MEMORY_MB = (2**63 - 1) >> 20
DEFAULT_RESULT_CAPACITY = 1 << 16
LOOKUP_ABSENT = -1
LOOKUP_ERROR = -2
SYNC_MODES = {"async": 0, "sync": 1}
KIND_UNSET = 0
KIND_INTEGER = 1
KIND_STRING = 2
KIND_NAMES = {KIND_UNSET: "no keys yet", KIND_INTEGER: "integer keys", KIND_STRING: "string keys"}


def _check_node(node):
    if not isinstance(node, int) or isinstance(node, bool):
        raise TypeError(f"node key must be an int, str, or UUID, got {type(node).__name__}")
    if node < 0 or node > MAX_NODE_ID:
        raise ValueError(f"node id out of range [0, {MAX_NODE_ID}]: {node}")
    return node


def _check_depth(depth):
    if not isinstance(depth, int) or isinstance(depth, bool):
        raise TypeError(f"max_depth must be an int, got {type(depth).__name__}")
    if depth < 0 or depth > 2**31 - 1:
        raise ValueError(f"max_depth out of range: {depth}")
    return depth


def _encode(key):
    if isinstance(key, uuid.UUID):
        key = str(key)
    if isinstance(key, str):
        return KIND_STRING, key.encode("utf-8")
    return KIND_INTEGER, _check_node(key)


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


def _int32_pointer(column):
    return (ctypes.c_int32 * len(column)).from_buffer(column)


class Graph:
    def __init__(self, library_path=None, result_capacity=DEFAULT_RESULT_CAPACITY, *, path=None, sync_mode="async",
                 max_memory_mb=None):
        limit = _memory_limit_bytes(max_memory_mb)
        self._lib, self._isolate = _native.load(library_path)
        self._lock = threading.RLock()
        self._bind_signatures()
        self._max_memory_mb = max_memory_mb
        handle = self._open(path, sync_mode, limit)
        self._owned = _native.OwnedHandle(
            self._lib, self._isolate, handle, _destroyer(self._lib),
            "graph close failed; the final checkpoint did not complete")
        self._finalizer = weakref.finalize(self, _native.release_unclosed, self._owned, "Graph")
        try:
            self._kind = self._lib.nodus_key_kind(self._thread, handle)
            if self._kind < 0:
                raise NodusError("could not read the graph's key kind")
            self._buffer = (ctypes.c_int64 * result_capacity)()
        except BaseException:
            self.close()
            raise

    @property
    def _thread(self):
        return _native.current_thread(self._lib, self._isolate)

    @property
    def _handle(self):
        return self._owned.value

    def _open(self, path, sync_mode, limit):
        if path is not None and sync_mode not in SYNC_MODES:
            raise ValueError(f"sync_mode must be 'async' or 'sync', got {sync_mode!r}")
        status = ctypes.c_int(0)
        if path is None:
            handle = self._lib.nodus_create_limited(self._thread, limit, ctypes.byref(status))
        else:
            handle = self._lib.nodus_open_durable_limited(
                self._thread, os.fsencode(os.fspath(path)), SYNC_MODES[sync_mode], limit, ctypes.byref(status))
        if handle:
            return handle
        if status.value == MEMORY_LIMIT:
            raise NodusMemoryError(f"the graph does not fit in max_memory_mb={self._max_memory_mb}")
        raise NodusError("failed to create graph handle")

    def _bind_signatures(self):
        lib = self._lib
        thread, handle, node = _native.THREAD, _native.HANDLE, _native.NODE
        lib.nodus_create_limited.restype = _native.HANDLE
        lib.nodus_create_limited.argtypes = [thread, ctypes.c_int64, ctypes.POINTER(ctypes.c_int)]
        lib.nodus_destroy.restype = ctypes.c_int
        lib.nodus_destroy.argtypes = [thread, handle]
        lib.nodus_open_durable_limited.restype = _native.HANDLE
        lib.nodus_open_durable_limited.argtypes = [
            thread, ctypes.c_char_p, ctypes.c_int, ctypes.c_int64, ctypes.POINTER(ctypes.c_int),
        ]
        lib.nodus_checkpoint.restype = ctypes.c_int
        lib.nodus_checkpoint.argtypes = [thread, handle]
        lib.nodus_sync.restype = ctypes.c_int
        lib.nodus_sync.argtypes = [thread, handle]
        lib.nodus_key_kind.restype = ctypes.c_int
        lib.nodus_key_kind.argtypes = [thread, handle]
        lib.nodus_claim_key_kind.restype = ctypes.c_int
        lib.nodus_claim_key_kind.argtypes = [thread, handle, ctypes.c_int]
        lib.nodus_intern.restype = ctypes.c_int64
        lib.nodus_intern.argtypes = [thread, handle, ctypes.c_char_p, ctypes.c_int]
        lib.nodus_lookup.restype = ctypes.c_int64
        lib.nodus_lookup.argtypes = [thread, handle, ctypes.c_char_p, ctypes.c_int]
        lib.nodus_resolve.restype = ctypes.c_int
        lib.nodus_resolve.argtypes = [thread, handle, node, ctypes.c_void_p, ctypes.c_int]
        lib.nodus_add_string_edges_batch.restype = ctypes.c_int
        lib.nodus_add_string_edges_batch.argtypes = [
            thread, handle, ctypes.c_char_p, ctypes.POINTER(ctypes.c_int32), ctypes.c_int,
        ]
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

    @serialized
    def checkpoint(self):
        self._require_open()
        if self._lib.nodus_checkpoint(self._thread, self._handle) != 0:
            raise NodusError("checkpoint failed; the graph is not durable up to this point")

    @serialized
    def sync(self):
        self._require_open()
        if self._lib.nodus_sync(self._thread, self._handle) != 0:
            raise NodusError("sync failed; accepted writes may not be on disk yet")

    @serialized
    def close(self):
        self._owned.close()
        self._finalizer.detach()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def _require_open(self):
        if not self._handle:
            raise NodusError("graph is closed")
        return self._handle

    @serialized
    def add_edge(self, u, v):
        if self._kind == KIND_INTEGER and _is_node(u) and _is_node(v):
            return self._added(self._lib.nodus_add_edge(self._thread, self._require_open(), u, v))
        first, second = self._write_ids((u, v))
        return self._added(self._lib.nodus_add_edge(self._thread, self._require_open(), first, second))

    @serialized
    def remove_edge(self, u, v):
        if self._kind == KIND_INTEGER and _is_node(u) and _is_node(v):
            return bool(self._lib.nodus_remove_edge(self._thread, self._require_open(), u, v))
        ids = self._read_ids((u, v))
        if ids is None:
            return False
        return bool(self._lib.nodus_remove_edge(self._thread, self._require_open(), ids[0], ids[1]))

    @serialized
    def add_edges_from(self, edges):
        items = edges if isinstance(edges, (list, tuple, array.array)) else list(edges)
        if len(items) and _is_string_pair(items[0]):
            return self._add_string_edges(items)
        if len(items):
            self._claim(KIND_INTEGER)
        return self._batch(self._lib.nodus_add_edges_batch, items, "add_edges_from")

    @serialized
    def remove_edges_from(self, edges):
        items = edges if isinstance(edges, (list, tuple, array.array)) else list(edges)
        if len(items) and _is_string_pair(items[0]):
            ids = [self._read_ids(pair) for pair in items]
            items = [pair for pair in ids if pair is not None]
        return self._batch(self._lib.nodus_remove_edges_batch, items, "remove_edges_from")

    @serialized
    def has_edge(self, u, v):
        if self._kind == KIND_INTEGER and _is_node(u) and _is_node(v):
            return bool(self._lib.nodus_has_edge(self._thread, self._require_open(), u, v))
        ids = self._read_ids((u, v))
        if ids is None:
            return False
        return bool(self._lib.nodus_has_edge(self._thread, self._require_open(), ids[0], ids[1]))

    @serialized
    def degree(self, u):
        ids = self._read_ids((u,))
        if ids is None:
            return 0
        result = self._lib.nodus_degree(self._thread, self._require_open(), ids[0])
        return self._checked(result, "degree")

    @serialized
    def in_degree(self, v):
        ids = self._read_ids((v,))
        if ids is None:
            return 0
        result = self._lib.nodus_in_degree(self._thread, self._require_open(), ids[0])
        return self._checked(result, "in_degree")

    @serialized
    def common_neighbors(self, u, v):
        ids = self._read_ids((u, v))
        if ids is None:
            return []
        found = self._collect(
            lambda buffer, capacity: self._lib.nodus_common_neighbors(
                self._thread, self._require_open(), ids[0], ids[1], buffer, capacity),
            "common_neighbors",
        )
        return self._to_keys(found)

    @serialized
    def khop(self, start, max_depth):
        _check_depth(max_depth)
        ids = self._read_ids((start,))
        if ids is None:
            return []
        found = self._collect(
            lambda buffer, capacity: self._lib.nodus_khop(
                self._thread, self._require_open(), ids[0], max_depth, buffer, capacity),
            "khop",
        )
        return self._to_keys(found)

    def _write_ids(self, keys):
        encoded = [_encode(key) for key in keys]
        kinds = {kind for kind, _ in encoded}
        if len(kinds) > 1:
            raise TypeError("an edge must use one kind of node key, not a mix of integers and strings")
        kind = kinds.pop()
        self._claim(kind)
        if kind == KIND_INTEGER:
            return [value for _, value in encoded]
        return [self._intern(value) for _, value in encoded]

    def _read_ids(self, keys):
        encoded = [_encode(key) for key in keys]
        kinds = {kind for kind, _ in encoded}
        if len(kinds) > 1:
            raise TypeError("an edge must use one kind of node key, not a mix of integers and strings")
        kind = kinds.pop()
        if self._kind not in (KIND_UNSET, kind):
            raise TypeError(f"this graph uses {KIND_NAMES[self._kind]}; got {KIND_NAMES[kind]}")
        if kind == KIND_INTEGER:
            return [value for _, value in encoded]
        if self._kind == KIND_UNSET:
            return None
        ids = [self._lookup(value) for _, value in encoded]
        if any(identifier == LOOKUP_ABSENT for identifier in ids):
            return None
        return ids

    def _claim(self, kind):
        if self._kind == kind:
            return
        current = self._lib.nodus_claim_key_kind(self._thread, self._require_open(), kind)
        if current < 0:
            raise NodusError("could not record the graph's key kind")
        self._kind = current
        if current != kind:
            raise TypeError(f"this graph uses {KIND_NAMES[current]}; got {KIND_NAMES[kind]}")

    def _intern(self, value):
        identifier = self._lib.nodus_intern(self._thread, self._require_open(), value, len(value))
        if identifier < 0:
            raise NodusError("string interning failed")
        return identifier

    def _lookup(self, value):
        identifier = self._lib.nodus_lookup(self._thread, self._require_open(), value, len(value))
        if identifier == LOOKUP_ERROR:
            raise NodusError("string lookup failed")
        return identifier

    def _to_keys(self, identifiers):
        if self._kind != KIND_STRING:
            return identifiers
        return [self._resolve(identifier) for identifier in identifiers]

    def _resolve(self, identifier):
        buffer = ctypes.create_string_buffer(64)
        length = self._lib.nodus_resolve(self._thread, self._require_open(), identifier, buffer, len(buffer))
        if length < 0:
            raise NodusError(f"no string is stored for id {identifier}")
        if length > len(buffer):
            buffer = ctypes.create_string_buffer(length)
            length = self._lib.nodus_resolve(self._thread, self._require_open(), identifier, buffer, len(buffer))
        return buffer.raw[:length].decode("utf-8")

    def _add_string_edges(self, pairs):
        encoded = []
        for pair in pairs:
            if not _is_string_pair(pair):
                raise TypeError("edges must not mix integer and string node keys")
            encoded.extend(_encode(key) for key in pair)
        if any(kind != KIND_STRING for kind, _ in encoded):
            raise TypeError("edges must not mix integer and string node keys")
        self._claim(KIND_STRING)
        values = [value for _, value in encoded]
        lengths = array.array("i", [len(value) for value in values])
        blob = b"".join(values)
        result = self._lib.nodus_add_string_edges_batch(
            self._thread, self._require_open(), blob, _int32_pointer(lengths), len(pairs),
        )
        return self._checked(result, "add_edges_from")

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

    def _added(self, result):
        if result == MEMORY_LIMIT:
            raise NodusMemoryError(self._limit_message("add_edge"))
        if result < 0:
            raise NodusError("add_edge failed inside the native kernel")
        return result == 1

    def _checked(self, result, operation):
        if result == MEMORY_LIMIT:
            raise NodusMemoryError(self._limit_message(operation))
        if result == ERROR:
            raise NodusError(f"{operation} failed inside the native kernel")
        return result

    def _limit_message(self, operation):
        return (f"{operation} would exceed max_memory_mb={self._max_memory_mb}; the graph is unchanged, "
                "except that a batch keeps the edges it applied before the limit")


def _memory_limit_bytes(megabytes):
    if megabytes is None:
        return 0
    if isinstance(megabytes, bool) or not isinstance(megabytes, int):
        raise TypeError(f"max_memory_mb must be an int or None, got {type(megabytes).__name__}")
    if not 1 <= megabytes <= MAX_MEMORY_MB:
        raise ValueError(f"max_memory_mb must be between 1 and {MAX_MEMORY_MB}, got {megabytes}")
    return megabytes << 20


def _destroyer(library):
    def destroy(thread, handle):
        return library.nodus_destroy(thread, handle) == 0
    return destroy


def _is_node(value):
    return type(value) is int and 0 <= value <= MAX_NODE_ID


def _is_string_pair(item):
    return isinstance(item, (tuple, list)) and len(item) == 2 and all(
        isinstance(key, (str, uuid.UUID)) for key in item)
