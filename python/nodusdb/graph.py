import array
import ctypes
import os
import threading
import typing
import uuid
import weakref

from . import _native
from ._native import MEMORY_LIMIT, NodusError, NodusMemoryError, error_for, serialized

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
DURABILITY = {"local": 0, "lake": 1}
NO_TOKEN = -1
CHECK_ABSENT = 0
CHECK_GRANTED = 1


class Token(typing.NamedTuple):
    """The position of a write: the epoch of the writer tenure and the log sequence number."""

    epoch: int
    lsn: int


class Transaction:
    """Tuple writes that commit together or not at all."""

    def __init__(self):
        self._operations = []

    def add(self, object, relation, subject, subject_relation=None):
        self._operations.append((1, object, relation, subject, subject_relation))
        return self

    def remove(self, object, relation, subject, subject_relation=None):
        self._operations.append((0, object, relation, subject, subject_relation))
        return self

    def __len__(self):
        return len(self._operations)

    def _encode(self):
        kinds = array.array("i")
        lengths = array.array("i")
        blob = bytearray()
        for kind, *fields in self._operations:
            kinds.append(kind)
            for position, field in enumerate(fields):
                if field is None and position == 3:
                    lengths.append(-1)
                    continue
                encoded = _text(field)
                lengths.append(len(encoded))
                blob.extend(encoded)
        return bytes(blob), lengths, kinds


def _text(value):
    if not isinstance(value, str):
        raise TypeError(f"expected a str, got {type(value).__name__}")
    return value.encode("utf-8")


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
            self._result_capacity = result_capacity
            self._local = threading.local()
            self._local.buffer = (ctypes.c_int64 * result_capacity)()
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
        detail = _native.last_error_message(self._lib, self._thread)
        raise error_for(status.value, f"failed to create graph handle: {detail}" if detail
                        else "failed to create graph handle")

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
        token_out = ctypes.POINTER(ctypes.c_int64)
        lib.nodus_token.restype = ctypes.c_int
        lib.nodus_token.argtypes = [thread, handle, token_out]
        lib.nodus_schema_version.restype = ctypes.c_int
        lib.nodus_schema_version.argtypes = [thread, handle]
        lib.nodus_schema_apply.restype = ctypes.c_int
        lib.nodus_schema_apply.argtypes = [thread, handle, ctypes.c_char_p, ctypes.c_int, token_out]
        lib.nodus_tuple_write.restype = ctypes.c_int
        lib.nodus_tuple_write.argtypes = [
            thread, handle, ctypes.c_char_p, ctypes.POINTER(ctypes.c_int32), ctypes.POINTER(ctypes.c_int32),
            ctypes.c_int, ctypes.c_int, token_out,
        ]
        lib.nodus_check.restype = ctypes.c_int
        lib.nodus_check.argtypes = [
            thread, handle, ctypes.c_char_p, ctypes.POINTER(ctypes.c_int32), ctypes.c_int64, ctypes.c_int64,
        ]

    @serialized
    def checkpoint(self):
        self._require_open()
        self._require_success(self._lib.nodus_checkpoint(self._thread, self._handle),
                              "checkpoint failed; the graph is not durable up to this point")

    @serialized
    def sync(self):
        self._require_open()
        self._require_success(self._lib.nodus_sync(self._thread, self._handle),
                              "sync failed; accepted writes may not be on disk yet")

    @property
    def token(self):
        """The position of the last write this graph has applied."""
        out = (ctypes.c_int64 * 2)()
        self._require_success(self._lib.nodus_token(self._thread, self._require_open(), out), "token failed")
        return Token(out[0], out[1])

    @property
    def schema_version(self):
        version = self._lib.nodus_schema_version(self._thread, self._require_open())
        if version < 0:
            raise self._failure(version, "schema_version failed")
        return version

    @serialized
    def apply_schema(self, document):
        """Apply the next version of the schema. Nothing changes if the document or a stored tuple conflicts."""
        encoded = _text(document)
        out = (ctypes.c_int64 * 2)()
        code = self._lib.nodus_schema_apply(self._thread, self._require_open(), encoded, len(encoded), out)
        self._refresh_kind()
        self._require_success(code, "apply_schema failed")
        return Token(out[0], out[1])

    @serialized
    def write(self, transaction, durability="local"):
        """Commit a Transaction. Every tuple applies or none does. Returns the token of the commit."""
        if durability not in DURABILITY:
            raise ValueError(f"durability must be one of {sorted(DURABILITY)}, got {durability!r}")
        if not isinstance(transaction, Transaction):
            raise TypeError(f"expected a Transaction, got {type(transaction).__name__}")
        if not len(transaction):
            return self.token
        blob, lengths, kinds = transaction._encode()
        out = (ctypes.c_int64 * 2)()
        code = self._lib.nodus_tuple_write(
            self._thread, self._require_open(), blob, _int32_pointer(lengths), _int32_pointer(kinds),
            len(transaction), DURABILITY[durability], out)
        self._refresh_kind()
        self._require_success(code, "write failed")
        return Token(out[0], out[1])

    def add_tuple(self, object, relation, subject, subject_relation=None):
        return self.write(Transaction().add(object, relation, subject, subject_relation))

    def remove_tuple(self, object, relation, subject, subject_relation=None):
        return self.write(Transaction().remove(object, relation, subject, subject_relation))

    def check(self, object, permission, subject, *, at_least=None):
        """Whether the subject holds the relation or permission on the object.

        With at_least, the answer reflects at least the write the token names."""
        parts = [_text(object), _text(permission), _text(subject)]
        lengths = array.array("i", [len(part) for part in parts])
        if at_least is None:
            epoch, lsn = NO_TOKEN, 0
        else:
            epoch, lsn = int(at_least[0]), int(at_least[1])
            if epoch < 0 or lsn < 0:
                raise ValueError("a token has a non-negative epoch and lsn")
        result = self._lib.nodus_check(
            self._thread, self._require_open(), b"".join(parts), _int32_pointer(lengths), epoch, lsn)
        if result < 0:
            raise self._failure(result, "check failed")
        return result == CHECK_GRANTED

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

    def has_edge(self, u, v):
        if self._kind == KIND_INTEGER and _is_node(u) and _is_node(v):
            return bool(self._lib.nodus_has_edge(self._thread, self._require_open(), u, v))
        ids = self._read_ids((u, v))
        if ids is None:
            return False
        return bool(self._lib.nodus_has_edge(self._thread, self._require_open(), ids[0], ids[1]))

    def degree(self, u):
        ids = self._read_ids((u,))
        if ids is None:
            return 0
        result = self._lib.nodus_degree(self._thread, self._require_open(), ids[0])
        return self._checked(result, "degree")

    def in_degree(self, v):
        ids = self._read_ids((v,))
        if ids is None:
            return 0
        result = self._lib.nodus_in_degree(self._thread, self._require_open(), ids[0])
        return self._checked(result, "in_degree")

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

    def _result_buffer(self):
        buffer = getattr(self._local, "buffer", None)
        if buffer is None:
            buffer = self._local.buffer = (ctypes.c_int64 * self._result_capacity)()
        return buffer

    def _collect(self, call, operation):
        buffer = self._result_buffer()
        total = self._checked(call(buffer, len(buffer)), operation)
        if total > len(buffer):
            buffer = self._local.buffer = (ctypes.c_int64 * total)()
            total = self._checked(call(buffer, total), operation)
        return list(buffer[:total])

    def _added(self, result):
        return self._checked(result, "add_edge") == 1

    def _checked(self, result, operation):
        if result < 0:
            raise self._failure(result, f"{operation} failed inside the native kernel", operation)
        return result

    def _require_success(self, code, message):
        if code != 0:
            raise self._failure(code, message)

    def _refresh_kind(self):
        kind = self._lib.nodus_key_kind(self._thread, self._handle)
        if kind >= 0:
            self._kind = kind

    def _failure(self, code, message, operation=None):
        if code == MEMORY_LIMIT and operation is not None:
            return NodusMemoryError(self._limit_message(operation))
        detail = _native.last_error_message(self._lib, self._thread)
        return error_for(code, f"{message}: {detail}" if detail else message)

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
