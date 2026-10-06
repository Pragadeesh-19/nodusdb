import array
import ctypes
import hashlib
import os
import struct
import threading
import weakref

from . import _native
from ._native import NodusError, serialized

MAX_FLUSH_ROWS = 1 << 29
MAX_VAR_BYTES = 1 << 30
INITIAL_VAR_BYTES = 1 << 12
BATCH_ROWS = 1 << 16

_JAVA_TYPES = {"int64": "INT64", "float64": "DOUBLE", "int32": "INT32", "utf8": "UTF8"}
_ARROW_TYPES = {"int64": "int64", "float64": "double", "int32": "int32"}
_GET_ABSENT = 0
_GET_FOUND = 1
_GET_BUFFER_TOO_SMALL = 2
_AGGREGATE_EMPTY = 0
_AGGREGATE_VALUE = 1
_CODEC_IDS = {"none": 0, "snappy": 1}

_bound = set()


def key_hash(key):
    if isinstance(key, bool):
        raise TypeError("key must be an int, str, or bytes, not bool")
    if isinstance(key, int):
        if not -(2**63) <= key < 2**63:
            raise ValueError(f"integer key out of signed 64-bit range: {key}")
        return key
    if isinstance(key, str):
        key = key.encode("utf-8")
    if isinstance(key, (bytes, bytearray, memoryview)):
        return int.from_bytes(hashlib.blake2b(bytes(key), digest_size=8).digest(), "little", signed=True)
    raise TypeError(f"key must be an int, str, or bytes, got {type(key).__name__}")


def _bind(library):
    if id(library) in _bound:
        return
    thread = ctypes.c_void_p
    handle = ctypes.c_void_p
    int64_pointer = ctypes.POINTER(ctypes.c_int64)
    int32_pointer = ctypes.POINTER(ctypes.c_int32)

    library.nodus_lake_open.restype = handle
    library.nodus_lake_open.argtypes = [
        thread, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_int,
        ctypes.c_int, ctypes.c_int, ctypes.c_int64, ctypes.c_int,
    ]
    library.nodus_lake_close.restype = ctypes.c_bool
    library.nodus_lake_close.argtypes = [thread, handle]
    library.nodus_lake_upsert.restype = ctypes.c_bool
    library.nodus_lake_upsert.argtypes = [
        thread, handle, ctypes.c_int64, int64_pointer, int32_pointer,
        ctypes.c_char_p, ctypes.c_int, int32_pointer,
    ]
    library.nodus_lake_upsert_batch.restype = ctypes.c_int64
    library.nodus_lake_upsert_batch.argtypes = [
        thread, handle, int64_pointer, ctypes.c_int64, int64_pointer, int32_pointer,
        ctypes.c_char_p, ctypes.c_int64, int32_pointer,
    ]
    library.nodus_lake_delete.restype = ctypes.c_bool
    library.nodus_lake_delete.argtypes = [thread, handle, ctypes.c_int64]
    library.nodus_lake_flush.restype = ctypes.c_bool
    library.nodus_lake_flush.argtypes = [thread, handle]
    library.nodus_lake_get.restype = ctypes.c_int
    library.nodus_lake_get.argtypes = [
        thread, handle, ctypes.c_int64, int64_pointer, int32_pointer,
        ctypes.c_char_p, ctypes.c_int64, int32_pointer,
    ]
    library.nodus_lake_upsert_columns.restype = ctypes.c_int64
    library.nodus_lake_upsert_columns.argtypes = [
        thread, handle, ctypes.c_int64, ctypes.c_void_p,
        ctypes.c_int32, ctypes.c_void_p, ctypes.c_int32, ctypes.c_void_p,
        ctypes.c_int32, ctypes.c_void_p, ctypes.c_void_p,
    ]
    for aggregate in (library.nodus_lake_sum, library.nodus_lake_avg):
        aggregate.restype = ctypes.c_int
        aggregate.argtypes = [thread, handle, ctypes.c_int32, ctypes.c_void_p]
    _bound.add(id(library))


def _pointer(values, ctype):
    if len(values) == 0:
        return (ctype * 1)()
    return (ctype * len(values)).from_buffer(values)


def _int32_check(name, value):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"column {name} must be an int")
    if not -(2**31) <= value < 2**31:
        raise ValueError(f"column {name} out of int32 range: {value}")
    return value


def _int64_check(name, value):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"column {name} must be an int")
    if not -(2**63) <= value < 2**63:
        raise ValueError(f"column {name} out of int64 range: {value}")
    return value


def _float_bits(name, value):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"column {name} must be a float")
    return struct.unpack("<q", struct.pack("<d", float(value)))[0]


def _float_from_bits(bits):
    return struct.unpack("<d", struct.pack("<q", bits))[0]


def _fixed_column(name, array, arrow_type, width):
    if str(array.type) != arrow_type:
        raise TypeError(f"column {name} must be Arrow {arrow_type}, not {array.type}")
    if array.null_count:
        raise ValueError(f"column {name} has nulls, which the columnar path does not accept")
    if len(array) == 0:
        return 0, 0
    return len(array), array.buffers()[1].address + array.offset * width


def _string_column(name, array):
    if str(array.type) != "string":
        raise TypeError(f"column {name} must be Arrow string, not {array.type}")
    if array.null_count:
        raise ValueError(f"column {name} has nulls, which the columnar path does not accept")
    if len(array) == 0:
        return 0, 0, 0
    offsets, data = array.buffers()[1], array.buffers()[2]
    return len(array), offsets.address + array.offset * 4, data.address if data is not None else 0


def _require_rows(name, length, expected):
    if length != expected:
        raise ValueError(f"column {name} has {length} rows, the key column has {expected}")


def _address_table(addresses):
    table = (ctypes.c_int64 * max(1, len(addresses)))(*addresses)
    return table, ctypes.addressof(table)


class LakeTable:
    def __init__(self, path, schema, *, flush_rows=1 << 20, max_slab_bytes=1 << 26,
                 flush_interval=None, compression="snappy", library_path=None):
        self._fields = _validate_schema(schema)
        self._long_fields = [(n, t) for n, t in self._fields if t in ("int64", "float64")]
        self._int_fields = [(n, t) for n, t in self._fields if t == "int32"]
        self._var_fields = [(n, t) for n, t in self._fields if t == "utf8"]
        if not 1 <= flush_rows <= MAX_FLUSH_ROWS:
            raise ValueError(f"flush_rows must be in [1, {MAX_FLUSH_ROWS}]")
        if flush_interval is not None and flush_interval <= 0:
            raise ValueError("flush_interval must be positive seconds")
        interval_millis = 0 if flush_interval is None else max(1, round(flush_interval * 1000))
        if compression not in _CODEC_IDS:
            raise ValueError(f"compression must be one of {sorted(_CODEC_IDS)}, not {compression!r}")

        self._lib, self._isolate = _native.load(library_path)
        self._lock = threading.RLock()
        _bind(self._lib)
        spec = ",".join(f"{name}:{_JAVA_TYPES[kind]}" for name, kind in self._fields).encode("ascii")
        location = os.fsencode(os.fspath(path))
        handle = self._lib.nodus_lake_open(
            self._thread, spec, len(spec), location, len(location),
            flush_rows, max_slab_bytes, interval_millis, _CODEC_IDS[compression],
        )
        if not handle:
            raise NodusError(f"could not open lake table at {os.fspath(path)}")
        self._owned = _native.OwnedHandle(
            self._lib, self._isolate, handle, _closer(self._lib),
            "lake close failed; the table is still open and close can be retried")
        self._finalizer = weakref.finalize(self, _native.release_unclosed, self._owned, "LakeTable")

    @property
    def _thread(self):
        return _native.current_thread(self._lib, self._isolate)

    @serialized
    def upsert(self, key, row):
        keys, longs, ints, var_bytes, var_lengths = self._pack([(key, row)])
        ok = self._lib.nodus_lake_upsert(
            self._thread, self._require_open(), keys[0],
            _pointer(longs, ctypes.c_int64), _pointer(ints, ctypes.c_int32),
            bytes(var_bytes), len(var_bytes), _pointer(var_lengths, ctypes.c_int32),
        )
        if not ok:
            raise NodusError("lake upsert failed")

    @serialized
    def upsert_from(self, rows):
        pending = []
        written = 0
        for key, row in rows:
            pending.append((key, row))
            if len(pending) == BATCH_ROWS:
                written += self._upsert_chunk(pending)
                pending = []
        if pending:
            written += self._upsert_chunk(pending)
        return written

    @serialized
    def upsert_columns(self, keys, columns):
        if set(columns) != {name for name, _ in self._fields}:
            raise ValueError(f"columns must be exactly {[name for name, _ in self._fields]}")
        key_rows, key_address = _fixed_column("key", keys, "int64", 8)
        if key_rows == 0:
            return 0
        long_addresses = []
        for name, kind in self._long_fields:
            length, address = _fixed_column(name, columns[name], _ARROW_TYPES[kind], 8)
            _require_rows(name, length, key_rows)
            long_addresses.append(address)
        int_addresses = []
        for name, _ in self._int_fields:
            length, address = _fixed_column(name, columns[name], _ARROW_TYPES["int32"], 4)
            _require_rows(name, length, key_rows)
            int_addresses.append(address)
        offset_addresses = []
        data_addresses = []
        for name, _ in self._var_fields:
            length, offset_address, data_address = _string_column(name, columns[name])
            _require_rows(name, length, key_rows)
            offset_addresses.append(offset_address)
            data_addresses.append(data_address)
        long_table, long_ptr = _address_table(long_addresses)
        int_table, int_ptr = _address_table(int_addresses)
        offset_table, offset_ptr = _address_table(offset_addresses)
        data_table, data_ptr = _address_table(data_addresses)
        applied = self._lib.nodus_lake_upsert_columns(
            self._thread, self._require_open(), key_rows, key_address,
            len(long_addresses), long_ptr, len(int_addresses), int_ptr,
            len(offset_addresses), offset_ptr, data_ptr,
        )
        if applied < 0:
            raise NodusError("lake columnar upsert failed; no rows were applied")
        return applied

    @serialized
    def upsert_table(self, table, key="id"):
        batches = table.to_batches() if hasattr(table, "to_batches") else [table]
        applied = 0
        for batch in batches:
            columns = {name: batch.column(name) for name, _ in self._fields}
            applied += self.upsert_columns(batch.column(key), columns)
        return applied

    @serialized
    def sum(self, name):
        out = ctypes.c_double()
        status = self._lib.nodus_lake_sum(self._thread, self._require_open(), self._numeric_index(name),
                                          ctypes.addressof(out))
        if status != _AGGREGATE_VALUE:
            raise NodusError("lake sum failed")
        return out.value

    @serialized
    def average(self, name):
        out = ctypes.c_double()
        status = self._lib.nodus_lake_avg(self._thread, self._require_open(), self._numeric_index(name),
                                          ctypes.addressof(out))
        if status == _AGGREGATE_EMPTY:
            return None
        if status != _AGGREGATE_VALUE:
            raise NodusError("lake average failed")
        return out.value

    @serialized
    def delete(self, key):
        if not self._lib.nodus_lake_delete(self._thread, self._require_open(), key_hash(key)):
            raise NodusError("lake delete failed")

    def _numeric_index(self, name):
        names = [field_name for field_name, _ in self._fields]
        if name not in names:
            raise KeyError(f"unknown column {name!r}")
        index = names.index(name)
        if self._fields[index][1] == "utf8":
            raise TypeError(f"column {name} is text; sum and average need a number")
        return index

    @serialized
    def get(self, key):
        keyed = key_hash(key)
        capacity = INITIAL_VAR_BYTES
        while True:
            longs = (ctypes.c_int64 * max(1, len(self._long_fields)))()
            ints = (ctypes.c_int32 * max(1, len(self._int_fields)))()
            lengths = (ctypes.c_int32 * max(1, len(self._var_fields)))()
            var_buffer = ctypes.create_string_buffer(capacity)
            status = self._lib.nodus_lake_get(
                self._thread, self._require_open(), keyed, longs, ints, var_buffer, capacity, lengths,
            )
            if status == _GET_ABSENT:
                return None
            if status == _GET_FOUND:
                return self._decode(longs, ints, lengths, var_buffer.raw)
            if status == _GET_BUFFER_TOO_SMALL and capacity < MAX_VAR_BYTES:
                capacity = min(capacity * 4, MAX_VAR_BYTES)
                continue
            raise NodusError("lake get failed")

    @serialized
    def flush(self):
        if not self._lib.nodus_lake_flush(self._thread, self._require_open()):
            raise NodusError("lake flush failed")

    @serialized
    def close(self):
        self._owned.close()
        self._finalizer.detach()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.close()

    def _require_open(self):
        if not self._owned.value:
            raise NodusError("lake table is closed")
        return self._owned.value

    def _upsert_chunk(self, rows):
        keys, longs, ints, var_bytes, var_lengths = self._pack(rows)
        applied = self._lib.nodus_lake_upsert_batch(
            self._thread, self._require_open(),
            _pointer(keys, ctypes.c_int64), len(keys),
            _pointer(longs, ctypes.c_int64), _pointer(ints, ctypes.c_int32),
            bytes(var_bytes), len(var_bytes), _pointer(var_lengths, ctypes.c_int32),
        )
        if applied < 0:
            raise NodusError("lake batch upsert failed; no rows were applied")
        return applied

    def _pack(self, rows):
        names = {name for name, _ in self._fields}
        keys = array.array("q")
        longs = array.array("q")
        ints = array.array("i")
        var_bytes = bytearray()
        var_lengths = array.array("i")
        for key, row in rows:
            unknown = set(row) - names
            if unknown:
                raise ValueError(f"unknown columns: {sorted(unknown)}")
            keys.append(key_hash(key))
            for name, kind in self._long_fields:
                value = row[name]
                longs.append(_int64_check(name, value) if kind == "int64" else _float_bits(name, value))
            for name, _ in self._int_fields:
                ints.append(_int32_check(name, row[name]))
            for name, _ in self._var_fields:
                value = row[name]
                data = value.encode("utf-8") if isinstance(value, str) else bytes(value)
                var_bytes += data
                var_lengths.append(len(data))
        return keys, longs, ints, bytes(var_bytes), var_lengths

    def _decode(self, longs, ints, lengths, var_bytes):
        row = {}
        for index, (name, kind) in enumerate(self._long_fields):
            raw = longs[index]
            row[name] = raw if kind == "int64" else _float_from_bits(raw)
        for index, (name, _) in enumerate(self._int_fields):
            row[name] = ints[index]
        offset = 0
        for index, (name, _) in enumerate(self._var_fields):
            length = lengths[index]
            row[name] = var_bytes[offset:offset + length].decode("utf-8")
            offset += length
        return row


def _closer(library):
    def close(thread, handle):
        return library.nodus_lake_close(thread, handle)
    return close


def _validate_schema(schema):
    fields = []
    for name, kind in schema.items():
        if kind not in _JAVA_TYPES:
            raise ValueError(f"unsupported column type {kind!r} for {name!r}")
        fields.append((name, kind))
    return fields
