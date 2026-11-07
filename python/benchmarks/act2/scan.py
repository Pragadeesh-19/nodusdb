"""Every copy NodusDB keeps, read the way its own reader reads it, and searched for one name.

NodusDB's own files (log segments, snapshots, chain objects) store names as plain UTF-8, so a byte match reads them
faithfully. The Iceberg table needs its readers: Parquet pages are compressed and split a name into a type column
and an id column, and manifests carry each data file's column bounds."""

import io
import json
from dataclasses import dataclass
from pathlib import Path

import fastavro
import pyarrow.parquet as pq

from act2 import lake

GRAPH_DIRECTORY = "graph directory"
CHAIN = "chain objects"
SNAPSHOTS = "bucket snapshots"
DATA = "Iceberg data files"
MANIFESTS = "Iceberg manifests"
METADATA = "Iceberg metadata"
OTHER = "other bucket objects"
LAYERS_THAT_HOLD_EVERY_NAME = (GRAPH_DIRECTORY, CHAIN, SNAPSHOTS, DATA)
NAME_COLUMNS = (("object_type", "object_id"), ("subject_type", "subject_id"))
LOCK_FILE = "nodus.lock"


class ScanError(RuntimeError):
    pass


@dataclass(frozen=True)
class Name:
    text: str

    @property
    def type(self):
        return self.text.split(":", 1)[0]

    @property
    def id(self):
        return self.text.split(":", 1)[1]


@dataclass(frozen=True)
class Hit:
    layer: str
    location: str
    how: str


def scan(name, graph_directory, objects):
    name = Name(name)
    field_ids = _name_field_ids(objects)
    hits = []
    for path in sorted(Path(graph_directory).rglob("*")):
        content = _read_file(path)
        if content is not None:
            hits.extend(Hit(GRAPH_DIRECTORY, path.as_posix(), how) for how in _raw(name, content))
    for key in sorted(objects.keys("")):
        content = objects.read(key)
        if content is not None:
            layer = layer_of(key)
            hits.extend(Hit(layer, objects.location(key), how) for how in _find(name, content, layer, field_ids))
    return hits


def require_every_layer(hits, name):
    missing = [layer for layer in LAYERS_THAT_HOLD_EVERY_NAME if not any(hit.layer == layer for hit in hits)]
    if missing:
        raise ScanError(f"before the erase the scan finds {name} nowhere in: {', '.join(missing)}; a scan that cannot "
                        f"see a layer cannot prove the name left it")


def require_nowhere(hits, name):
    if hits:
        first = hits[0]
        raise ScanError(f"{len(hits)} copies still hold {name}, first {first.location} ({first.layer}, {first.how})")


def layer_of(key):
    if key.startswith("_nodus/chain/"):
        return CHAIN
    if key.startswith("_nodus/snapshots/"):
        return SNAPSHOTS
    if key.startswith("iceberg/data/") and key.endswith(".parquet"):
        return DATA
    if key.startswith("iceberg/metadata/") and key.endswith(".avro"):
        return MANIFESTS
    if key.startswith("iceberg/metadata/"):
        return METADATA
    return OTHER


def _find(name, content, layer, field_ids):
    found = list(_raw(name, content))
    if layer == DATA:
        found.extend(_parquet(name, content))
    elif layer == MANIFESTS:
        found.extend(_manifest(name, content, field_ids))
    elif layer == METADATA and content.lstrip().startswith(b"{"):
        found.extend(_json(name, content))
    return found


def _raw(name, content):
    return ["raw bytes"] if name.text.encode("utf-8") in content else []


def _parquet(name, content):
    found = []
    parquet = pq.ParquetFile(io.BytesIO(content))
    table = parquet.read()
    columns = {column: table.column(column).to_pylist() for column in table.column_names}
    if any(_names_row(name, columns, row) for row in range(table.num_rows)):
        found.append("parquet row")
    if any(isinstance(value, str) and name.text in value for values in columns.values() for value in values):
        found.append("parquet value")
    groups = range(parquet.metadata.num_row_groups)
    if any(_names_row_group(name, parquet.metadata.row_group(group)) for group in groups):
        found.append("parquet statistics")
    metadata = parquet.metadata.metadata or {}
    if any(name.text.encode("utf-8") in value for value in metadata.values()):
        found.append("parquet metadata")
    return found


def _names_row(name, columns, row):
    return any(column_type in columns and column_id in columns and columns[column_type][row] == name.type
               and columns[column_id][row] == name.id for column_type, column_id in NAME_COLUMNS)


def _names_row_group(name, row_group):
    bounds = {}
    for index in range(row_group.num_columns):
        column = row_group.column(index)
        statistics = column.statistics
        if statistics is not None and statistics.has_min_max:
            bounds[column.path_in_schema] = (_text(statistics.min), _text(statistics.max))
    return _names_bounds(name, bounds)


def _manifest(name, content, field_ids):
    found = []
    reader = fastavro.reader(io.BytesIO(content))
    if any(name.text.encode("utf-8") in _bytes(value) for value in reader.metadata.values()):
        found.append("manifest metadata")
    for record in reader:
        if _holds_text(record, name.text):
            found.append("manifest value")
        data_file = record.get("data_file") or {}
        lower = {entry["key"]: entry["value"] for entry in data_file.get("lower_bounds") or ()}
        upper = {entry["key"]: entry["value"] for entry in data_file.get("upper_bounds") or ()}
        bounds = {column: (_text(lower[field]), _text(upper[field])) for column, field in field_ids.items()
                  if field in lower and field in upper}
        if _names_bounds(name, bounds):
            found.append("manifest bounds")
    return found


def _names_bounds(name, bounds):
    for column_type, column_id in NAME_COLUMNS:
        if column_type in bounds and column_id in bounds and bounds[column_type] == (name.type, name.type) \
                and name.id in bounds[column_id]:
            return True
    return any(name.text in bound for pair in bounds.values() for bound in pair)


def _json(name, content):
    return ["json value"] if _holds_text(json.loads(content), name.text) else []


def _holds_text(value, text):
    if isinstance(value, str):
        return text in value
    if isinstance(value, bytes):
        return text.encode("utf-8") in value
    if isinstance(value, dict):
        return any(_holds_text(key, text) or _holds_text(item, text) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return any(_holds_text(item, text) for item in value)
    return False


def _name_field_ids(objects):
    version = lake.newest_version_key(objects.keys(lake.METADATA_PREFIX))
    if version is None:
        return {}
    content = objects.read(version[1])
    if content is None:
        return {}
    metadata = json.loads(content)
    schema = next(schema for schema in metadata["schemas"] if schema["schema-id"] == metadata["current-schema-id"])
    wanted = {column for pair in NAME_COLUMNS for column in pair}
    return {field["name"]: field["id"] for field in schema["fields"] if field["name"] in wanted}


def _read_file(path):
    if not path.is_file():
        return None
    try:
        return path.read_bytes()
    except FileNotFoundError:
        return None
    except PermissionError:
        if path.name == LOCK_FILE:
            return None
        raise


def _text(value):
    return value.decode("utf-8", "replace") if isinstance(value, bytes) else str(value)


def _bytes(value):
    return value if isinstance(value, bytes) else str(value).encode("utf-8")
