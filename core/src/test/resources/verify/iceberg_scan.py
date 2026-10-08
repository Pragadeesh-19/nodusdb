import json
import sys

try:
    import pyarrow as pa
    from pyiceberg.table import StaticTable
except ImportError:
    sys.exit(77)


def plain(value):
    if isinstance(value, (bytes, bytearray)):
        return {"hex": bytes(value).hex()}
    if isinstance(value, dict):
        return {str(key): plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def columns_of(arrow_table):
    result = {}
    for name in arrow_table.column_names:
        column = arrow_table.column(name)
        if pa.types.is_timestamp(column.type):
            column = column.cast(pa.int64())
        result[name] = column.to_pylist()
    return result


def snapshot_info(snapshot):
    summary = dict(snapshot.summary.additional_properties) if snapshot.summary else {}
    if snapshot.summary is not None:
        summary["operation"] = snapshot.summary.operation.value
    return {
        "id": snapshot.snapshot_id,
        "parent": snapshot.parent_snapshot_id,
        "sequence": snapshot.sequence_number,
        "manifest_list": snapshot.manifest_list,
        "summary": summary,
    }


def file_info(task):
    data_file = task.file
    return {
        "path": data_file.file_path,
        "records": data_file.record_count,
        "size": data_file.file_size_in_bytes,
        "partition": plain(dict(data_file.partition.__dict__)) if hasattr(data_file.partition, "__dict__") else None,
        "lower": plain(dict(data_file.lower_bounds or {})),
        "upper": plain(dict(data_file.upper_bounds or {})),
        "value_counts": plain(dict(data_file.value_counts or {})),
        "sort_order_id": data_file.sort_order_id,
    }


def main(location, filters):
    table = StaticTable.from_metadata(location)
    scan = table.scan()
    tasks = list(scan.plan_files())
    arrow_table = scan.to_arrow()
    filtered = {}
    for expression in filters:
        filtered_scan = table.scan(row_filter=expression)
        filtered[expression] = {
            "files": len(list(filtered_scan.plan_files())),
            "rows": filtered_scan.to_arrow().num_rows,
        }
    schema = table.schema()
    print(json.dumps({
        "rows": arrow_table.num_rows,
        "columns": columns_of(arrow_table),
        "schema": [{"id": f.field_id, "name": f.name, "required": f.required, "type": str(f.field_type)}
                   for f in schema.fields],
        "spec": [{"name": f.name, "transform": str(f.transform), "source_id": f.source_id, "field_id": f.field_id}
                 for f in table.spec().fields],
        "snapshots": [snapshot_info(s) for s in table.metadata.snapshots],
        "current_snapshot_id": table.metadata.current_snapshot_id,
        "format_version": table.metadata.format_version,
        "files": [file_info(task) for task in tasks],
        "filters": filtered,
        "properties": dict(table.properties),
    }))


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
